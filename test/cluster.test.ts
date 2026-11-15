import { spawn, execFile, type ChildProcess } from 'node:child_process';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:net';
import { randomInt } from 'node:crypto';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { Cluster } from 'ioredis';
import { Cluster as LegacyCluster } from 'ioredis-legacy';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SyncedMap, type SyncedMapRedisClusterClient } from '../src/index.js';

const execFileAsync = promisify(execFile);
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const clusterSize = 6;
let basePort: number;
let directory: string;
const processes: ChildProcess[] = [];
const clients: Cluster[] = [];
const maps: Array<SyncedMap<string, number>> = [];

async function canBind(port: number): Promise<boolean> {
  const probe = createServer();
  return new Promise<boolean>(resolve => {
    probe.once('error', () => resolve(false));
    probe.listen(port, '127.0.0.1', () => probe.close(() => resolve(true)));
  });
}

async function reserveClusterPorts(): Promise<number> {
  for (let i = 0; i < 100; i++) {
    const candidate = randomInt(17000, 24000);
    const ports = Array.from({ length: clusterSize }, (_, n) => candidate + n);
    if ((await Promise.all([...ports, ...ports.map(port => port + 10000)].map(canBind))).every(Boolean)) {
      return candidate;
    }
  }
  throw new Error('No free Redis Cluster ports for the integration test.');
}

async function cli(port: number, ...args: string[]): Promise<string> {
  const { stdout } = await execFileAsync('redis-cli', ['-h', '127.0.0.1', '-p', String(port), ...args], {
    timeout: 7000,
  });
  return stdout.trim();
}

async function waitUntil(predicate: () => Promise<boolean>, timeout = 12000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch {
      // The cluster is still electing or starting.
    }
    await sleep(80);
  }
  throw new Error('Redis Cluster did not reach its expected state within ' + timeout + ' ms.');
}

async function createCluster(): Promise<void> {
  basePort = await reserveClusterPorts();
  directory = await mkdtemp(path.join(os.tmpdir(), 'syncedmap-real-cluster-'));
  for (let i = 0; i < clusterSize; i++) {
    const port = basePort + i;
    const folder = path.join(directory, 'node-' + i);
    await mkdir(folder);
    const server = spawn('redis-server', [
      '--bind', '127.0.0.1', '--port', String(port),
      '--cluster-enabled', 'yes',
      '--cluster-config-file', path.join(folder, 'nodes.conf'),
      '--cluster-node-timeout', '1500',
      '--cluster-announce-ip', '127.0.0.1',
      '--cluster-announce-port', String(port),
      '--cluster-announce-bus-port', String(port + 10000),
      '--save', '', '--appendonly', 'no',
      '--protected-mode', 'yes',
      '--dir', folder,
    ], { stdio: 'ignore' });
    processes.push(server);
  }
  await waitUntil(async () => {
    const results = await Promise.all(
      Array.from({ length: clusterSize }, (_, i) => cli(basePort + i, 'PING'))
    );
    return results.every(result => result === 'PONG');
  });
  await execFileAsync('redis-cli', [
    '--cluster', 'create',
    ...Array.from({ length: clusterSize }, (_, i) => '127.0.0.1:' + (basePort + i)),
    '--cluster-replicas', '1', '--cluster-yes',
  ], { timeout: 30000 });
  await waitUntil(async () =>
    (await cli(basePort, 'CLUSTER', 'INFO')).includes('cluster_state:ok')
  );
}

function newClient(): Cluster {
  const client = new Cluster(
    [{ host: '127.0.0.1', port: basePort }],
    { slotsRefreshInterval: 500, retryDelayOnFailover: 50 }
  );
  client.on('error', () => undefined);
  clients.push(client);
  // Compile-time check: public API accepts a real ioredis Cluster instance.
  const compatible: SyncedMapRedisClusterClient = client;
  expect(compatible.isCluster).toBe(true);
  return client;
}

function openMap(namespace: string, client = newClient()): SyncedMap<string, number> {
  const map = new SyncedMap<string, number>({
    namespace, redis: { client }, resyncIntervalMs: 0,
  });
  maps.push(map);
  return map;
}

describe('real six-node Redis Cluster (three masters and three replicas)', () => {
  beforeAll(createCluster, 45000);
  afterAll(async () => {
    for (const map of maps) await map.dispose().catch(() => undefined);
    for (const client of clients) client.disconnect();
    for (const server of processes) {
      if (server.exitCode === null && !server.killed) server.kill('SIGTERM');
    }
    await Promise.all(processes.map(server => new Promise<void>(resolve => {
      if (server.exitCode !== null || server.signalCode !== null) return resolve();
      server.once('exit', () => resolve());
    })));
    if (directory) await rm(directory, { recursive: true, force: true });
  }, 20000);

  it('opens a cluster-aware client and replicates writes across independently connected maps', async () => {
    const first = openMap('cluster-replication');
    const second = openMap('cluster-replication');
    await Promise.all([first.ready(), second.ready()]);
    await first.set('alpha', 12);
    await waitUntil(async () => second.get('alpha') === 12);
    expect(await second.getFresh('alpha')).toBe(12);
    await first.update('alpha', value => (value ?? 0) + 8);
    await waitUntil(async () => second.get('alpha') === 20);
    expect(await second.readLocked('alpha', value => value)).toBe(20);
    await first.delete('alpha');
    await waitUntil(async () => second.get('alpha') === undefined);
  }, 20000);

  it('hydrates and clears namespaced keys distributed across different master shards', async () => {
    const writer = newClient();
    await writer.ping();
    const namespaces = Array.from({ length: 15 }, (_, i) => 'cluster-sharded-' + i);
    for (const namespace of namespaces) {
      const map = openMap(namespace, writer);
      await map.ready();
      await map.set('exists', 8);
      await map.set('another', 4);
    }
    const occupiedMasters = new Set<number>();
    for (const node of writer.nodes('master')) {
      const results = await node.keys('syncedmap:{cluster-sharded-*}:entry:*');
      if (results.length > 0) occupiedMasters.add(node.options.port!);
    }
    expect(occupiedMasters.size).toBeGreaterThanOrEqual(2);
    for (const namespace of namespaces) {
      const second = openMap(namespace);
      await second.ready();
      expect(second.size).toBe(2);
      expect(second.get('exists')).toBe(8);
      await second.clear();
      expect(second.size).toBe(0);
      expect(await second.getFresh('another')).toBeUndefined();
    }
  }, 30000);

  it('serializes contended updates and holds readLocked across cluster clients', async () => {
    const first = openMap('cluster-locks');
    const second = openMap('cluster-locks');
    await Promise.all([first.ready(), second.ready()]);
    await first.set('count', 0);
    await Promise.all([
      ...Array.from({ length: 12 }, () => first.update('count', x => (x ?? 0) + 1)),
      ...Array.from({ length: 12 }, () => second.update('count', x => (x ?? 0) + 1)),
    ]);
    expect(await first.getFresh('count')).toBe(24);
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const held = new Promise<void>(resolve => { entered = resolve; });
    const reading = first.readLocked('count', async value => {
      expect(value).toBe(24);
      entered();
      await gate;
    });
    await held;
    let wrote = false;
    const writing = second.set('count', 25).then(() => { wrote = true; });
    await sleep(60);
    expect(wrote).toBe(false);
    release();
    await Promise.all([reading, writing]);
    expect(await first.getFresh('count')).toBe(25);
  }, 25000);

  it('reconciles a missed cluster notification and preserves expiration', async () => {
    const writer = openMap('cluster-resync');
    const other = openMap('cluster-resync');
    await Promise.all([writer.ready(), other.ready()]);
    const subscriber = (other as unknown as { subscriber: Cluster }).subscriber;
    await subscriber.unsubscribe('syncedmap:{cluster-resync}:db:0:events');
    await writer.set('missing', 99, { ttlMs: 1500 });
    expect(other.get('missing')).toBeUndefined();
    await other.resync();
    expect(other.get('missing')).toBe(99);
    await waitUntil(async () => other.size === 0, 4000);
    expect(await other.getFresh('missing')).toBeUndefined();
  }, 12000);

  it('repairs missed messages after only the internal Cluster Pub/Sub socket reconnects', async () => {
    const writer = openMap('cluster-subscriber-recovery');
    const reader = openMap('cluster-subscriber-recovery');
    await Promise.all([writer.ready(), reader.ready()]);
    const cluster = (reader as unknown as { subscriber: Cluster }).subscriber;
    const internal = (cluster as unknown as {
      subscriber: {
        getInstance(): { disconnect(): void };
        selectSubscriber(): void;
      };
    }).subscriber;
    expect(internal.getInstance()).toBeDefined();
    let outerReadyEvents = 0;
    const countReady = () => { outerReadyEvents++; };
    cluster.on('ready', countReady);
    try {
      for (let i = 1; i <= 2; i++) {
        const socket = internal.getInstance();
        const originalSelect = internal.selectSubscriber.bind(internal);
        let attemptedReconnect = false;
        internal.selectSubscriber = () => { attemptedReconnect = true; };
        try {
          socket.disconnect();
          await waitUntil(async () => attemptedReconnect);
          // This write occurs while the actual subscription is absent.
          await writer.set('missed-' + i, 30 + i);
          expect(reader.get('missed-' + i)).toBeUndefined();
        } finally {
          internal.selectSubscriber = originalSelect;
          originalSelect();
        }
        await waitUntil(async () => reader.get('missed-' + i) === 30 + i, 8000);
      }
      // Cluster's own ready event is not emitted for physical Pub/Sub
      // replacement, so it cannot be the only resync trigger.
      expect(outerReadyEvents).toBe(0);
    } finally {
      cluster.removeListener('ready', countReady);
    }
  }, 25000);

  it('repairs missed events when the SAME internal subscriber automatically reconnects', async () => {
    const writer = openMap('cluster-same-subscriber-reconnect');
    const reader = openMap('cluster-same-subscriber-reconnect');
    await Promise.all([writer.ready(), reader.ready()]);
    const cluster = (reader as unknown as { subscriber: Cluster }).subscriber;
    const internal = (cluster as unknown as {
      subscriber: {
        getInstance(): {
          status: string;
          connector?: { stream?: { destroy(): void } };
          options: { retryStrategy?: (times: number) => number | null };
          once(event: 'close' | 'reconnecting' | 'ready', listener: () => void): void;
        };
      };
    }).subscriber;
    const socket = internal.getInstance();
    expect(socket).toBeDefined();
    expect(socket.connector?.stream).toBeDefined();
    const previousStrategy = socket.options.retryStrategy;
    let replaced = false;
    let readyEvents = 0;
    const watchReplacement = setInterval(() => {
      if (internal.getInstance() !== socket) replaced = true;
    }, 5);
    const onReady = () => { readyEvents++; };
    cluster.on('ready', onReady);
    // Newer ioredis defaults to replacing the socket. A retryStrategy
    // simulates the same-socket reconnect used by the supported 5.0 client.
    socket.options.retryStrategy = () => 180;
    const disconnected = new Promise<void>(resolve => socket.once('close', resolve));
    try {
      socket.connector!.stream!.destroy();
      await disconnected;
      await writer.set('missed', 66);
      expect(reader.get('missed')).toBeUndefined();
      await waitUntil(async () => reader.get('missed') === 66, 9000);
      expect(replaced).toBe(false);
      expect(internal.getInstance()).toBe(socket);
      expect(readyEvents).toBe(0);
    } finally {
      clearInterval(watchReplacement);
      cluster.removeListener('ready', onReady);
      socket.options.retryStrategy = previousStrategy;
    }
  }, 15000);

  it('moves Pub/Sub off a permanently unavailable retry endpoint', async () => {
    const writer = openMap('cluster-stranded-subscriber');
    const reader = openMap('cluster-stranded-subscriber');
    await Promise.all([writer.ready(), reader.ready()]);
    const cluster = (reader as unknown as { subscriber: Cluster }).subscriber;
    const internal = (cluster as unknown as {
      subscriber: {
        getInstance(): {
          options: { port: number; retryStrategy?: (times: number) => number | null };
          connector?: { stream?: { destroy(): void } };
          once(event: 'close', callback: () => void): void;
        };
      };
    }).subscriber;
    const strandedSocket = internal.getInstance();
    const originalPort = strandedSocket.options.port;
    const originalRetry = strandedSocket.options.retryStrategy;
    expect(strandedSocket.connector?.stream).toBeDefined();
    // This particular Pub/Sub endpoint is unreachable on every retry.
    // The rest of the Redis Cluster remains fully healthy.
    strandedSocket.options.port = 1;
    strandedSocket.options.retryStrategy = () => 100;
    const close = new Promise<void>(resolve => strandedSocket.once('close', resolve));
    let outerReady = 0;
    const onOuterReady = () => { outerReady++; };
    cluster.on('ready', onOuterReady);
    try {
      strandedSocket.connector!.stream!.destroy();
      await close;
      await writer.set('missed', 91);
      expect(reader.get('missed')).toBeUndefined();
      await waitUntil(async () => reader.get('missed') === 91, 10000);
      expect(internal.getInstance()).not.toBe(strandedSocket);
      expect(outerReady).toBe(0);
    } finally {
      cluster.removeListener('ready', onOuterReady);
      strandedSocket.options.retryStrategy = originalRetry;
      strandedSocket.options.port = originalPort;
    }
  }, 18000);

  it('recovers with real ioredis 5.0.0 when both background refresh intervals are disabled', async () => {
    // The library supports the entire ioredis 5.x range. The 5.0.0 Cluster
    // subscriber retries the same dead endpoint indefinitely, without
    // emitting a top-level ready event. Exercise the actual older package,
    // not only a simulated retry with the newer client.
    const legacyVersion = (await import('ioredis-legacy/package.json')).version;
    expect(legacyVersion).toBe('5.0.0');
    const createLegacy = () => {
      const client = new LegacyCluster(
        [{ host: '127.0.0.1', port: basePort }],
        { slotsRefreshInterval: 0 }
      );
      client.on('error', () => undefined);
      clients.push(client as unknown as Cluster);
      return client as unknown as Cluster;
    };
    const namespace = 'legacy-5-no-refresh';
    const writer = new SyncedMap<string, number>({
      namespace, redis: { client: createLegacy() }, resyncIntervalMs: 0,
    });
    const reader = new SyncedMap<string, number>({
      namespace, redis: { client: createLegacy() }, resyncIntervalMs: 0,
    });
    maps.push(writer, reader);
    await Promise.all([writer.ready(), reader.ready()]);
    await writer.set('baseline', 10);
    await waitUntil(async () => reader.get('baseline') === 10);

    const cluster = (reader as unknown as { subscriber: Cluster }).subscriber;
    const internal = (cluster as unknown as {
      subscriber: {
        getInstance(): {
          options: { port: number; retryStrategy?: (times: number) => number | null };
          connector?: { stream?: { destroy(): void } };
          once(event: 'close', listener: () => void): void;
        };
      };
    }).subscriber;
    const socket = internal.getInstance();
    expect(socket.connector?.stream).toBeDefined();
    const oldPort = socket.options.port;
    const oldRetryStrategy = socket.options.retryStrategy;
    let outerReadyEvents = 0;
    const onOuterReady = () => { outerReadyEvents++; };
    cluster.on('ready', onOuterReady);
    socket.options.port = 1;
    socket.options.retryStrategy = () => 100;
    try {
      const disconnected = new Promise<void>(resolve => socket.once('close', resolve));
      socket.connector!.stream!.destroy();
      await disconnected;
      await writer.set('missed', 91);
      expect(reader.get('missed')).toBeUndefined();
      await waitUntil(async () => reader.get('missed') === 91, 10000);
      expect(internal.getInstance()).not.toBe(socket);
      expect(outerReadyEvents).toBe(0);
    } finally {
      cluster.removeListener('ready', onOuterReady);
      socket.options.port = oldPort;
      socket.options.retryStrategy = oldRetryStrategy;
    }
  }, 18000);

  it('shares one command-client error observer across many namespaces', async () => {
    const client = newClient();
    await client.ping();
    const baseline = client.listenerCount('error');
    const mapsForClient = Array.from({ length: 15 }, (_, i) =>
      openMap('cluster-shared-observers-' + i, client)
    );
    await Promise.all(mapsForClient.map(map => map.ready()));
    expect(client.listenerCount('error')).toBe(baseline + 1);
    await Promise.all(mapsForClient.map(map => map.dispose()));
    expect(client.listenerCount('error')).toBe(baseline);
  }, 20000);

  it('rejects both top-level and nested Cluster keyPrefix options', async () => {
    for (const options of [
      { keyPrefix: 'bad:', lazyConnect: true },
      { redisOptions: { keyPrefix: 'bad:' }, lazyConnect: true },
    ]) {
      const client = new Cluster([{ host: '127.0.0.1', port: basePort }], options);
      client.on('error', () => undefined);
      clients.push(client);
      const map = openMap('cluster-prefixed-key-rejection', client);
      try {
        await expect(map.ready()).rejects.toThrow(/keyPrefix/);
      } finally {
        await map.dispose();
      }
    }
  }, 12000);

  it('rejects replica-read routing that would make authoritative reads stale', async () => {
    const client = new Cluster(
      [{ host: '127.0.0.1', port: basePort }],
      { scaleReads: 'slave' }
    );
    client.on('error', () => undefined);
    clients.push(client);
    const map = openMap('cluster-replica-read-rejection', client);
    await expect(map.ready()).rejects.toThrow(/scaleReads=master/);
    await map.dispose();
  }, 12000);

  it('checks every discovered Cluster primary for unsafe eviction', async () => {
    const client = newClient();
    await client.ping();
    const otherPrimary = client.nodes('master')[1]!;
    try {
      await otherPrimary.config('SET', 'maxmemory', '67108864');
      await otherPrimary.config('SET', 'maxmemory-policy', 'allkeys-lru');
      const map = openMap('cluster-other-primary-eviction', client);
      await expect(map.ready()).rejects.toThrow(/maxmemory-policy/);
      await map.dispose();
    } finally {
      await otherPrimary.config('SET', 'maxmemory-policy', 'noeviction');
      await otherPrimary.config('SET', 'maxmemory', '0');
    }
  }, 12000);

  it('refuses scans while Redis Cluster slots are actively migrating', async () => {
    const namespace = 'cluster-active-slot-migration';
    const client = newClient();
    const map = openMap(namespace, client);
    await map.ready();
    await map.set('preserve', 72);
    const key = 'syncedmap:{' + namespace + '}:entry:preserve';
    const slot = Number(await client.cluster('KEYSLOT', key));
    const sourcePort = Number(client.slots[slot]![0]!.split(':').at(-1));
    const destination = client.nodes('master').find(node => node.options.port !== sourcePort);
    expect(destination).toBeDefined();
    const destinationPort = destination!.options.port!;
    const sourceId = await cli(sourcePort, 'CLUSTER', 'MYID');
    const destinationId = await cli(destinationPort, 'CLUSTER', 'MYID');
    const originalSlots = await client.cluster('SLOTS');
    try {
      expect(await cli(destinationPort, 'CLUSTER', 'SETSLOT', String(slot), 'IMPORTING', sourceId)).toBe('OK');
      expect(await cli(sourcePort, 'CLUSTER', 'SETSLOT', String(slot), 'MIGRATING', destinationId)).toBe('OK');
      const nodeState = await cli(sourcePort, 'CLUSTER', 'NODES');
      expect(nodeState).toContain('[' + slot + '->-' + destinationId + ']');
      // Redis still advertises the same slot owner while migrating: SLOTS
      // comparison alone cannot detect this unstable scan.
      expect(await client.cluster('SLOTS')).toEqual(originalSlots);
      await expect(map.resync()).rejects.toThrow(/slot migration/);
      await expect(map.clear()).rejects.toThrow(/slot migration/);
      expect(map.get('preserve')).toBe(72);
      expect(await map.getFresh('preserve')).toBe(72);
    } finally {
      await cli(sourcePort, 'CLUSTER', 'SETSLOT', String(slot), 'STABLE');
      await cli(destinationPort, 'CLUSTER', 'SETSLOT', String(slot), 'STABLE');
    }
    await map.resync();
    await map.clear();
    expect(await map.getFresh('preserve')).toBeUndefined();
  }, 20000);

  it('retries clear target collection when Cluster topology changes during the scan', async () => {
    const client = newClient();
    const map = openMap('cluster-clear-topology-retry', client);
    await map.ready();
    await map.set('existing', 44);
    const raw = client as unknown as { cluster(command: string): Promise<unknown> };
    const originalTopology = raw.cluster.bind(client);
    let topologyCalls = 0;
    raw.cluster = async command => {
      const value = await originalTopology(command);
      if (command === 'SLOTS' && ++topologyCalls === 2) return ['topology changed'];
      return value;
    };
    const primaries = client.nodes('master');
    const originalScans = primaries.map(node => node.scan.bind(node));
    let scans = 0;
    for (let i = 0; i < primaries.length; i++) {
      const node = primaries[i]!;
      const scan = originalScans[i]!;
      node.scan = ((...args: Parameters<typeof node.scan>) => {
        scans++;
        if (topologyCalls < 2) return Promise.resolve(['0', []]);
        return scan(...args);
      }) as typeof node.scan;
    }
    try {
      await map.clear();
      expect(topologyCalls).toBeGreaterThanOrEqual(4);
      expect(scans).toBeGreaterThan(primaries.length);
      expect(await map.getFresh('existing')).toBeUndefined();
    } finally {
      raw.cluster = originalTopology;
      for (let i = 0; i < primaries.length; i++) {
        primaries[i]!.scan = originalScans[i]! as typeof primaries[number]['scan'];
      }
    }
  }, 20000);

  it('rejects clear without deletion if Cluster keeps changing its topology', async () => {
    const client = newClient();
    const map = openMap('cluster-clear-topology-churn', client);
    await map.ready();
    await map.set('keep', 83);
    const raw = client as unknown as { cluster(command: string): Promise<unknown> };
    const original = raw.cluster.bind(client);
    let generation = 0;
    raw.cluster = async command =>
      command === 'SLOTS' ? { generation: ++generation } : original(command);
    try {
      await expect(map.clear()).rejects.toThrow(/topology changed repeatedly/);
      expect(await map.getFresh('keep')).toBe(83);
    } finally {
      raw.cluster = original;
    }
  }, 15000);

  it('refreshes the Cluster primary inventory before clear with automatic slot refresh disabled', async () => {
    const client = new Cluster(
      [{ host: '127.0.0.1', port: basePort }],
      { slotsRefreshInterval: 0 }
    );
    client.on('error', () => undefined);
    clients.push(client);
    const namespace = 'cluster-inventory-clear';
    const map = openMap(namespace, client);
    await map.ready();
    await map.set('required', 123);
    const originalNodes = client.nodes.bind(client);
    const slot = Number(await client.cluster('KEYSLOT', 'syncedmap:{cluster-inventory-clear}:entry:required'));
    const owner = client.slots[slot]?.[0];
    expect(owner).toBeDefined();
    const ownerPort = Number(owner!.split(':').at(-1));
    const omittedPrimary = originalNodes('master').find(node => node.options.port === ownerPort);
    expect(omittedPrimary).toBeDefined();
    const originalRefresh = client.refreshSlotsCache.bind(client);
    let refreshed = false;
    let refreshCalls = 0;
    // Simulate a caller with an obsolete pool lacking the master that owns
    // our namespace. A plain CLUSTER SLOTS command does not refresh that pool.
    client.nodes = ((role: 'all' | 'master' | 'slave' = 'all') => {
      const nodes = originalNodes(role);
      if (!refreshed && role === 'master') {
        return nodes.filter(node => node !== omittedPrimary);
      }
      return nodes;
    }) as typeof client.nodes;
    client.refreshSlotsCache = ((callback?: (error?: Error | null) => void) => {
      refreshCalls++;
      originalRefresh(error => {
        if (!error) refreshed = true;
        callback?.(error);
      });
    }) as typeof client.refreshSlotsCache;
    try {
      await map.clear();
      expect(refreshCalls).toBeGreaterThanOrEqual(2);
      expect(await map.getFresh('required')).toBeUndefined();
    } finally {
      client.nodes = originalNodes as typeof client.nodes;
      client.refreshSlotsCache = originalRefresh as typeof client.refreshSlotsCache;
    }
  }, 20000);

  it('retries a scan that missed keys during a Cluster topology transition', async () => {
    const client = newClient();
    const writer = openMap('cluster-topology-retry', client);
    await writer.ready();
    await writer.set('must-survive', 43);

    const readerClient = newClient();
    const reader = openMap('cluster-topology-retry', readerClient);
    await reader.ready();
    expect(reader.get('must-survive')).toBe(43);

    const raw = readerClient as unknown as { cluster(command: string): Promise<unknown> };
    const originalTopology = raw.cluster.bind(readerClient);
    let topologyCalls = 0;
    raw.cluster = async command => {
      const value = await originalTopology(command);
      if (command === 'SLOTS' && ++topologyCalls === 2) return ['changed during scan'];
      return value;
    };
    const primaries = readerClient.nodes('master');
    const originalScans = primaries.map(node => node.scan.bind(node));
    let scanCalls = 0;
    for (let index = 0; index < primaries.length; index++) {
      const node = primaries[index]!;
      const scan = originalScans[index]!;
      node.scan = ((...args: Parameters<typeof node.scan>) => {
        scanCalls++;
        // The first pass skipped the record, as a moving slot could.
        if (topologyCalls < 2) return Promise.resolve(['0', []]);
        return scan(...args);
      }) as typeof node.scan;
    }
    const changes: string[] = [];
    const stop = reader.subscribe(change => changes.push(change.type));
    try {
      await reader.resync();
      expect(reader.get('must-survive')).toBe(43);
      expect(scanCalls).toBeGreaterThan(primaries.length);
      expect(changes).toEqual(['resync']);
    } finally {
      stop();
      raw.cluster = originalTopology;
      for (let index = 0; index < primaries.length; index++) {
        primaries[index]!.scan = originalScans[index]! as typeof primaries[number]['scan'];
      }
    }
  }, 15000);

  it('rejects perpetually unstable topology without replacing a good replica', async () => {
    const client = newClient();
    const reader = openMap('cluster-topology-unstable', client);
    await reader.ready();
    await reader.set('preserve', 81);

    const raw = client as unknown as { cluster(command: string): Promise<unknown> };
    const original = raw.cluster.bind(client);
    let generation = 0;
    raw.cluster = async command =>
      command === 'SLOTS' ? { generation: ++generation } : original(command);

    const notifications: string[] = [];
    const stop = reader.subscribe(change => notifications.push(change.type));
    try {
      await expect(reader.resync()).rejects.toThrow(/topology changed repeatedly/);
      expect(reader.get('preserve')).toBe(81);
      expect(notifications).toEqual([]);
      expect(generation).toBeGreaterThanOrEqual(6);
    } finally {
      stop();
      raw.cluster = original;
    }
    await reader.resync();
    expect(reader.get('preserve')).toBe(81);
  }, 15000);

  it('replays live updates arriving during the final topology check', async () => {
    const writer = openMap('cluster-topology-replay');
    const client = newClient();
    const replica = openMap('cluster-topology-replay', client);
    await Promise.all([writer.ready(), replica.ready()]);
    await writer.set('live', 1);
    await waitUntil(async () => replica.get('live') === 1);

    const raw = client as unknown as { cluster(command: string): Promise<unknown> };
    const original = raw.cluster.bind(client);
    let probes = 0;
    let unblock!: () => void;
    const gate = new Promise<void>(resolve => { unblock = resolve; });
    let signalEntered!: () => void;
    const entered = new Promise<void>(resolve => { signalEntered = resolve; });
    raw.cluster = async command => {
      if (command === 'SLOTS' && ++probes === 2) {
        signalEntered();
        await gate;
      }
      return original(command);
    };
    try {
      const scanning = replica.resync();
      await entered;
      await writer.set('live', 2);
      await waitUntil(async () => replica.get('live') === 2);
      unblock();
      await scanning;
      expect(replica.get('live')).toBe(2);
      expect(await replica.getFresh('live')).toBe(2);
    } finally {
      unblock();
      raw.cluster = original;
    }
  }, 15000);

  it('replicates and serializes updates between separate Node processes', async () => {
    const namespace = 'cluster-real-processes';
    const main = openMap(namespace);
    await main.ready();
    await main.set('counter', 0);

    const fixture = fileURLToPath(new URL('./fixtures/peer.mjs', import.meta.url));
    const child = spawn(process.execPath, [fixture, 'cluster://127.0.0.1:' + basePort, namespace], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', chunk => { stderr += String(chunk); });
    let ready!: () => void;
    const isReady = new Promise<void>(resolve => { ready = resolve; });
    const pending = new Map<number, { resolve(value: number): void; reject(error: Error): void }>();
    let nextId = 0;
    createInterface({ input: child.stdout }).on('line', line => {
      const msg = JSON.parse(line) as { ready?: boolean; id?: number; result?: number; error?: string };
      if (msg.ready) { ready(); return; }
      if (msg.id === undefined) return;
      const waiter = pending.get(msg.id);
      if (!waiter) return;
      pending.delete(msg.id);
      if (msg.error) waiter.reject(new Error(msg.error));
      else waiter.resolve(msg.result ?? 0);
    });
    const command = (op: string, extra: Record<string, unknown>): Promise<number> =>
      new Promise((resolve, reject) => {
        const id = ++nextId;
        pending.set(id, { resolve, reject });
        child.stdin.write(JSON.stringify({ id, op, ...extra }) + '\n');
      });
    try {
      await Promise.race([
        isReady,
        sleep(5000).then(() => { throw new Error('Cluster peer failed to start: ' + stderr); }),
      ]);
      await Promise.all([
        main.update('counter', n => (n ?? 0) + 1),
        command('update', { key: 'counter', count: 12 }),
      ]);
      expect(await main.getFresh('counter')).toBe(13);
      await command('set', { key: 'other-process', value: 73 });
      await waitUntil(async () => main.get('other-process') === 73);
      expect(await command('get', { key: 'counter' })).toBe(13);
      await command('shutdown', {});
    } finally {
      child.stdin.end();
      if (child.exitCode === null) child.kill('SIGTERM');
    }
  }, 20000);

  it('recovers after a planned promotion of the key-owning master replica', async () => {
    const map = openMap('cluster-failover');
    const peer = openMap('cluster-failover');
    await Promise.all([map.ready(), peer.ready()]);
    await map.set('critical', 101);
    await waitUntil(async () => peer.get('critical') === 101);

    const client = clients[clients.length - 1]!;
    const slot = Number(await client.cluster('KEYSLOT', 'syncedmap:{cluster-failover}:entry:critical'));
    const owner = client.slots[slot]?.[0];
    expect(owner).toBeTruthy();
    const ownerPort = Number(owner!.split(':').at(-1));
    const topology = await cli(basePort, 'CLUSTER', 'NODES');
    const lines = topology.split('\n').map(line => line.trim().split(' '));
    const primary = lines.find(parts => Number(parts[1]?.split(':')[1]?.split('@')[0]) === ownerPort);
    expect(primary).toBeDefined();
    const replica = lines.find(parts => parts[3] === primary![0] && parts[2]?.includes('slave'));
    expect(replica).toBeDefined();
    const replicaPort = Number(replica![1]!.split(':')[1]!.split('@')[0]);
    expect(await cli(replicaPort, 'CLUSTER', 'FAILOVER')).toBe('OK');
    await waitUntil(async () => {
      const nodes = await cli(replicaPort, 'CLUSTER', 'NODES');
      return nodes.split('\n').some(line =>
        line.startsWith(replica![0] + ' ') && line.includes('myself,master')
      );
    }, 16000);
    // The cluster client refreshes its routing and the live application
    // continues to use the same namespace after the promoted primary takes over.
    await waitUntil(async () => {
      try { return (await map.getFresh('critical')) === 101; } catch { return false; }
    }, 16000);
    await peer.update('critical', n => (n ?? 0) + 1);
    expect(await map.getFresh('critical')).toBe(102);
    // A key may change shard ownership without increasing its revision.
    // Even after another write has caused the new replica to advance, the
    // prior topology must invalidate the periodic revision-only shortcut.
    const observer = peer as unknown as {
      lastHydrationTopology?: string;
      runResync(periodic: boolean): Promise<void>;
    };
    const topologyBefore = observer.lastHydrationTopology;
    await observer.runResync(true);
    expect(observer.lastHydrationTopology).toBeDefined();
    expect(observer.lastHydrationTopology).not.toBe(topologyBefore);
  }, 35000);
  it('continues on surviving Redis Cluster nodes after an unexpected primary crash', async () => {
    const writer = openMap('cluster-crash-failover');
    const follower = openMap('cluster-crash-failover');
    await Promise.all([writer.ready(), follower.ready()]);
    await writer.set('initial', 11);
    await waitUntil(async () => follower.get('initial') === 11);

    const clusterClient = clients[clients.length - 1]!;
    const slot = Number(await clusterClient.cluster(
      'KEYSLOT', 'syncedmap:{cluster-crash-failover}:entry:initial'
    ));
    const owner = clusterClient.slots[slot]?.[0];
    expect(owner).toBeDefined();
    const crashedPort = Number(owner!.split(':').at(-1));
    const server = processes[crashedPort - basePort];
    expect(server).toBeDefined();
    expect(server!.kill('SIGKILL')).toBe(true);
    const survivor = crashedPort === basePort ? basePort + 1 : basePort;

    await waitUntil(async () =>
      (await cli(survivor, 'CLUSTER', 'INFO')).includes('cluster_state:ok'),
    25000);
    await waitUntil(async () =>
      clusterClient.slots[slot]?.[0] !== owner &&
      clusterClient.slots[slot]?.[0] !== undefined,
    25000);
    await writer.set('after-recovery', 78);
    await waitUntil(async () => follower.get('after-recovery') === 78, 15000);
    expect(await follower.getFresh('after-recovery')).toBe(78);
    await follower.update('after-recovery', v => (v ?? 0) + 1);
    expect(await writer.getFresh('after-recovery')).toBe(79);
  }, 45000);
});
