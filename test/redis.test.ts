import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createServer } from 'node:net';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import Redis from 'ioredis';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { SyncedMap } from '../src/index.js';

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
let server: ChildProcessWithoutNullStreams;
let url: string;
const maps: Array<SyncedMap<string, number>> = [];
const backgroundErrors: Error[] = [];

async function createRedisServer(): Promise<string> {
  const probe = createServer();
  await new Promise<void>((resolve, reject) => {
    probe.once('error', reject);
    probe.listen(0, '127.0.0.1', resolve);
  });
  const address = probe.address();
  if (!address || typeof address === 'string') throw new Error('Unable to reserve Redis port.');
  const port = address.port;
  await new Promise<void>(resolve => probe.close(() => resolve()));
  server = spawn('redis-server', [
    '--bind', '127.0.0.1', '--port', String(port),
    '--save', '', '--appendonly', 'no', '--protected-mode', 'yes',
  ]);
  let stderr = '';
  server.stderr.on('data', chunk => { stderr += String(chunk); });
  const target = 'redis://127.0.0.1:' + String(port);
  for (let attempt = 0; attempt < 80; attempt++) {
    const client = new Redis(target, {
      lazyConnect: true, retryStrategy: () => null, maxRetriesPerRequest: 1,
      connectTimeout: 500,
    });
    client.on('error', () => undefined);
    try {
      await client.connect();
      if (await client.ping() === 'PONG') {
        client.disconnect();
        return target;
      }
    } catch {
      client.disconnect();
    }
    if (server.exitCode !== null) throw new Error('Redis server exited: ' + stderr);
    await sleep(40);
  }
  throw new Error('Redis server did not become ready: ' + stderr);
}

function openMap(namespace: string): SyncedMap<string, number> {
  const map = new SyncedMap<string, number>({
    namespace, redis: { url },
    onError: error => { backgroundErrors.push(error); },
  });
  maps.push(map);
  return map;
}

async function eventually(check: () => boolean, timeout = 3000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (check()) return;
    await sleep(10);
  }
  expect(check()).toBe(true);
}

describe('real Redis replication', () => {
  beforeAll(async () => { url = await createRedisServer(); }, 15000);
  afterEach(() => {
    expect(backgroundErrors.map(error => error.message)).toEqual([]);
    backgroundErrors.length = 0;
  });
  afterAll(async () => {
    for (const map of maps) await map.dispose();
    if (server && server.exitCode === null) {
      server.kill('SIGTERM');
      await new Promise<void>(resolve => {
        if (server.exitCode !== null) return resolve();
        server.once('exit', () => resolve());
      });
    }
  }, 15000);

  it('hydrates late-joining peers and pushes set/delete events', async () => {
    const namespace = 'replication';
    const first = openMap(namespace);
    await first.ready();
    await first.set('hello', 12);
    const second = openMap(namespace);
    await second.ready();
    expect(second.get('hello')).toBe(12);
    const changes: string[] = [];
    second.subscribe(change => changes.push(change.type));
    await first.set('hello', 17);
    await eventually(() => second.get('hello') === 17);
    await first.delete('hello');
    await eventually(() => second.get('hello') === undefined);
    expect(changes).toContain('set');
    expect(changes).toContain('delete');
  });

  it('uses the mutex and atomic version check for concurrent updates', async () => {
    const first = openMap('concurrent');
    const second = openMap('concurrent');
    await Promise.all([first.ready(), second.ready()]);
    await first.set('count', 0);
    await Promise.all([
      ...Array.from({ length: 25 }, () => first.update('count', value => (value ?? 0) + 1)),
      ...Array.from({ length: 25 }, () => second.update('count', value => (value ?? 0) + 1)),
    ]);
    expect(await first.getFresh('count')).toBe(50);
    await eventually(() => second.get('count') === 50);
  }, 20000);

  it('gets authoritative freshness when subscription is disconnected and recovers on reconnection', async () => {
    const first = openMap('disconnect');
    const second = openMap('disconnect');
    await Promise.all([first.ready(), second.ready()]);
    const subscriber = (second as unknown as { subscriber: Redis }).subscriber;
    subscriber.disconnect();
    await first.set('fresh', 90);
    expect(second.get('fresh')).toBeUndefined();
    expect(await second.readLocked('fresh', async value => value)).toBe(90);
    await first.set('other', 99);
    expect(second.get('other')).toBeUndefined();
    await subscriber.connect();
    await eventually(() => second.get('other') === 99);
  }, 12000);

  it('blocks writes from another Redis-connected node throughout readLocked', async () => {
    const first = openMap('locked-read');
    const second = openMap('locked-read');
    await Promise.all([first.ready(), second.ready()]);
    await first.set('critical', 10);
    let release: () => void = () => undefined;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let signalEntered: () => void = () => undefined;
    const entered = new Promise<void>(resolve => { signalEntered = resolve; });
    const reading = second.readLocked('critical', async (current, { assertLock }) => {
      expect(current).toBe(10);
      assertLock();
      signalEntered();
      await gate;
      assertLock();
    });
    await entered;
    let finished = false;
    const writing = first.set('critical', 11).then(() => { finished = true; });
    await sleep(55);
    expect(finished).toBe(false);
    release();
    await Promise.all([reading, writing]);
    expect(await second.getFresh('critical')).toBe(11);
  }, 12000);

  it('clears all existing keys when no concurrent writers are present', async () => {
    const map = openMap('clearing');
    await map.ready();
    for (let i = 0; i < 25; i++) await map.set('key-' + i, i);
    expect(map.size).toBe(25);
    await map.clear();
    expect(map.size).toBe(0);
    await map.resync();
    expect(map.size).toBe(0);
  }, 12000);

  it('rejects a late stale notification and uses Redis expiry', async () => {
    const first = openMap('revisions');
    const second = openMap('revisions');
    await Promise.all([first.ready(), second.ready()]);
    await first.set('ordered', 123);
    await first.set('ordered', 456);
    await eventually(() => second.get('ordered') === 456);
    const redis = new Redis(url);
    try {
      const channel = 'syncedmap:{revisions}:events';
      await redis.publish(channel, JSON.stringify({
        key: 'ordered', revision: 1, record: '1|0|0', origin: 'fake-old-peer',
      }));
      await sleep(40);
      expect(second.get('ordered')).toBe(456);

      await first.set('short', 5, { ttlMs: 65 });
      await eventually(() => second.get('short') === 5);
      await sleep(100);
      expect(first.get('short')).toBeUndefined();
      expect(second.get('short')).toBeUndefined();
      expect(await second.getFresh('short')).toBeUndefined();
      expect(await redis.exists('syncedmap:{revisions}:entry:short')).toBe(0);
    } finally {
      redis.disconnect();
    }
  });

  it('reuses a supplied Redis client without closing it and supports nested Redis locks', async () => {
    const client = new Redis(url);
    const map = new SyncedMap<string, number>({
      namespace: 'supplied-client',
      redis: { client },
    });
    try {
      await map.ready();
      await map.set('nested', 1);
      await map.readLocked('nested', async current => {
        expect(current).toBe(1);
        await map.set('nested', 2);
      });
      expect(await map.getFresh('nested')).toBe(2);
      await map.dispose();
      expect(await client.ping()).toBe('PONG');
    } finally {
      await map.dispose();
      client.disconnect();
    }
  }, 12000);

  it('keeps glob characters in namespace names separate during hydration and clear', async () => {
    const wildcard = openMap('client*');
    const literal = openMap('clientX');
    await Promise.all([wildcard.ready(), literal.ready()]);
    await wildcard.set('shared', 10);
    await literal.set('shared', 20);
    await wildcard.set('only-wildcard', 101);
    await literal.set('only-literal', 202);
    const late = openMap('client*');
    await late.ready();
    expect(late.get('shared')).toBe(10);
    expect(late.get('only-literal')).toBeUndefined();
    await wildcard.clear();
    expect(await literal.getFresh('shared')).toBe(20);
    expect(await literal.getFresh('only-literal')).toBe(202);
  });

  it('rejects externally prefixed command clients and removes owned listeners after dispose', async () => {
    const prefixed = new Redis(url, { keyPrefix: 'other:' });
    const rejected = new SyncedMap({ namespace: 'prefix-reject', redis: { client: prefixed } });
    await expect(rejected.ready()).rejects.toThrow(/keyPrefix/);
    await rejected.dispose();
    expect(await prefixed.ping()).toBe('PONG');
    prefixed.disconnect();

    const client = new Redis(url);
    const before = client.listenerCount('error');
    const map = new SyncedMap({ namespace: 'listener-cleanup', redis: { client } });
    await map.ready();
    expect(client.listenerCount('error')).toBe(before + 1);
    await map.dispose();
    expect(client.listenerCount('error')).toBe(before);
    expect(await client.ping()).toBe('PONG');
    client.disconnect();
  }, 12000);

  it('continues authoritative operations while a subscriber resync is stalled', async () => {
    const map = openMap('subscriber-stalled');
    await map.ready();
    await map.set('counter', 1);
    const subscriber = (map as unknown as { subscriber: Redis }).subscriber;
    const original = subscriber.subscribe.bind(subscriber);
    subscriber.subscribe = (async () => new Promise<never>(() => undefined)) as typeof subscriber.subscribe;
    // Resync is now waiting for subscriber rather than the healthy command client.
    void map.resync();
    await sleep(30);
    const command = (async () => {
      expect(await map.getFresh('counter')).toBe(1);
      await map.set('counter', 2);
      expect(map.get('counter')).toBe(2);
      expect(await map.readLocked('counter', async value => value)).toBe(2);
    })();
    await Promise.race([
      command,
      sleep(2000).then(() => { throw new Error('Foreground operations blocked by subscriber'); }),
    ]);
    subscriber.subscribe = original;
  }, 12000);

  it('does not create resources after immediate disposal during initialization', async () => {
    const map = openMap('dispose-during-init');
    await map.dispose();
    await expect(map.ready()).rejects.toThrow(/disposed/);
    expect((map as unknown as { resyncTimer?: unknown }).resyncTimer).toBeUndefined();
    await map.dispose(); // Idempotent
  }, 12000);

  it('finishes disposal even if subscription during initialization never resolves', async () => {
    const client = new Redis(url);
    const originalDuplicate = client.duplicate.bind(client);
    let createdSubscriber: Redis | undefined;
    client.duplicate = ((options?: { lazyConnect?: boolean }) => {
      const subscriber = originalDuplicate(options);
      createdSubscriber = subscriber;
      subscriber.subscribe = (async () => new Promise<never>(() => undefined)) as typeof subscriber.subscribe;
      return subscriber;
    }) as typeof client.duplicate;
    const map = new SyncedMap<string, number>({
      namespace: 'stalled-init-disposal',
      redis: { client },
    });
    try {
      await eventually(() => Boolean(createdSubscriber));
      await Promise.race([
        map.dispose(),
        sleep(1500).then(() => { throw new Error('dispose blocked on a stalled subscriber'); }),
      ]);
      await expect(map.ready()).rejects.toThrow(/disposed/);
      // An in-flight connect may finish on the following tick. It must close
      // itself instead of resurrecting a socket after disposal.
      await sleep(80);
      expect(createdSubscriber?.status).toBe('end');
      expect(await client.ping()).toBe('PONG');
    } finally {
      await map.dispose();
      client.disconnect();
    }
  }, 10000);

  it('compacts buffered events and avoids duplicate notifications during a stale snapshot', async () => {
    const namespace = 'buffered-replay';
    const writer = openMap(namespace);
    await writer.ready();
    await writer.set('anchor', 0);

    const client = new Redis(url);
    const replica = new SyncedMap<string, number>({
      namespace, redis: { client },
      onError: error => { backgroundErrors.push(error); },
    });
    maps.push(replica);
    await replica.ready();
    const observed: Array<{ type: string; key?: string; value?: number }> = [];
    replica.subscribe(change => {
      if (change.type === 'set') observed.push({ type: 'set', key: change.key, value: change.value });
      else observed.push({ type: change.type });
    });

    const originalMget = client.mget.bind(client);
    let releaseSnapshot: () => void = () => undefined;
    const snapshotGate = new Promise<void>(resolve => { releaseSnapshot = resolve; });
    let markSnapshotPaused: () => void = () => undefined;
    const snapshotPaused = new Promise<void>(resolve => { markSnapshotPaused = resolve; });
    let holdOnce = true;
    client.mget = (async (...keys: string[]) => {
      const snapshot = await originalMget(...keys);
      if (holdOnce) {
        holdOnce = false;
        markSnapshotPaused();
        await snapshotGate;
      }
      return snapshot;
    }) as typeof client.mget;

    try {
      const running = replica.resync();
      await snapshotPaused;

      for (let i = 1; i <= 35; i++) await writer.set('anchor', i);
      await writer.set('late', 1001);
      await eventually(() => replica.get('anchor') === 35 && replica.get('late') === 1001);
      expect((replica as unknown as {
        bufferedEvents: Map<string, unknown>;
      }).bufferedEvents.size).toBe(2);

      const priorSetNotifications = observed.filter(change => change.type === 'set');
      expect(priorSetNotifications).toHaveLength(36);
      releaseSnapshot();
      await running;
      expect(replica.get('anchor')).toBe(35);
      expect(replica.get('late')).toBe(1001);
      expect(observed.filter(change => change.type === 'set')).toEqual(priorSetNotifications);
      expect(observed.filter(change => change.type === 'resync')).toHaveLength(1);
      expect((replica as unknown as {
        bufferedEvents: Map<string, unknown>;
      }).bufferedEvents.size).toBe(0);
    } finally {
      releaseSnapshot();
      client.mget = originalMget as typeof client.mget;
      await replica.dispose();
      client.disconnect();
    }
  }, 20000);

  it('keeps already-notified live state after a failed hydration', async () => {
    const namespace = 'buffered-replay-error';
    const writer = openMap(namespace);
    await writer.ready();
    await writer.set('key', 0);

    const client = new Redis(url);
    const replica = new SyncedMap<string, number>({ namespace, redis: { client } });
    maps.push(replica);
    await replica.ready();
    const seen: number[] = [];
    replica.subscribe(change => {
      if (change.type === 'set') seen.push(change.value);
    });

    const originalMget = client.mget.bind(client);
    let releaseSnapshot: () => void = () => undefined;
    const snapshotGate = new Promise<void>(resolve => { releaseSnapshot = resolve; });
    let markSnapshotPaused: () => void = () => undefined;
    const snapshotPaused = new Promise<void>(resolve => { markSnapshotPaused = resolve; });
    let failOnce = true;
    client.mget = (async (...keys: string[]) => {
      if (!failOnce) return originalMget(...keys);
      failOnce = false;
      markSnapshotPaused();
      await snapshotGate;
      throw new Error('Injected hydration failure');
    }) as typeof client.mget;

    try {
      const running = replica.resync();
      await snapshotPaused;
      await writer.set('key', 1);
      await writer.set('key', 2);
      await eventually(() => replica.get('key') === 2);
      releaseSnapshot();
      await expect(running).rejects.toThrow('Injected hydration failure');
      expect(replica.get('key')).toBe(2);
      expect(seen).toEqual([1, 2]);
      expect((replica as unknown as {
        bufferedEvents: Map<string, unknown>;
      }).bufferedEvents.size).toBe(0);
    } finally {
      releaseSnapshot();
      client.mget = originalMget as typeof client.mget;
      await replica.dispose();
      client.disconnect();
    }
  }, 12000);

  it('does not treat Redis-authoritative TTL values as expired due to client clock skew', async () => {
    const map = openMap('redis-clock-skew');
    await map.ready();
    await map.set('still-live', 3, { ttlMs: 7000 });
    const accurateNow = Date.now.bind(Date);
    const clock = vi.spyOn(Date, 'now').mockImplementation(() => accurateNow() + 60_000);
    try {
      expect(await map.getFresh('still-live')).toBe(3);
      expect(await map.readLocked('still-live', async value => value)).toBe(3);
      let received: number | undefined;
      const updated = await map.update('still-live', value => {
        received = value;
        return (value ?? 0) + 1;
      }, { ttlMs: 7000 });
      expect(received).toBe(3);
      expect(updated).toBe(4);
      expect(await map.getFresh('still-live')).toBe(4);
    } finally {
      clock.mockRestore();
    }
  }, 12000);

  it('synchronizes real separate Node processes, not just instances in one process', async () => {
    const namespace = 'processes';
    const main = openMap(namespace);
    await main.ready();
    await main.set('counter', 0);
    const fixture = fileURLToPath(new URL('./fixtures/peer.mjs', import.meta.url));
    const child = spawn(process.execPath, [fixture, url, namespace], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let nextId = 0;
    const pending = new Map<number, { resolve: (value: number) => void; reject: (error: Error) => void }>();
    let signalReady: () => void = () => undefined;
    const isReady = new Promise<void>(resolve => { signalReady = resolve; });
    let childStderr = '';
    child.stderr.on('data', chunk => { childStderr += String(chunk); });
    createInterface({ input: child.stdout }).on('line', line => {
      const message = JSON.parse(line) as { ready?: boolean; id?: number; result?: number; error?: string };
      if (message.ready) { signalReady(); return; }
      if (message.id === undefined) return;
      const item = pending.get(message.id);
      if (!item) return;
      pending.delete(message.id);
      if (message.error) item.reject(new Error(message.error));
      else item.resolve(message.result ?? 0);
    });
    const command = (op: string, extra: Record<string, unknown>): Promise<number> => {
      const id = ++nextId;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        child.stdin.write(JSON.stringify({ id, op, ...extra }) + '\n');
      });
    };
    try {
      await Promise.race([
        isReady,
        sleep(4000).then(() => { throw new Error('Peer did not start: ' + childStderr); }),
      ]);
      await Promise.all([
        main.update('counter', value => (value ?? 0) + 1),
        command('update', { key: 'counter', count: 15 }),
      ]);
      expect(await main.getFresh('counter')).toBe(16);
      await command('set', { key: 'from-child', value: 77 });
      await eventually(() => main.get('from-child') === 77);
      expect(await command('get', { key: 'counter' })).toBe(16);
      await command('shutdown', {});
    } finally {
      child.stdin.end();
      if (child.exitCode === null) child.kill('SIGTERM');
    }
  }, 20000);
});
