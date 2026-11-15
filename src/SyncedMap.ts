import { randomUUID } from 'node:crypto';
import loadOptionalRedis from './redisLoader.cjs';
import { ExpiryQueue } from './expiryQueue.js';
import { UniqueMutexManager } from 'uniquemutexmanager';
import { READ_SCRIPT, WRITE_SCRIPT, encodeRecord, freezeValue, parseRecord, serializeJson } from './redisProtocol.js';
import type { StoredRecord } from './redisProtocol.js';
import { SyncedMapConflictError } from './types.js';
import type {
  SyncedMapChange, SyncedMapLockContext, SyncedMapOptions, SyncedMapSetOptions,
  SyncedMapRedisClient, SyncedMapRedisClusterClient, SyncedMapRedisConnection,
} from './types.js';

type ReplicationEvent<Key extends string> = {
  key: Key;
  revision: number;
  record: string | null;
  origin: string;
};

type CurrentState<Value> = {
  entry?: StoredRecord<Value>;
  expectedRevision: number;
  value: Value | undefined;
};

type WriteAction = 'set' | 'delete';

function isClusterClient(client: SyncedMapRedisConnection): client is SyncedMapRedisClusterClient {
  return 'isCluster' in client && client.isCluster === true && typeof client.nodes === 'function';
}

// ioredis Cluster emits no top-level 'ready' when its *internal* Pub/Sub
// socket is replaced. Observe that physical socket's 'end' instead.
// ClusterSubscriber.getInstance() is present in the supported ioredis 5.x
// implementations; fail initialization rather than silently lose recovery.
type ClusterSubscriptionSocket = {
  on(event: 'end', listener: () => void): unknown;
  removeListener(event: 'end', listener: () => void): unknown;
};

function getClusterSubscriptionSocket(client: SyncedMapRedisClusterClient): ClusterSubscriptionSocket | null {
  const internal = client as unknown as {
    subscriber?: { getInstance?: () => ClusterSubscriptionSocket | null };
  };
  if (typeof internal.subscriber?.getInstance !== 'function') {
    throw new Error('Unsupported ioredis Cluster Pub/Sub implementation: cannot observe subscriber recovery.');
  }
  return internal.subscriber.getInstance();
}

// A supplied Redis/Cluster client can be shared by many maps. Keep one
// EventEmitter listener per connection instead of hitting Node's 10-listener
// warning and repeating the event-handler overhead for every namespace.
const commandErrorObservers = new WeakMap<SyncedMapRedisConnection, {
  dispatch: (error: Error) => void;
  callbacks: Set<(error: Error) => void>;
}>();

function observeCommandErrors(
  client: SyncedMapRedisConnection,
  callback: (error: Error) => void
): () => void {
  let listeners = commandErrorObservers.get(client);
  if (!listeners) {
    const callbacks = new Set<(error: Error) => void>();
    const dispatch = (error: Error) => {
      for (const observer of [...callbacks]) observer(error);
    };
    listeners = { dispatch, callbacks };
    commandErrorObservers.set(client, listeners);
    client.on('error', dispatch);
  }
  listeners.callbacks.add(callback);
  return () => {
    listeners!.callbacks.delete(callback);
    if (listeners!.callbacks.size === 0) {
      client.removeListener('error', listeners!.dispatch);
      commandErrorObservers.delete(client);
    }
  };
}

/** Redis cursor pages are larger than transport value batches. */
const redisScanCount = 1000;
const redisReadBatchSize = 200;

function positiveTtl(ttlMs: number | undefined): number {
  // Zero explicitly disables a default TTL for one write.
  if (ttlMs === undefined || ttlMs === 0) return 0;
  if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
    throw new TypeError('SyncedMap TTL must be a positive integer in milliseconds.');
  }
  return ttlMs;
}

/**
 * A Map-like in-process replica, optionally coordinated through Redis.
 *
 * Local reads are synchronous and can be stale. All writes use the same UMM key
 * as readLocked; Redis writes are atomic, version-checked, and published.
 * Values are JSON-serialized and deeply frozen on ingress. Only strings can be
 * keys, so all processes agree on an unambiguous representation.
 */
export class SyncedMap<Key extends string, Value> implements Iterable<[Key, Value]> {
  private entriesByKey = new Map<Key, StoredRecord<Value>>();
  private versionsByKey = new Map<Key, number>();
  private baselineRevision = 0;
  private localRevision = 0;
  private readonly instanceId = randomUUID();
  private readonly namespace: string;
  private readonly redisPrefix: string;
  private readonly entryPrefix: string;
  private readonly revisionKey: string;
  private eventChannel: string;
  private readonly defaultTtlMs: number;
  private readonly errorHandler?: (error: Error) => void;
  /** Background scans can be skipped on a fully unchanged, non-evictable replica. */
  private lastHydrationRevision?: number;
  private canSkipUnchangedResync = false;
  private lastHydrationTopology?: string;
  private readonly listeners = new Set<(change: SyncedMapChange<Key, Value>) => void>();
  /** Both local and Redis replicas expire cached entries without a full scan. */
  private readonly expiryQueue: ExpiryQueue<Key>;

  private mutex: UniqueMutexManager;
  private redis?: SyncedMapRedisConnection;
  private subscriber?: SyncedMapRedisConnection;
  private ownsRedis = false;
  private redisSubscribed = false;
  private readyFlag = false;
  private disposed = false;
  /** Latest event per key during hydration; bounds memory by changed key count. */
  private bufferedEvents = new Map<Key, ReplicationEvent<Key>>();
  private buffering = false;
  private resyncJob?: Promise<void>;
  private resyncAgain = false;
  private readonly initialization: Promise<void>;
  private resyncTimer?: ReturnType<typeof setInterval>;
  private disposeJob?: Promise<void>;
  private stopObservingCommandErrors?: () => void;
  private observedClusterSubscriptionSocket?: ClusterSubscriptionSocket;
  private clusterRecoveryJob?: Promise<void>;
  private clusterRecoveryNeeded = false;
  private readonly onClusterSubscriptionEnd = () => {
    if (this.disposed || !this.redisSubscribed) return;
    // ioredis replaces the physical socket on 'end', without emitting
    // 'ready' on the enclosing Cluster. Defer until its selector has run.
    void Promise.resolve().then(() => this.requestClusterRecovery()).catch(error => this.reportError(error));
  };
  private cancelInitialization: () => void = () => undefined;
  private readonly initializationStopped = new Promise<void>(resolve => {
    this.cancelInitialization = resolve;
  });
  /** Remove this listener on disposal when using a caller-owned command client. */
  private readonly onCommandError = (error: Error): void => {
    if (!this.disposed) this.reportError(error);
  };

  constructor(options: SyncedMapOptions) {
    if (!options.namespace || !options.namespace.trim()) {
      throw new TypeError('SyncedMap namespace must be nonempty.');
    }
    this.namespace = options.namespace;
    this.defaultTtlMs = positiveTtl(options.ttlMs);
    this.errorHandler = options.onError;
    // encodeURIComponent leaves '*' unchanged, but Redis SCAN treats it as a glob.
    const encodedNamespace = encodeURIComponent(options.namespace).replace(/\*/g, '%2A');
    this.redisPrefix = 'syncedmap:{' + encodedNamespace + '}';
    this.entryPrefix = this.redisPrefix + ':entry:';
    this.revisionKey = this.redisPrefix + ':revision';
    this.eventChannel = this.redisPrefix + ':events';
    this.mutex = new UniqueMutexManager({ namespace: this.redisPrefix });
    this.expiryQueue = new ExpiryQueue((key, expiresAt) => {
      const entry = this.entriesByKey.get(key);
      // A newer write can replace this key before the expiration callback.
      if (entry && entry.expiresAt === expiresAt && expiresAt <= Date.now()) {
        this.entriesByKey.delete(key);
        // Redis-backed replicas must preserve revisions against late events.
        if (!this.redis) this.versionsByKey.delete(key);
      }
    });

    if (!options.redis) {
      this.readyFlag = true;
      this.initialization = Promise.resolve();
    } else {
      this.initialization = this.initializeRedis(options.redis, options.resyncIntervalMs);
      // Consumers receive initialization failures from ready(), without an
      // unhandled rejection if they construct a map before awaiting readiness.
      void this.initialization.catch(() => undefined);
    }
  }

  private reportError(error: unknown): void {
    const normalized = error instanceof Error ? error : new Error(String(error));
    if (this.errorHandler) {
      try {
        this.errorHandler(normalized);
      } catch {
        // A caller's error observer must not kill the replication subscription.
      }
    } else {
      console.error('SyncedMap [' + this.namespace + ']:', normalized);
    }
  }

  /** Close late-opening owned sockets even if dispose won the initialization race. */
  private closeOwnedConnections(): void {
    this.subscriber?.disconnect();
    if (this.ownsRedis) this.redis?.disconnect();
  }

  /** Follow the physical Pub/Sub connection used by ioredis Cluster. */
  private watchClusterSubscriptionSocket(): void {
    const subscriber = this.subscriber;
    if (!subscriber || !isClusterClient(subscriber)) return;
    const socket = getClusterSubscriptionSocket(subscriber);
    if (!socket || socket === this.observedClusterSubscriptionSocket) return;
    this.observedClusterSubscriptionSocket?.removeListener('end', this.onClusterSubscriptionEnd);
    this.observedClusterSubscriptionSocket = socket;
    socket.on('end', this.onClusterSubscriptionEnd);
  }

  /** Repair messages lost while the Cluster's own subscriber reconnects. */
  private requestClusterRecovery(): void {
    if (this.disposed || !this.redisSubscribed) return;
    this.clusterRecoveryNeeded = true;
    if (this.clusterRecoveryJob) return;
    const repair = async () => {
      while (this.clusterRecoveryNeeded && !this.disposed) {
        this.clusterRecoveryNeeded = false;
        this.watchClusterSubscriptionSocket();
        try {
          // performResync first re-subscribes, then reads authoritative state.
          await this.runResync();
          this.watchClusterSubscriptionSocket();
        } catch (error) {
          if (this.disposed) break;
          this.reportError(error);
          this.clusterRecoveryNeeded = true;
          // Retry only after a real subscription outage, not on the normal
          // read/write path or when periodic reconciliation is disabled.
          await new Promise<void>(resolve => {
            const timer = setTimeout(resolve, 500);
            timer.unref?.();
          });
        }
      }
    };
    const job = repair();
    this.clusterRecoveryJob = job;
    void job.then(
      () => { if (this.clusterRecoveryJob === job) this.clusterRecoveryJob = undefined; },
      error => {
        if (this.clusterRecoveryJob === job) this.clusterRecoveryJob = undefined;
        if (!this.disposed) this.reportError(error);
      }
    );
  }

  /** Break unresponsive subscriber/command promises when disposal begins. */
  private async duringInitialization<Result>(operation: Promise<Result>): Promise<Result> {
    const monitored = operation.then(
      value => {
        if (this.disposed) this.closeOwnedConnections();
        return { kind: 'completed' as const, value };
      },
      error => {
        if (this.disposed) this.closeOwnedConnections();
        throw error;
      }
    );
    const outcome = await Promise.race([
      monitored,
      this.initializationStopped.then(() => ({ kind: 'stopped' as const })),
    ]);
    if (outcome.kind === 'stopped') {
      throw new Error('SyncedMap was disposed during initialization.');
    }
    return outcome.value;
  }

  private async initializeRedis(
    config: NonNullable<SyncedMapOptions['redis']>,
    resyncIntervalMs: number | undefined
  ): Promise<void> {
    if (config.client) {
      // ioredis keyPrefix is applied to keyed commands but not SCAN patterns.
      // Refuse a configuration which would silently corrupt hydration/clear.
      const client = config.client;
      const keyPrefix = isClusterClient(client)
        ? client.options?.keyPrefix || client.options?.redisOptions?.keyPrefix
        : client.options?.keyPrefix;
      if (keyPrefix) {
        throw new TypeError('SyncedMap does not support a Redis client with keyPrefix.');
      }
      if (isClusterClient(client) && client.options?.redisOptions?.db !== undefined &&
          client.options.redisOptions.db !== 0) {
        throw new TypeError('Redis Cluster only supports database 0.');
      }
      if (isClusterClient(client) && client.options.scaleReads !== undefined &&
          client.options.scaleReads !== 'master') {
        throw new TypeError('SyncedMap requires Redis Cluster scaleReads=master for authoritative replication.');
      }
      this.redis = config.client;
    } else {
      const RedisConstructor = loadOptionalRedis();
      this.redis = new RedisConstructor(config.url, { lazyConnect: true });
      this.ownsRedis = true;
    }
    const redis = this.redis;
    if (!redis) throw new Error('Unable to initialize the SyncedMap Redis connection.');
    this.stopObservingCommandErrors = observeCommandErrors(redis, this.onCommandError);
    if (this.ownsRedis) await this.duringInitialization(redis.connect());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    // Commands without keys (including INFO) hit arbitrary Redis Cluster nodes.
    // Check every discovered node, not an arbitrary cluster INFO target.
    // Redis may not advertise replicas until replication is ready; operators
    // must configure failover candidates with the same memory policy.
    // A cluster client must first finish slot discovery.
    const cluster = isClusterClient(redis) ? redis : undefined;
    if (cluster) await this.duringInitialization(cluster.ping());
    const nodes: SyncedMapRedisClient[] = cluster ? cluster.nodes('all') : [redis as SyncedMapRedisClient];
    if (nodes.length === 0) throw new Error('No Redis Cluster nodes available.');
    const memoryInfos = await this.duringInitialization(Promise.all(nodes.map(node => node.info('memory'))));
    this.canSkipUnchangedResync = true;
    for (const memoryInfo of memoryInfos) {
      const maxmemory = /^maxmemory:(\d+)\s*$/m.exec(memoryInfo)?.[1];
      const policy = /^maxmemory_policy:([^\r\n]+)\s*$/m.exec(memoryInfo)?.[1];
      if (maxmemory === undefined || policy === undefined) {
        throw new Error('SyncedMap requires Redis INFO memory to verify revision counter safety.');
      }
      if (Number(maxmemory) > 0 && policy.startsWith('allkeys-')) {
        throw new Error('SyncedMap requires a noeviction or volatile-* Redis maxmemory-policy when maxmemory is enabled.');
      }
      if (Number(maxmemory) > 0 && policy !== 'noeviction') {
        // Volatile-* evictions can delete entries without new revisions.
        this.canSkipUnchangedResync = false;
      }
    }
    // Cluster has only DB 0; standalone Redis can select another DB.
    const database = cluster ? 0 : (redis as SyncedMapRedisClient).options?.db;
    if (!Number.isSafeInteger(database) || database === undefined || database < 0) {
      throw new TypeError('SyncedMap requires a Redis client with a valid options.db.');
    }
    this.eventChannel = this.redisPrefix + ':db:' + database + ':events';
    // UMM 0.2 reuses the caller-owned command client. One subscriber connection
    // is necessary because Redis puts SUBSCRIBE clients into subscriber mode.
    const previous = this.mutex;
    this.mutex = new UniqueMutexManager({
      redis: { clients: [redis] },
      namespace: this.redisPrefix,
    });
    await this.duringInitialization(previous.dispose());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    // ioredis Cluster.duplicate(startupNodes, options) has a different
    // signature from Redis.duplicate(options).
    this.subscriber = cluster
      ? cluster.duplicate([], { lazyConnect: true })
      : (redis as SyncedMapRedisClient).duplicate({ lazyConnect: true });
    const subscriber = this.subscriber;
    subscriber.on('error', error => {
      if (!this.disposed) this.reportError(error);
    });
    subscriber.on('message', (channel, message) => {
      if (channel !== this.eventChannel || this.disposed) return;
      try {
        const parsed = JSON.parse(message) as ReplicationEvent<Key>;
        if (typeof parsed.key !== 'string' || !Number.isSafeInteger(parsed.revision) ||
            parsed.revision <= 0 || (parsed.record !== null && typeof parsed.record !== 'string') ||
            typeof parsed.origin !== 'string') {
          throw new Error('Malformed SyncedMap replication event.');
        }
        this.receiveEvent(parsed);
      } catch (error) {
        this.reportError(error);
      }
    });
    subscriber.on('ready', () => {
      if (this.redisSubscribed && !this.disposed) {
        if (cluster) this.watchClusterSubscriptionSocket();
        void this.runResync().catch(error => this.reportError(error));
      }
    });
    await this.duringInitialization(subscriber.connect());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    await this.duringInitialization(subscriber.subscribe(this.eventChannel));
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    this.redisSubscribed = true;
    if (cluster) this.watchClusterSubscriptionSocket();
    await this.duringInitialization(this.runResync());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    this.readyFlag = true;

    const intervalMs = resyncIntervalMs ?? 5 * 60 * 1000;
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 0 || intervalMs > 0x7fffffff) {
      throw new TypeError('resyncIntervalMs must be between 0 and 2147483647 milliseconds.');
    }
    if (intervalMs > 0) {
      this.resyncTimer = setInterval(() => {
        if (!this.disposed) void this.runResync(true).catch(error => this.reportError(error));
      }, intervalMs);
      this.resyncTimer.unref?.();
    }
  }

  /** Wait for initial Redis subscription + hydration. Required before synchronous reads. */
  async ready(): Promise<void> {
    if (this.disposed) throw new Error('SyncedMap has been disposed.');
    await this.initialization;
  }

  private assertReady(): void {
    if (this.disposed) throw new Error('SyncedMap has been disposed.');
    if (!this.readyFlag) throw new Error('Await SyncedMap.ready() before reading its local replica.');
  }

  private assertKey(key: Key): void {
    if (typeof key !== 'string') throw new TypeError('SyncedMap keys must be strings.');
  }

  private entryRedisKey(key: Key): string {
    return this.entryPrefix + encodeURIComponent(key);
  }

  private currentValue(entry: StoredRecord<Value> | undefined): Value | undefined {
    if (!entry || (entry.expiresAt !== 0 && entry.expiresAt <= Date.now())) return undefined;
    return entry.value;
  }

  /** O(1) synchronous local read. May be stale when Redis is enabled. */
  get(key: Key): Value | undefined {
    this.assertReady();
    this.assertKey(key);
    const entry = this.entriesByKey.get(key);
    const value = this.currentValue(entry);
    if (value === undefined && entry) {
      this.entriesByKey.delete(key);
      if (!this.redis) this.versionsByKey.delete(key);
      this.expiryQueue.delete(key);
    }
    return value;
  }

  has(key: Key): boolean {
    return this.get(key) !== undefined;
  }

  get size(): number {
    this.assertReady();
    this.expiryQueue.expireDue();
    return this.entriesByKey.size;
  }

  *entries(): IterableIterator<[Key, Value]> {
    this.assertReady();
    this.expiryQueue.expireDue();
    for (const [key, entry] of this.entriesByKey) {
      const value = this.currentValue(entry);
      if (value !== undefined) yield [key, value];
    }
  }

  *keys(): IterableIterator<Key> {
    for (const [key] of this.entries()) yield key;
  }

  *values(): IterableIterator<Value> {
    for (const [, value] of this.entries()) yield value;
  }

  [Symbol.iterator](): IterableIterator<[Key, Value]> {
    return this.entries();
  }

  forEach(callback: (value: Value, key: Key, map: SyncedMap<Key, Value>) => void): void {
    for (const [key, value] of this.entries()) callback(value, key, this);
  }

  /** Listen for a change or a full resynchronization. Returns an unsubscribe function. */
  subscribe(listener: (change: SyncedMapChange<Key, Value>) => void): () => void {
    this.assertReady();
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private emit(change: SyncedMapChange<Key, Value>): void {
    // Callbacks may unsubscribe/re-subscribe. A live Set iterator would
    // revisit newly appended entries indefinitely during this notification.
    for (const listener of [...this.listeners]) {
      try {
        listener(change);
      } catch (error) {
        this.reportError(error);
      }
    }
  }

  private receiveEvent(event: ReplicationEvent<Key>): void {
    if (this.buffering) {
      // Live observers receive each change now; snapshot replay will be silent.
      // Only the highest revision per key is needed to repair a stale scan.
      const previous = this.bufferedEvents.get(event.key);
      if (!previous || event.revision > previous.revision) {
        this.bufferedEvents.set(event.key, event);
      }
    }
    this.applyEvent(event);
  }

  private applyEvent(event: ReplicationEvent<Key>, notify = true): void {
    if (event.revision <= this.baselineRevision ||
        event.revision <= (this.versionsByKey.get(event.key) ?? 0)) return;
    if (event.record !== null) {
      const entry = parseRecord<Value>(event.record);
      if (entry.version !== event.revision) throw new Error('SyncedMap event version mismatch.');
      entry.value = freezeValue(entry.value);
      this.entriesByKey.set(event.key, entry);
      this.versionsByKey.set(event.key, event.revision);
      this.expiryQueue.set(event.key, entry.expiresAt);
      if (notify) {
        this.emit({
          type: 'set', key: event.key, value: entry.value, revision: event.revision,
          origin: event.origin === this.instanceId ? 'local' : 'remote',
        });
      }
    } else {
      const existed = this.entriesByKey.has(event.key);
      this.entriesByKey.delete(event.key);
      this.expiryQueue.delete(event.key);
      // Local mode has no delayed remote events: retaining tombstones would
      // leak one Map entry per deleted key indefinitely.
      if (this.redis) this.versionsByKey.set(event.key, event.revision);
      else this.versionsByKey.delete(event.key);
      if (existed && notify) {
        this.emit({
          type: 'delete', key: event.key, revision: event.revision,
          origin: event.origin === this.instanceId ? 'local' : 'remote',
        });
      }
    }
  }

  /** In Cluster mode SCAN is node-local. Examine every primary.
   * Keyed MGET and EVAL still route via ioredis to the correct hash slot.
   */
  private async *scanEntryKeys(): AsyncIterableIterator<string[]> {
    const redis = this.redis;
    if (!redis) return;
    const nodes = isClusterClient(redis) ? redis.nodes('master') : [redis as SyncedMapRedisClient];
    if (nodes.length === 0) throw new Error('No Redis Cluster primary nodes available.');
    for (const node of nodes) {
      let cursor = '0';
      do {
        const [next, keys] = await node.scan(
          cursor, 'MATCH', this.entryPrefix + '*', 'COUNT', redisScanCount
        );
        cursor = next;
        yield keys;
      } while (cursor !== '0');
    }
  }

  /** Build one full snapshot; never install it before checking Cluster topology. */
  private async readRedisSnapshot(
    redis: SyncedMapRedisConnection
  ): Promise<{ values: Map<Key, StoredRecord<Value>>; versions: Map<Key, number> }> {
    const values = new Map<Key, StoredRecord<Value>>();
    const versions = new Map<Key, number>();
    for await (const keys of this.scanEntryKeys()) {
      const batches: string[][] = [];
      for (let offset = 0; offset < keys.length; offset += redisReadBatchSize) {
        batches.push(keys.slice(offset, offset + redisReadBatchSize));
      }
      // SCAN cursors are sequential, but MGET batches within a page can
      // run concurrently. Promise.all retains the batch ordering.
      const results = await Promise.all(batches.map(batch => redis.mget(...batch)));
      for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
        const batch = batches[batchIndex]!;
        const rawValues = results[batchIndex]!;
        for (let i = 0; i < batch.length; i++) {
          const redisKey = batch[i];
          const raw = rawValues[i];
          if (!redisKey || !raw) continue; // Key might have expired during SCAN.
          const key = decodeURIComponent(redisKey.slice(this.entryPrefix.length)) as Key;
          const entry = parseRecord<Value>(raw);
          values.set(key, { ...entry, value: freezeValue(entry.value) });
          versions.set(key, entry.version);
        }
      }
    }
    return { values, versions };
  }

  /** Reconstruct state from Redis; pub/sub events arriving during SCAN are replayed. */
  private async performResync(periodic = false): Promise<void> {
    const redis = this.redis;
    const subscriber = this.subscriber;
    if (!redis || !subscriber || this.disposed) return;

    // A periodic check can avoid a full SCAN when this namespace has seen
    // no writes and Redis cannot silently evict entries. Expiry itself is
    // already maintained by the local heap. Explicit/reconnect resyncs always
    // rehydrate, and volatile-* eviction policies must always do a full scan.
    const cluster = isClusterClient(redis) ? redis : undefined;
    const topology = cluster ? JSON.stringify(await cluster.cluster('SLOTS')) : undefined;
    const tooManyTombstones = this.versionsByKey.size > this.entriesByKey.size + 512;
    if (periodic && this.canSkipUnchangedResync &&
        this.lastHydrationRevision !== undefined && !tooManyTombstones &&
        (!cluster || (this.lastHydrationTopology !== undefined &&
            topology === this.lastHydrationTopology))) {
      const revision = await redis.get(this.revisionKey);
      if ((revision ?? '0') === String(this.lastHydrationRevision)) return;
    }

    this.buffering = true;
    this.bufferedEvents = new Map();
    try {
      await subscriber.subscribe(this.eventChannel);
      // Redis Cluster can move a namespace between primaries while SCAN
      // walks them. An unstable scan is incomplete, even if no write changed
      // the revision counter. Retry against a fresh topology and baseline;
      // never replace a good replica with a known-incomplete snapshot.
      for (let attempt = 0; attempt < (cluster ? 3 : 1); attempt++) {
        const scanTopology = attempt === 0
          ? topology
          : JSON.stringify(await cluster!.cluster('SLOTS'));
        const baselineRaw = await redis.get(this.revisionKey);
        const baseline = Number(baselineRaw ?? '0');
        if (!Number.isSafeInteger(baseline) || baseline < 0) {
          throw new Error('Invalid SyncedMap revision counter.');
        }
        const { values, versions } = await this.readRedisSnapshot(redis);
        // No awaits between taking pending events and replaying them.
        const afterTopology = cluster ? JSON.stringify(await cluster.cluster('SLOTS')) : undefined;
        if (scanTopology !== afterTopology) {
          if (attempt < 2) continue;
          throw new Error('Redis Cluster topology changed repeatedly during SyncedMap reconciliation.');
        }
        if (this.disposed) throw new Error('SyncedMap disposed during reconciliation.');
        const pending = this.bufferedEvents;
        this.entriesByKey = values;
        this.versionsByKey = versions;
        this.baselineRevision = baseline;
        this.expiryQueue.replace(values);
        this.lastHydrationRevision = baseline;
        this.lastHydrationTopology = afterTopology;
        this.bufferedEvents = new Map();
        this.buffering = false;
        // These mutations already reached live observers. Replay silently
        // and emit one resync only after a stable scan succeeded.
        for (const event of pending.values()) this.applyEvent(event, false);
        this.emit({ type: 'resync' });
        return;
      }
    } catch (error) {
      // Failed hydration never swaps the snapshot: live events have already
      // been applied and notified, so replaying would only duplicate them.
      this.bufferedEvents = new Map();
      this.buffering = false;
      throw error;
    }
  }

  private runResync(periodic = false): Promise<void> {
    if (this.resyncJob) {
      // Periodic maintenance must not request a redundant second full scan
      // while a reconciliation is already running.
      if (!periodic) this.resyncAgain = true;
      return this.resyncJob;
    }
    const run = async () => {
      let checkOnly = periodic;
      do {
        this.resyncAgain = false;
        try {
          await this.performResync(checkOnly);
        } catch (error) {
          // A reconnect that arrived during a failed scan still needs its
          // separately requested reconciliation. Without that request, fail
          // normally rather than retrying indefinitely on persistent errors.
          if (!this.resyncAgain || this.disposed) throw error;
          this.reportError(error);
        }
        checkOnly = false;
      } while (this.resyncAgain && !this.disposed);
    };
    const job = run();
    this.resyncJob = job;
    const clearJob = () => {
      if (this.resyncJob === job) this.resyncJob = undefined;
    };
    void job.then(clearJob, clearJob);
    return job;
  }

  /** Force a full authoritative resynchronization, e.g. after suspecting missed messages. */
  async resync(): Promise<void> {
    await this.ready();
    if (this.redis) await this.runResync();
  }

  private async readAuthoritative(key: Key): Promise<CurrentState<Value>> {
    const redis = this.redis;
    if (!redis) {
      const entry = this.entriesByKey.get(key);
      // Local expiration is authoritative and handled by our timer / clock.
      return { entry, expectedRevision: entry?.version ?? 0, value: this.currentValue(entry) };
    }
    const result = await redis.eval(READ_SCRIPT, 2, this.entryRedisKey(key), this.revisionKey);
    if (!Array.isArray(result) || result.length < 2) throw new Error('Invalid SyncedMap Redis read result.');
    const globalRevision = Number(result[0]);
    if (!Number.isSafeInteger(globalRevision) || globalRevision < 0) {
      throw new Error('Invalid SyncedMap Redis read revision.');
    }
    const raw = result[1];
    if (typeof raw === 'string') {
      const entry = parseRecord<Value>(raw);
      entry.value = freezeValue(entry.value);
      this.receiveEvent({ key, revision: entry.version, record: raw, origin: 'fresh-read' });
      // Redis checked expiry while serving GET. Do not second-guess its
      // result with a different machine's clock, which may be skewed.
      return { entry, expectedRevision: entry.version, value: entry.value };
    }
    // A missing entry is authoritative as of the global revision. The
    // revision is needed to suppress older, delayed set notifications.
    this.receiveEvent({ key, revision: globalRevision, record: null, origin: 'fresh-read' });
    return { expectedRevision: 0, value: undefined };
  }

  /** Current authoritative value at the instant Redis processes the read; no lock. */
  async getFresh(key: Key): Promise<Value | undefined> {
    this.assertKey(key);
    await this.ready();
    return (await this.readAuthoritative(key)).value;
  }

  /** Exclusive key lock. Its callback can call getFresh/set/delete/update reentrantly. */
  async withLock<Result>(
    key: Key,
    operation: (context: SyncedMapLockContext) => Promise<Result> | Result
  ): Promise<Result> {
    this.assertKey(key);
    await this.ready();
    return this.mutex.runOperation('entry:' + key, async ({ assertLock, abortSignal }) => {
      const result = await operation({ assertLock, abortSignal });
      assertLock();
      return result;
    });
  }

  /**
   * Read authoritative data while holding the same key lock used by writers.
   * The lock remains held until the callback completes (or fails).
   */
  async readLocked<Result>(
    key: Key,
    reader: (value: Value | undefined, context: SyncedMapLockContext) => Promise<Result> | Result
  ): Promise<Result> {
    return this.withLock(key, async context => {
      const state = await this.readAuthoritative(key);
      context.assertLock();
      const result = await reader(state.value, context);
      context.assertLock();
      return result;
    });
  }

  private async commit(
    key: Key,
    action: WriteAction,
    expectedRevision: number,
    jsonValue: string,
    ttlMs: number
  ): Promise<boolean> {
    const redis = this.redis;
    if (!redis) {
      const existing = this.entriesByKey.get(key);
      const currentRevision = existing?.version ?? 0;
      // Reentrant sibling updates inside withLock can overlap. The local
      // branch must preserve the same revision CAS guarantee as Redis.
      if (currentRevision !== expectedRevision) throw new SyncedMapConflictError(key);
      const existed = this.currentValue(existing) !== undefined;
      if (action === 'delete' && !existing) return false;
      const expiresAt = ttlMs > 0 ? Date.now() + ttlMs : 0;
      if (!Number.isSafeInteger(expiresAt)) {
        throw new RangeError('SyncedMap expiration exceeds the supported timestamp range.');
      }
      const version = ++this.localRevision;
      const record = action === 'set'
        ? encodeRecord({ version, expiresAt, value: undefined }, jsonValue)
        : null;
      this.receiveEvent({ key, revision: version, record, origin: this.instanceId });
      return existed;
    }
    const output = await redis.eval(
      WRITE_SCRIPT, 3, this.entryRedisKey(key), this.revisionKey, this.eventChannel,
      String(expectedRevision), action, jsonValue, String(ttlMs), key, this.instanceId
    );
    if (!Array.isArray(output) || output.length < 3) throw new Error('Invalid SyncedMap Redis write result.');
    const status = Number(output[0]);
    if (status === 0) throw new SyncedMapConflictError(key);
    if (status === 2) return false;
    if (status === 3) throw new RangeError('SyncedMap expiration exceeds the supported timestamp range.');
    if (status !== 1) throw new Error('Invalid SyncedMap Redis write status.');
    const version = Number(output[1]);
    if (!Number.isSafeInteger(version) || version < 1) throw new Error('Invalid SyncedMap Redis write version.');
    this.receiveEvent({
      key, revision: version, record: action === 'set' ? String(output[2]) : null,
      origin: this.instanceId,
    });
    // Reconciliation buffers and replays this acknowledged commit; do not
    // block the successful write behind subscriber availability or SCAN.
    return action === 'delete';
  }

  /** A coordinated write. Values are JSON-only; mutate by replacing, not by changing a get() reference. */
  async set(key: Key, value: Value, options: SyncedMapSetOptions = {}): Promise<this> {
    this.assertKey(key);
    const json = serializeJson(value);
    const ttlMs = positiveTtl(options.ttlMs ?? this.defaultTtlMs);
    await this.withLock(key, async context => {
      const previous = await this.readAuthoritative(key);
      context.assertLock();
      await this.commit(key, 'set', previous.expectedRevision, json, ttlMs);
    });
    return this;
  }

  /** Delete one key while protecting it with the same distributed mutex. */
  async delete(key: Key): Promise<boolean> {
    this.assertKey(key);
    return this.withLock(key, async context => {
      const previous = await this.readAuthoritative(key);
      context.assertLock();
      const existed = previous.value !== undefined;
      await this.commit(key, 'delete', previous.expectedRevision, '', 0);
      return existed;
    });
  }

  /**
   * Atomic read-modify-write under one per-key lock, with a Redis CAS check
   * to detect a conflicting writer if lock ownership was lost. Returning
   * undefined deletes the key. The updater is not automatically retried.
   */
  async update(
    key: Key,
    updater: (current: Value | undefined) => Value | undefined | Promise<Value | undefined>,
    options: SyncedMapSetOptions = {}
  ): Promise<Value | undefined> {
    this.assertKey(key);
    const ttlMs = positiveTtl(options.ttlMs ?? this.defaultTtlMs);
    return this.withLock(key, async context => {
      const previous = await this.readAuthoritative(key);
      const nextValue = await updater(previous.value);
      context.assertLock();
      if (nextValue === undefined) {
        await this.commit(key, 'delete', previous.expectedRevision, '', 0);
        return undefined;
      }
      const json = serializeJson(nextValue);
      await this.commit(key, 'set', previous.expectedRevision, json, ttlMs);
      // This write was acknowledged. Its local cache might look expired if
      // this process's clock is ahead of Redis. Return the committed value.
      return this.entriesByKey.get(key)?.value ?? freezeValue(JSON.parse(json) as Value);
    });
  }

  /**
   * Best-effort per-key clear, not an atomic namespace transaction.
   * New keys created concurrently may remain.
   */
  async clear(): Promise<void> {
    await this.ready();
    const targets = new Set<Key>();
    if (this.redis) {
      // Collect before deleting: mutating the keyspace while scanning can
      // skip keys. Keep deletion concurrency bounded to avoid Redis overload.
      for await (const keys of this.scanEntryKeys()) {
        for (const fullKey of keys) {
          targets.add(decodeURIComponent(fullKey.slice(this.entryPrefix.length)) as Key);
        }
      }
    } else {
      for (const key of this.entriesByKey.keys()) targets.add(key);
    }

    const iterator = targets.values();
    let failed = false;
    let failure: unknown;
    // Every worker retains per-key UMM locking and Redis CAS semantics.
    // Stop assigning new keys on error, and drain all in-flight workers
    // before rejecting so no deletions continue after clear() returns.
    const workers = Array.from({ length: Math.min(32, targets.size) }, async () => {
      while (!failed) {
        const next = iterator.next();
        if (next.done) return;
        try {
          await this.delete(next.value);
        } catch (error) {
          if (!failed) failure = error;
          failed = true;
        }
      }
    });
    await Promise.all(workers);
    if (failed) throw failure;
  }

  /** Dispose only Redis connections created by this map; never close a supplied Redis client. */
  dispose(): Promise<void> {
    if (this.disposeJob) return this.disposeJob;
    this.disposed = true;
    this.readyFlag = false;
    this.cancelInitialization();
    if (this.resyncTimer) clearInterval(this.resyncTimer);
    this.clusterRecoveryNeeded = false;
    this.observedClusterSubscriptionSocket?.removeListener('end', this.onClusterSubscriptionEnd);
    this.observedClusterSubscriptionSocket = undefined;
    this.expiryQueue.dispose();
    this.listeners.clear();
    // Unblock connection/subscription attempts that are still initializing.
    this.closeOwnedConnections();
    this.disposeJob = this.finishDispose();
    return this.disposeJob;
  }

  private async finishDispose(): Promise<void> {
    // Initialization checks disposed after each await, so it cannot create
    // new resources after this completes. Ignore expected shutdown errors.
    await this.initialization.catch(() => undefined);
    this.closeOwnedConnections();
    this.stopObservingCommandErrors?.();
    await this.mutex.dispose();
    this.closeOwnedConnections();
  }
}
