import { randomUUID } from 'node:crypto';
import loadOptionalRedis from './redisLoader.cjs';
import { LocalExpiryQueue } from './localExpiryQueue.js';
import { UniqueMutexManager } from 'uniquemutexmanager';
import { READ_SCRIPT, WRITE_SCRIPT, encodeRecord, freezeValue, parseRecord, serializeJson } from './redisProtocol.js';
import type { StoredRecord } from './redisProtocol.js';
import { SyncedMapConflictError } from './types.js';
import type {
  SyncedMapChange, SyncedMapLockContext, SyncedMapOptions, SyncedMapSetOptions, SyncedMapRedisClient,
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
  private readonly eventChannel: string;
  private readonly defaultTtlMs: number;
  private readonly errorHandler?: (error: Error) => void;
  private readonly listeners = new Set<(change: SyncedMapChange<Key, Value>) => void>();
  /** Local mode needs expiry cleanup even when nobody calls get() or iterates. */
  private readonly localExpiryQueue?: LocalExpiryQueue<Key>;

  private mutex: UniqueMutexManager;
  private redis?: SyncedMapRedisClient;
  private subscriber?: SyncedMapRedisClient;
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

    if (!options.redis) {
      this.localExpiryQueue = new LocalExpiryQueue((key, expiresAt) => {
        const entry = this.entriesByKey.get(key);
        // A newer write can replace this key before the expiration callback.
        if (entry && entry.expiresAt === expiresAt && expiresAt <= Date.now()) {
          this.entriesByKey.delete(key);
          this.versionsByKey.delete(key);
        }
      });
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
      if (config.client.options?.keyPrefix) {
        throw new TypeError('SyncedMap does not support a Redis client with keyPrefix.');
      }
      this.redis = config.client;
    } else {
      const RedisConstructor = loadOptionalRedis();
      this.redis = new RedisConstructor(config.url, { lazyConnect: true });
      this.ownsRedis = true;
    }
    const redis = this.redis;
    if (!redis) throw new Error('Unable to initialize the SyncedMap Redis connection.');
    redis.on('error', this.onCommandError);
    if (this.ownsRedis) await this.duringInitialization(redis.connect());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    // UMM 0.2 reuses the caller-owned command client. One subscriber connection
    // is necessary because Redis puts SUBSCRIBE clients into subscriber mode.
    const previous = this.mutex;
    this.mutex = new UniqueMutexManager({
      redis: { clients: [redis] },
      namespace: this.redisPrefix,
    });
    await this.duringInitialization(previous.dispose());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    this.subscriber = redis.duplicate({ lazyConnect: true });
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
        void this.runResync().catch(error => this.reportError(error));
      }
    });
    await this.duringInitialization(subscriber.connect());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    await this.duringInitialization(subscriber.subscribe(this.eventChannel));
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    this.redisSubscribed = true;
    await this.duringInitialization(this.runResync());
    if (this.disposed) throw new Error('SyncedMap was disposed during initialization.');
    this.readyFlag = true;

    const intervalMs = resyncIntervalMs ?? 5 * 60 * 1000;
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 0 || intervalMs > 0x7fffffff) {
      throw new TypeError('resyncIntervalMs must be between 0 and 2147483647 milliseconds.');
    }
    if (intervalMs > 0) {
      this.resyncTimer = setInterval(() => {
        if (!this.disposed) void this.runResync().catch(error => this.reportError(error));
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
    if (value === undefined && entry && !this.redis) {
      this.entriesByKey.delete(key);
      this.versionsByKey.delete(key);
      this.localExpiryQueue?.delete(key);
    }
    return value;
  }

  has(key: Key): boolean {
    return this.get(key) !== undefined;
  }

  get size(): number {
    this.assertReady();
    let size = 0;
    for (const entry of this.entriesByKey.values()) {
      if (this.currentValue(entry) !== undefined) size++;
    }
    return size;
  }

  *entries(): IterableIterator<[Key, Value]> {
    this.assertReady();
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
    for (const listener of this.listeners) {
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
      if (!this.redis) this.localExpiryQueue?.set(event.key, entry.expiresAt);
      if (notify) {
        this.emit({
          type: 'set', key: event.key, value: entry.value, revision: event.revision,
          origin: event.origin === this.instanceId ? 'local' : 'remote',
        });
      }
    } else {
      const existed = this.entriesByKey.has(event.key);
      this.entriesByKey.delete(event.key);
      if (!this.redis) this.localExpiryQueue?.delete(event.key);
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

  /** Reconstruct state from Redis; pub/sub events arriving during SCAN are replayed. */
  private async performResync(): Promise<void> {
    const redis = this.redis;
    const subscriber = this.subscriber;
    if (!redis || !subscriber || this.disposed) return;
    this.buffering = true;
    this.bufferedEvents = new Map();
    try {
      await subscriber.subscribe(this.eventChannel);
      const baselineRaw = await redis.get(this.revisionKey);
      const baseline = Number(baselineRaw ?? '0');
      if (!Number.isSafeInteger(baseline) || baseline < 0) {
        throw new Error('Invalid SyncedMap revision counter.');
      }
      const values = new Map<Key, StoredRecord<Value>>();
      const versions = new Map<Key, number>();
      let cursor = '0';
      do {
        const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', this.entryPrefix + '*', 'COUNT', 200);
        cursor = nextCursor;
        for (let offset = 0; offset < keys.length; offset += 200) {
          const batch = keys.slice(offset, offset + 200);
          const rawValues = await redis.mget(...batch);
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
      } while (cursor !== '0');
      if (this.disposed) throw new Error('SyncedMap disposed during reconciliation.');
      // This swap and event replay are synchronous, so subscriber callbacks
      // cannot interleave between the snapshot and the buffered updates.
      const pending = this.bufferedEvents;
      this.entriesByKey = values;
      this.versionsByKey = versions;
      this.baselineRevision = baseline;
      this.bufferedEvents = new Map();
      this.buffering = false;
      // These mutations were already delivered to live subscribers. Replay
      // only the newest state per key, then emit one resync notification.
      for (const event of pending.values()) this.applyEvent(event, false);
      this.emit({ type: 'resync' });
    } catch (error) {
      // Failed hydration never swaps the snapshot: live events have already
      // been applied and notified, so replaying would only duplicate them.
      this.bufferedEvents = new Map();
      this.buffering = false;
      throw error;
    }
  }

  private runResync(): Promise<void> {
    if (this.resyncJob) {
      this.resyncAgain = true;
      return this.resyncJob;
    }
    const run = async () => {
      do {
        this.resyncAgain = false;
        await this.performResync();
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
      const version = ++this.localRevision;
      const record = action === 'set'
        ? encodeRecord({ version, expiresAt: ttlMs > 0 ? Date.now() + ttlMs : 0, value: undefined }, jsonValue)
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
    if (this.redis) {
      // Complete SCAN before deleting: changing the keyspace mid-iteration
      // can otherwise cause Redis SCAN to miss keys even without other writers.
      const targets = new Set<Key>();
      let cursor = '0';
      do {
        const [next, keys] = await this.redis.scan(cursor, 'MATCH', this.entryPrefix + '*', 'COUNT', 200);
        cursor = next;
        for (const fullKey of keys) {
          targets.add(decodeURIComponent(fullKey.slice(this.entryPrefix.length)) as Key);
        }
      } while (cursor !== '0');
      for (const key of targets) await this.delete(key);
    } else {
      for (const key of [...this.entriesByKey.keys()]) await this.delete(key);
    }
  }

  /** Dispose only Redis connections created by this map; never close a supplied Redis client. */
  dispose(): Promise<void> {
    if (this.disposeJob) return this.disposeJob;
    this.disposed = true;
    this.readyFlag = false;
    this.cancelInitialization();
    if (this.resyncTimer) clearInterval(this.resyncTimer);
    this.localExpiryQueue?.dispose();
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
    this.redis?.removeListener('error', this.onCommandError);
    await this.mutex.dispose();
    this.closeOwnedConnections();
  }
}
