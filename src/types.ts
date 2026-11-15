import type { RedisClientLike } from 'uniquemutexmanager';

/**
 * Minimal Redis client contract. An ioredis Redis instance implements this
 * interface; no Redis types are needed for local-only consumers.
 */
export interface SyncedMapRedisClient extends RedisClientLike {
  get(key: string): Promise<string | null>;
  mget(...keys: string[]): Promise<Array<string | null>>;
  scan(cursor: string, match: 'MATCH', pattern: string, count: 'COUNT', amount: number): Promise<[string, string[]]>;
  subscribe(channel: string): Promise<unknown>;
  duplicate(options?: { lazyConnect?: boolean }): SyncedMapRedisClient;
  connect(): Promise<unknown>;
  on(event: string, listener: (...args: any[]) => void): unknown;
  disconnect(): void;
  quit(): Promise<unknown>;
  info(section: 'memory'): Promise<string>;
  removeListener(event: string, listener: (...args: any[]) => void): unknown;
  /** ioredis applies this prefix to keyed commands but not SCAN patterns.
   * db must identify the selected database; changing it after construction
   * is unsupported because the replication subscription is database-scoped.
   */
  options?: { keyPrefix?: string; db?: number };
}

/** Redis Cluster routing is supplied by the caller's ioredis Cluster client.
 * All Redis keys for a SyncedMap namespace already share one hash slot.
 * The Cluster client owns its Redis sockets; SyncedMap closes only its
 * own duplicated subscriber.
 */
export interface SyncedMapRedisClusterClient extends Omit<SyncedMapRedisClient, 'duplicate' | 'options'> {
  readonly isCluster: boolean;
  ping(): Promise<string>;
  cluster(command: 'SLOTS'): Promise<unknown>;
  nodes(role: 'master' | 'all'): SyncedMapRedisClient[];
  duplicate(
    startupNodes?: Array<string | number | { host?: string; port?: number }>,
    options?: { lazyConnect?: boolean }
  ): SyncedMapRedisClusterClient;
  options: { keyPrefix?: string; redisOptions?: { keyPrefix?: string; db?: number }; scaleReads?: unknown };
}

export type SyncedMapRedisConnection = SyncedMapRedisClient | SyncedMapRedisClusterClient;

export type SyncedMapRedisConfig =
  | { url: string; client?: never }
  | { client: SyncedMapRedisConnection; url?: never };

export interface SyncedMapOptions {
  /** Shared namespace. All participants in a logical map must use exactly the same namespace. */
  namespace: string;
  /** Omit for a fully local map. */
  redis?: SyncedMapRedisConfig;
  /** Default TTL, in milliseconds. Omit for no expiry. */
  ttlMs?: number;
  /** Optional periodic reconciliation for recovering missed Pub/Sub messages; 0 disables it. */
  resyncIntervalMs?: number;
  /** Errors from background replication and subscriber failures. */
  onError?: (error: Error) => void;
}

export interface SyncedMapSetOptions {
  ttlMs?: number;
}

export type SyncedMapChange<Key extends string, Value> =
  | { type: 'set'; key: Key; value: Value; revision: number; origin: 'local' | 'remote' }
  | { type: 'delete'; key: Key; revision: number; origin: 'local' | 'remote' }
  | { type: 'resync' };

export interface SyncedMapLockContext {
  readonly abortSignal: AbortSignal;
  /** Check that the distributed lease has not been lost before an irreversible operation. */
  assertLock(): void;
}

export class SyncedMapConflictError extends Error {
  constructor(key: string) {
    super('SyncedMap concurrent modification detected for key: ' + key);
    this.name = 'SyncedMapConflictError';
  }
}
