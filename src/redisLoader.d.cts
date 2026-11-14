import type { SyncedMapRedisClient } from './types.js';

declare function loadOptionalRedis(): new (
  url: string,
  options: { lazyConnect: boolean }
) => SyncedMapRedisClient;
export = loadOptionalRedis;
