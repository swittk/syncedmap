/** Atomic Redis operations. All keys share a namespace hash tag for cluster compatibility. */
export const WRITE_SCRIPT = [
  "local old = redis.call('GET', KEYS[1])",
  "local revision = 0",
  "if old then revision = tonumber(string.match(old, '^(%d+)|')) or -1 end",
  "if revision ~= tonumber(ARGV[1]) then return {0, tostring(revision), ''} end",
  "if ARGV[2] == 'delete' and not old then return {2, '0', ''} end",
  "local nextRevision = redis.call('INCR', KEYS[2])",
  "local record = ''",
  "if ARGV[2] == 'set' then",
  "  local ttl = tonumber(ARGV[4])",
  "  local expires = 0",
  "  if ttl > 0 then",
  "    local now = redis.call('TIME')",
  "    expires = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) + ttl",
  "  end",
  "  record = tostring(nextRevision) .. '|' .. tostring(expires) .. '|' .. ARGV[3]",
  "  if ttl > 0 then",
  "    redis.call('SET', KEYS[1], record, 'PX', ttl)",
  "  else",
  "    redis.call('SET', KEYS[1], record)",
  "  end",
  "else",
  "  redis.call('DEL', KEYS[1])",
  "end",
  "local published = {key = ARGV[5], revision = nextRevision, origin = ARGV[6]}",
  "if ARGV[2] == 'set' then published.record = record else published.record = cjson.null end",
  "redis.call('PUBLISH', KEYS[3], cjson.encode(published))",
  "return {1, tostring(nextRevision), record}",
].join('\n');

export const READ_SCRIPT = [
  "return {tostring(redis.call('GET', KEYS[2]) or 0), redis.call('GET', KEYS[1])}",
].join('\n');

/** Values stored in Redis are JSON prefixed by revision and absolute expiry. */
export type StoredRecord<Value> = {
  version: number;
  expiresAt: number;
  value: Value;
};

export function parseRecord<Value>(raw: string): StoredRecord<Value> {
  const first = raw.indexOf('|');
  const second = raw.indexOf('|', first + 1);
  if (first <= 0 || second <= first) {
    throw new Error('SyncedMap received a malformed Redis record.');
  }
  const version = Number(raw.slice(0, first));
  const expiresAt = Number(raw.slice(first + 1, second));
  if (!Number.isSafeInteger(version) || version <= 0 ||
      !Number.isSafeInteger(expiresAt) || expiresAt < 0) {
    throw new Error('SyncedMap received an invalid Redis record revision or expiry.');
  }
  return {
    version,
    expiresAt,
    value: JSON.parse(raw.slice(second + 1)) as Value,
  };
}

export function encodeRecord<Value>(record: StoredRecord<Value>, jsonValue: string): string {
  return String(record.version) + '|' + String(record.expiresAt) + '|' + jsonValue;
}

/** Freeze JSON values once when they enter the local replica; get() stays O(1). */
export function freezeValue<Value>(value: Value): Value {
  if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
    const object = value as Record<string, unknown>;
    for (const key of Object.keys(object)) freezeValue(object[key]);
    Object.freeze(object);
  }
  return value;
}

export function serializeJson(value: unknown): string {
  const json = JSON.stringify(value);
  if (typeof json !== 'string') {
    throw new TypeError('SyncedMap values must be JSON-serializable; undefined is reserved for deletion.');
  }
  return json;
}
