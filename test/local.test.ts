import { afterEach, describe, expect, it } from 'vitest';
import { SyncedMap } from '../src/index.js';

const opened: Array<SyncedMap<string, unknown>> = [];
function createMap<Value>() {
  const map = new SyncedMap<string, Value>({ namespace: 'local-tests' });
  opened.push(map as SyncedMap<string, unknown>);
  return map;
}
afterEach(async () => {
  for (const map of opened.splice(0)) await map.dispose();
});

describe('SyncedMap local mode', () => {
  it('uses immediate Map reads, JSON snapshots, and immutable values', async () => {
    const map = createMap<{ name: string; nested: { count: number } }>();
    await map.ready();
    const supplied = { name: 'first', nested: { count: 1 } };
    expect(map.get('one')).toBeUndefined();
    await map.set('one', supplied);
    supplied.nested.count = 900;
    expect(map.get('one')).toEqual({ name: 'first', nested: { count: 1 } });
    expect(Object.isFrozen(map.get('one'))).toBe(true);
    expect(Object.isFrozen(map.get('one')?.nested)).toBe(true);
    expect(map.size).toBe(1);
    expect(map.has('one')).toBe(true);
    expect([...map]).toEqual([['one', { name: 'first', nested: { count: 1 } }]]);
    expect([...map.keys()]).toEqual(['one']);
    expect([...map.values()]).toHaveLength(1);
    const keys: string[] = [];
    map.forEach((_value, key) => keys.push(key));
    expect(keys).toEqual(['one']);
    expect(await map.delete('one')).toBe(true);
    expect(await map.delete('one')).toBe(false);
    expect(map.size).toBe(0);
  });

  it('serializes concurrent update operations on one key', async () => {
    const map = createMap<number>();
    await map.set('count', 0);
    await Promise.all(
      Array.from({ length: 80 }, () => map.update('count', count => (count ?? 0) + 1))
    );
    expect(map.get('count')).toBe(80);
    await map.update('count', () => undefined);
    expect(map.get('count')).toBeUndefined();
  });

  it('holds readLocked for its entire callback and blocks the next write', async () => {
    const map = createMap<number>();
    await map.set('balance', 12);
    let releaseRead: () => void = () => undefined;
    const gate = new Promise<void>(resolve => { releaseRead = resolve; });
    let readerEntered: () => void = () => undefined;
    const entered = new Promise<void>(resolve => { readerEntered = resolve; });
    let seen: number | undefined;
    const reading = map.readLocked('balance', async (value, context) => {
      seen = value;
      context.assertLock();
      readerEntered();
      await gate;
    });
    await entered;
    let writerFinished = false;
    const writing = map.set('balance', 99).then(() => { writerFinished = true; });
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(seen).toBe(12);
    expect(writerFinished).toBe(false);
    expect(map.get('balance')).toBe(12);
    releaseRead();
    await Promise.all([reading, writing]);
    expect(map.get('balance')).toBe(99);
  });

  it('expires TTL values and respects resync / fresh-read semantics in memory', async () => {
    const map = createMap<string>();
    await map.set('short', 'hello', { ttlMs: 25 });
    expect(await map.getFresh('short')).toBe('hello');
    await new Promise(resolve => setTimeout(resolve, 35));
    expect(map.get('short')).toBeUndefined();
    expect(await map.getFresh('short')).toBeUndefined();
    await map.resync();
  });

  it('allows per-write TTL zero to override a default TTL', async () => {
    const map = new SyncedMap<string, number>({ namespace: 'ttl-default', ttlMs: 15 });
    opened.push(map as SyncedMap<string, unknown>);
    await map.set('persistent', 8, { ttlMs: 0 });
    await map.set('short', 9);
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(map.get('short')).toBeUndefined();
    expect(map.get('persistent')).toBe(8);
  });

  it('allows nested writes inside a locked callback', async () => {
    const map = createMap<number>();
    await map.set('key', 1);
    const outcome = await map.readLocked('key', async (current, context) => {
      expect(current).toBe(1);
      await map.set('key', 2);
      context.assertLock();
      return await map.getFresh('key');
    });
    expect(outcome).toBe(2);
    expect(map.get('key')).toBe(2);
  });

  it('does not retain tombstones for deleted local-only keys', async () => {
    const map = createMap<number>();
    for (let i = 0; i < 250; i++) {
      const key = 'short-lived-' + i;
      await map.set(key, i);
      expect(await map.delete(key)).toBe(true);
    }
    expect(map.size).toBe(0);
    // Inspect the internal revision index: no deleted-key history should be
    // retained indefinitely by local-only maps.
    const revisions = (map as unknown as { versionsByKey: Map<string, number> }).versionsByKey;
    expect(revisions.size).toBe(0);
    await map.set('reused', 1);
    await map.delete('reused');
    await map.set('reused', 2);
    expect(map.get('reused')).toBe(2);
    expect(revisions.size).toBe(1);
    await map.clear();
    expect(revisions.size).toBe(0);
  });

  it('publishes writes and protects against returning live input objects', async () => {
    const map = createMap<{ score: number }>();
    const observed: string[] = [];
    const stop = map.subscribe(change => observed.push(change.type));
    await map.set('key', { score: 1 });
    await map.update('key', item => ({ score: (item?.score ?? 0) + 1 }));
    await map.delete('key');
    expect(observed).toEqual(['set', 'set', 'delete']);
    stop();
    await map.set('key', { score: 4 });
    expect(observed).toHaveLength(3);
    await map.clear();
    expect(map.size).toBe(0);
  });
});
