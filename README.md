# SyncedMap

A Map-like TypeScript/JavaScript collection with **synchronous local reads**,
**optional Redis-backed propagation**, and **keyed locking using
UniqueMutexManager 0.2.0**.

For small or medium *ephemeral backend state*: peer presence, session ownership,
cooldowns, and shared runtime registries. This is **not** a database, a ledger,
a distributed transaction engine, or an authority for durable business records.

## Quick start

Without Redis, each instance is an independent local in-process map:

    import { SyncedMap } from 'syncedmap';

    const peers = new SyncedMap<string, { online: boolean }>({
      namespace: 'example:peers',
    });
    peers.get('a');                           // undefined, synchronous
    await peers.set('a', { online: true });    // keyed async write
    peers.get('a');                           // { online: true }
    await peers.dispose();

With Redis, the same namespace and Redis backend share authoritative state.
Install the optional Redis peer packages only when using the Redis backend (ioredis and redlock):

    const peers = new SyncedMap<string, { online: boolean }>({
      namespace: 'example:peers',
      redis: { url: process.env.REDIS_URL! },
      ttlMs: 30_000,
    });

    await peers.ready();                       // subscribe + hydrate
    const maybeStale = peers.get('a');          // immediate local Map read
    const latest = await peers.getFresh('a');   // authoritative Redis read
    await peers.set('a', { online: true });     // UMM lock + atomic write
    await peers.dispose();

Alternatively pass an existing ioredis client through redis: { client }.
SyncedMap uses it for Redis commands and UMM locks, creates one separate
subscriber connection, and never closes the caller-owned command client.
The client's configured options.db selects the database; Pub/Sub is scoped by
that database so equal namespaces in different Redis databases stay isolated.
Changing a supplied client's selected database after construction is unsupported.

**Use a separate namespace and security/deployment boundary per tenant.**
Namespaces are not an authorization mechanism.

## Fresh and locked reads

    await peers.readLocked('a', async (value, { assertLock, abortSignal }) => {
      // Redis read happened AFTER taking the shared key mutex.
      await performWorkWith(value, abortSignal);
      assertLock(); // detect lost lease before irreversible work
    });

readLocked(key, callback) begins with the latest committed authoritative value
at the locked read. Other *cooperating* SyncedMap writers cannot acquire that
key's mutex **while the UMM lease remains valid**. The lock spans the callback,
rather than only the initial fetch, but expiration or loss of the lease can
allow another writer to enter before the callback finishes (see below).

getFresh(key) reads Redis without locking; another writer can change the value
immediately afterward.

withLock(key, callback) supports several coordinated operations under a
single UMM lock. UMM 0.2.0 permits reentrant use within the same async context.

UMM uses leases: long operations must respect abortSignal / assertLock. A
stale writer that already issued a Redis command can still commit after its
lease expires: the version CAS does **not** check UMM's lock token. This is
not a strict fencing/linearizability guarantee across lease loss. A stale
write may succeed before a new holder changes the revision, even when that
holder is inside readLocked. Do not use this package alone to protect
irreversible external side effects or durable business invariants. Direct
Redis writes outside SyncedMap also bypass its mutex protocol.

## Atomic read-modify-write

    await map.update('counter', old => (old ?? 0) + 1);

This takes the key's UMM lock, reads Redis, invokes the updater, verifies lock
ownership, then performs an atomic version-checked Redis Lua commit. Conflicting
writes produce SyncedMapConflictError rather than silently clobbering state.

Update callbacks can be async but should be side-effect-free. They are not
automatically retried. Returning undefined deletes the entry.

set(), delete(), update(), and clear() are asynchronous. get(), has(), size,
entries(), keys(), values(), and iteration are synchronous.

## Replication

1. The Redis write, per-namespace revision increment, and Pub/Sub publication
   happen atomically in one Redis Lua script.
2. Other nodes update their local Map immediately upon receiving the message.
3. Revision checks reject out-of-order stale messages, including after deletion.
4. Startup subscribes before SCAN hydration and replays buffered notifications.
5. Subscriber reconnection triggers resynchronization. Periodic full
   reconciliation (default 5 minutes) repairs missed Pub/Sub notifications.

Pub/Sub has at-most-once delivery and is not an audit log. Distributed get()
is eventually consistent. To force a refresh, await map.resync(), or use
getFresh()/readLocked() for authoritative reads.

Set resyncIntervalMs: 0 to disable periodic reconciliation. Since resync
scans all entries in the namespace, keep each map scoped; this is not for
millions of keys.

    const unsubscribe = map.subscribe(change => {
      if (change.type === 'resync') return; // rebuild derived indexes if needed
      console.log(change.type, change.key, change.revision);
    });

## Values and TTL

- Keys must be strings; values must be JSON-serializable.
- Values are deeply frozen after JSON deserialization; mutate with set/update,
  not by modifying the object returned from get().
- ttlMs can be provided as a default or per write. Per-write ttlMs: 0 turns
  off a default expiry for that entry.
- Redis handles physical key expiry, while local reads check absolute expiry
  without waiting for a Pub/Sub notification. Fresh Redis reads use Redis's
  expiry decision, not the Node.js clock, which may be skewed.
- Local-only mode uses one timer and an indexed expiration queue to discard
  expired entries even when there are no subsequent reads. Expiry bookkeeping
  is O(log n) per changed expiring key and does not retain stale tombstones.
- Time-based expiry cannot be frozen by a mutex. Expiry itself does not publish
  a SyncedMap delete notification.
- clear() deletes keys individually and is not globally atomic; new writes
  concurrent with clear() may remain.

## Redis failures

When Redis mode is enabled, locked reads, authoritative reads, and writes
**fail closed** on Redis errors. The package does not automatically fall back
to unsafe local writes. Synchronous get() continues reading its potentially
stale local replica. Provide an onError callback to observe background
subscription and resynchronization failures.

Offline writes, CRDT merge semantics, distributed multi-key transactions,
user authorization, and durable event processing are deliberately out of scope.

## Build and test

    npm install
    npm run check

The suite covers local concurrency and **real Redis**, including separate Node
processes. It starts and stops an ephemeral redis-server; ensure redis-server
is on PATH. Builds provide ESM, CommonJS, and TypeScript declarations.

Release status: initial standalone implementation.
Not published to npm or integrated into any application.

## Compatibility

The source and distributable JavaScript target ES2015 (ES6) syntax.
Both native ESM and CommonJS are supported, with separate declaration entry
points for NodeNext and legacy TypeScript consumers. The package requires a
Node.js runtime (Node 18+) for built-in Node APIs and Redis dependencies;
ES2015 syntax is not a promise of Node 6/browser runtime compatibility.

The import entry point is dist/index.js with index.d.ts.
The require entry point is dist/index.cjs with index.d.cts.
Redis mode loads the optional peer packages only when Redis is configured. Purely local usage neither installs nor loads Redis/Redlock. The tiny optional-client loader is a separate lazy CommonJS module, so both ESM and CJS exports remain ES2015 syntax without a dynamic import.
