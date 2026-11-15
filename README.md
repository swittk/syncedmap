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

### Redis Cluster

A real **ioredis Cluster** client is also supported. Provide a caller-owned
client; a single Redis URL creates a standalone Redis connection, not a
Cluster-aware connection.

    import { Cluster } from 'ioredis';
    import { SyncedMap } from 'syncedmap';

    const cluster = new Cluster([
      { host: 'redis-a.internal', port: 6379 },
      { host: 'redis-b.internal', port: 6379 },
      { host: 'redis-c.internal', port: 6379 },
    ]);
    const shared = new SyncedMap<string, { online: boolean }>({
      namespace: 'presence',
      redis: { client: cluster },
    });
    await shared.ready();
    await shared.set('user-1', { online: true });
    const local = shared.get('user-1'); // synchronous local replica
    await shared.dispose();
    cluster.disconnect(); // caller owns the Cluster client

Each namespace uses a Redis hash tag, so its records and revision counter
reside in one slot and its multi-key Lua writes remain atomic. Different
namespaces can land on different Cluster primaries. Full reconciliation and
clear scan **every Cluster primary** because Redis SCAN is node-local; MGET,
reads, and writes are routed through the Cluster client. Periodic reconciliation
also checks slot topology: a reshard or failover forces a full scan even if
the revision counter appears unchanged. If topology changes *during* a full
scan, SyncedMap retries instead of installing a potentially incomplete
snapshot and rejects persistently unstable scans. Active MIGRATING/IMPORTING
slots also block full scans and `clear()` until the migration completes;
otherwise Redis could omit keys even when CLUSTER SLOTS looks unchanged.
`clear()` retries key collection on topology changes before deleting any keys.

Reconnection of the internal Cluster Pub/Sub socket triggers a full repair,
including automatic same-socket reconnection in older ioredis 5.x, even with
`resyncIntervalMs: 0`.

Redis Cluster uses only database 0. Configure the Cluster client with
\`scaleReads: 'master'\` (or leave it at the default); replica-read routing
would make hydration potentially stale. The client must not use keyPrefix.
Configure consistent noeviction or acceptable volatile-* memory policies on
**all masters and replicas that may be promoted**. The startup guard checks
all nodes currently advertised to ioredis, but a replica not yet advertised
may not be checked. Do not rely on dynamically inconsistent memory policies.

Replication events use normal Redis Pub/Sub, which is broadcast throughout
the cluster; sharded Pub/Sub is not used. Redis Cluster failover is
asynchronously replicated: a sudden primary failure can lose recently
acknowledged writes or lock state. This does not provide durability, strong
fencing across failover, or protection for financial/clinical invariants.
Use Redis Cluster for ephemeral shared state, with authoritative durable data
stored elsewhere.

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
5. Subscriber reconnection triggers full resynchronization. Periodic
   reconciliation (default 5 minutes) checks the revision first when Redis
   cannot evict keys; unchanged namespaces avoid SCAN/MGET entirely.
   Changed namespaces still receive a full scan. Volatile eviction policies
   retain full scans because entries can disappear without new revisions.

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

- Keys must be strings; values must survive a JSON stringify/parse round trip
  without changing their intended types. A Date, for example, becomes a string;
  class instances and Map/Set objects do not preserve their prototypes.
  Use plain JSON data, not objects requiring custom methods.
- Values are deeply frozen after JSON deserialization; mutate with set/update,
  not by modifying the object returned from get().
- ttlMs can be provided as a default or per write. Per-write ttlMs: 0 turns
  off a default expiry for that entry. Expiries beyond the safe JSON timestamp
  range are rejected before committing a write.
- Redis handles physical key expiry, while local reads check absolute expiry
  without waiting for a Pub/Sub notification. Fresh Redis reads use Redis's
  expiry decision, not the Node.js clock, which may be skewed.
- Every replica uses one timer and an indexed expiration queue to discard
  expired cached entries even without subsequent reads. The synchronous size
  read is O(1) unless due expirations must first be drained; TTL updates cost
  O(log n). Redis replicas retain revision tombstones against late messages.
- Time-based expiry cannot be frozen by a mutex. Expiry itself does not publish
  a SyncedMap delete notification.
- clear() snapshots keys first, then deletes them with up to 32 concurrent
  workers using the same per-key locks as individual writes. It is not
  globally atomic; new writes concurrent with clear() may remain. If one
  deletion fails, it waits for in-flight workers to settle before rejecting.

## Redis configuration

The per-namespace revision counter has no TTL, so a volatile-* eviction policy
cannot evict it. However, entries **with TTLs can be evicted early** under
volatile-* when Redis reaches maxmemory. Those evictions do not produce
SyncedMap notifications: another replica's synchronous get() may return the
evicted value until reconciliation or its recorded expiration. getFresh() and
readLocked() use authoritative Redis state and do not return the evicted entry.

Use noeviction when premature entry loss is unacceptable; note that Redis then
rejects writes when it runs out of memory. Use volatile-* only when such entry
loss is acceptable, and never use allkeys-* because it can evict the revision
counter. SyncedMap checks Redis INFO memory at initialization. Keep the eviction
policy unchanged while the map is running.

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

The suite covers local concurrency, **real standalone Redis**, and a real
six-node Redis Cluster (three primaries, three replicas), including separate
Node processes and both planned and abrupt primary failover. It starts and
stops temporary redis-server processes; ensure redis-server **and redis-cli**
are on PATH. Builds provide ESM, CommonJS, and TypeScript declarations.

The package is published on npm. Redis integration is optional; no
application-specific setup or deployment is bundled with the library.

## Compatibility

The source and distributable JavaScript target ES2015 (ES6) syntax.
Both native ESM and CommonJS are supported, with separate declaration entry
points for NodeNext and legacy TypeScript consumers. The package requires a
Node.js runtime (Node 18+) for built-in Node APIs and Redis dependencies;
ES2015 syntax is not a promise of Node 6/browser runtime compatibility.

The import entry point is dist/index.js with index.d.ts.
The require entry point is dist/index.cjs with index.d.cts.
Redis mode loads the optional peer packages only when Redis is configured. Purely local usage neither installs nor loads Redis/Redlock. The tiny optional-client loader is a separate lazy CommonJS module, so both ESM and CJS exports remain ES2015 syntax without a dynamic import.
