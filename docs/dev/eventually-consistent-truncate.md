# Eventually-consistent TRUNCATE for tablet-based tables

## Motivation

`TRUNCATE` cannot work reliably in the presence of concurrent writes:

1. It is not synchronized across nodes and shards. Each replica shard takes its
   own cut point (`truncated_at = now()` after that shard's flush, see
   `replica::database::truncate_table_on_all_shards`), so concurrent writes are
   kept or dropped arbitrarily depending on which shard they raced with.
2. It carries no write timestamp. The cut is physical: sstables are deleted by
   `max_data_age` (load/creation time) and racing memtable writes are silently
   dropped by replay position (`mutation_reordered_with_truncate_exception`).
   Since it does not participate in the timestamp-based conflict resolution
   that eventual consistency is built on, there is no well-defined outcome for
   writes concurrent with a TRUNCATE.

These problems are tracked by
[#11230](https://github.com/scylladb/scylladb/issues/11230) ("Truncate must
be based on a tombstone"), which this design fixes for tablet-based tables.
#11230 already outlines its core: one authoritative tombstone timestamp
applied on all nodes, sstables with `max_timestamp <= T` deleted whole,
sstables with `min_timestamp > T` left alone, the rest rewritten, and the
tombstone purged after gc_grace or repair. It differs in two respects: #11230
suggested propagating T through the schema, whereas here it lives in the
tablet map; and it treated auto-snapshot before T becomes visible as one of
the tricky parts, whereas here there is no auto-snapshot (see Execution flow).

[#16411](https://github.com/scylladb/scylladb/issues/16411) ("Make TRUNCATE
TABLE safe with tablets") was resolved by making TRUNCATE with tablets a
topology operation, serialized against migration and repair. It does not
address the lack of a write timestamp; the discussion there also considered
token-range tombstones, see Alternatives below.

This document proposes making `TRUNCATE` on tablet-based tables
**eventually consistent**: a TRUNCATE carries a write timestamp, either
provided by the server or by the client through a new syntax extension:

```
TRUNCATE [TABLE] <keyspace>.<table> [USING TIMESTAMP <t> [AND TIMEOUT <d>]]
```

Its semantics are **those of a single-partition `DELETE`, applied to every
partition of the table**: a tombstone `T = {timestamp, deletion_time}` that
shadows every cell with `write_timestamp <= T.timestamp`, applied by
timestamp comparison on read and on compaction, propagated to all replicas,
and eventually garbage-collected under the same rules as partition tombstones
(gc_grace / repair-based tombstone GC).

This is cheap in practice, not only correct. A TRUNCATE timestamp is
typically larger than all or most write timestamps in the table, so nearly
every sstable is *fully* shadowed by T and compaction drops it whole without
reading it, the same way TWCS drops fully-expired sstables, including any
tombstones it holds, with no tombstone GC checks (see Compaction below). Disk
space is reclaimed almost as promptly as with today's physical cut.

The GC requirement is essential, not incidental: truncates are sometimes
erroneous, and the recovery path is to re-add the original data with its
original write timestamps (e.g. `nodetool refresh --load-and-stream`
replaying the original mutations). That only works if the table-wide
tombstone is eventually purged, exactly as a partition tombstone would be.

### Scope

- **Tablet-based, eventually-consistent tables** only.
- Vnode keyspaces keep the existing TRUNCATE behavior; `USING TIMESTAMP` is
  rejected there.
- Strongly-consistent tables keep their own TRUNCATE, ordered in each
  tablet's Raft log with a leader-assigned `truncated_at` write timestamp
  (#31576, #31772). With linearizable writes, a physical cut ordered in the
  log is already correct.
- The existing TRUNCATE-with-tablets implementation, a topology operation
  that makes a physical cut, stays in place until the new cluster feature is
  enabled, and is then superseded for eventually-consistent tablet tables.

## Design summary

The design separates the *authoritative fact* from its *observation*:

- **The fact** — "table X data is truncated as of timestamp T" — is a
  per-table record in the tablet map (`system.tablets` static columns),
  written through a group0 mutation. Group0 gives atomic, linearizable,
  cluster-wide propagation to every current and future replica with no
  dependency on replica liveness, hints, or repair.
- **The observation** — readers, compaction and the write path *strip*
  covered data: every atom with `ts <= T.timestamp` is dropped from the
  stream, and T itself is never emitted. No new mutation fragment kind, and
  no commitlog, memtable or sstable representation of T, is required.
- **GC of the fact** — a coordinator-driven, per-tablet protocol removes T
  from the tablet map once (a) T satisfies the table's tombstone GC policy
  (gc_grace elapsed, or repair-confirmed in `repair` mode), and (b) every
  tablet replica has confirmed that no covered data remains in its sstables
  or memtables. Until both hold, T keeps shadowing covered data everywhere,
  so there is no resurrection path.

### Alternative: token-range tombstones in the data path (#31496)

The main alternative represents T as a token-range tombstone that is part of
the data itself: a `(start-token, end-token]` interval carrying a tombstone,
participating in mutation algebra and written like data through commitlog,
memtable, cache and sstables. It was first suggested in the #16411
discussion
([comment](https://github.com/scylladb/scylladb/issues/16411#issuecomment-1854277583),
[follow-up](https://github.com/scylladb/scylladb/issues/16411#issuecomment-1873005060)),
and #31496 implements it for TRUNCATE.

Besides making the replica self-contained (below), that discussion points
out that a token-range tombstone makes TRUNCATE a *data-plane* operation:
the coordinator writes the tombstone for each tablet's range at a
consistency level, like an INSERT, so a TRUNCATE at `LOCAL_QUORUM` succeeds
when only the local datacenter is available. It also need not wait for
anything, including memtable flushes. The tablet-map design shares the
latter property (no flush or other replica-side work is on the statement's
critical path), but not the former: writing T requires a group0 majority,
so TRUNCATE is a control-plane operation that cannot complete while group0
has no quorum, e.g. when only one datacenter of several is reachable.

Its main advantage is that a replica is self-contained: memtables plus
cache/sstables hold all the information needed to produce a mutation, with
no separately-managed metadata. In particular, snapshots, backup/restore and
load-and-stream carry the tombstone together with the sstables. It also
works for vnode keyspaces.

Its cost is breadth. `mutation` is per-partition by IDL definition, the
memtable is keyed by partition, and nothing above the partition level exists
in the fragment stream. #31496 therefore extends the sstable format (Scylla.db
component, sstables without partitions), the fragment stream, frozen
mutations and their serialization, memtables and the cache, the combining
reader, compaction (split, purge, whole-sstable drop), the tools, and the
repair protocol. Repair is the subtle part: row-level repair hashes
per-partition fragments and only sees partitions that exist on some replica,
so a replica that missed the tombstone would never get it for partitions
only it holds. Hints are per-partition mutations and have the same gap.
#31496 adds a per-range tombstone exchange to repair to close it.

The tablet-map design needs none of these format, wire or protocol changes:
group0 is the propagation channel, and the data path only learns to drop
atoms by timestamp. In exchange it must capture T in snapshot manifests and
apply it on restore, it must run a distributed protocol to garbage-collect
T, where a data-path tombstone would be purged locally by compaction, and
TRUNCATE depends on group0 availability rather than on a per-tablet
consistency level. This
design does not pursue token-range tombstones.

### Relation to existing mechanisms

- TRUNCATE with tablets (`TRUNCATE_AS_TOPOLOGY_OPERATION`) coordinates a
  *physical* cut through the topology state machine: barriers, sessions,
  serialization against migrations, and a hard requirement that every
  replica is alive. A timestamped TRUNCATE needs none of that for
  correctness: the timestamp decides every conflict, so a plain group0
  mutation suffices, and a replica that is down simply observes T when it
  comes back and catches up on group0. This removes the requirement that
  every replica be alive.
- `table::truncate_tablet_locally` (#31576) is the local half of
  strongly-consistent TRUNCATE (#31772): a physical, per-tablet cut ordered
  by the tablet's Raft log, with a leader-assigned `truncated_at` timestamp.
  It is prior art for per-tablet coordination and for carrying a write
  timestamp with TRUNCATE, but it is not reusable as the mechanism here.
- The per-tablet `repair_time` → `tombstone_gc_state` interval-map flow
  (`replica/tablets.cc`, `tombstone_gc.cc::flush_pending_repair_time_update`)
  is the proven pattern for projecting tablet-map state into per-shard lookup
  structures consulted on read/compaction hot paths; `sstables_repaired_at`
  is the proven pattern for compaction consulting per-tablet metadata.

## Semantics

Let `T = {timestamp, deletion_time}` where `timestamp` follows the same rules
as DML: `USING TIMESTAMP` if given, else the client-side default timestamp
sent at the protocol level if any, else the coordinator's clock; and
`deletion_time` is the group0 commit time (gc_clock).

- A cell (or row marker, or narrower tombstone) with
  `write_timestamp <= T.timestamp` is shadowed, table-wide.
- A write with `write_timestamp > T.timestamp` survives, regardless of
  whether it was applied before or after the TRUNCATE executed. There is no
  "reordered with truncate" error and no silent drop: commitlog replay,
  batchlog replay, hint replay and Paxos learn need no special-casing — a
  replayed old-timestamped mutation is simply shadowed by T, exactly as it
  would be by a partition tombstone.
- Multiple TRUNCATEs compose like tombstones: the table record keeps
  `max(T_old, T_new)` under `tombstone::apply` ordering (timestamp, then
  deletion_time). A TRUNCATE with a timestamp lower than the current T is
  absorbed, same as applying an older partition tombstone over a newer one.
- `USING TTL` is not supported. `USING TIMEOUT` keeps its meaning.
- Permission (`MODIFY`) and the restrictions on views and system tables are
  unchanged from today's TRUNCATE.

### Behavior changes compared to the physical cut

With the cluster feature enabled, TRUNCATE on an eventually-consistent tablet
table differs from today's in ways users can observe:

- Data written with a timestamp **above** T (e.g. a client clock ahead of
  the coordinator's, or an explicit future `USING TIMESTAMP`) survives the
  TRUNCATE. Today it is removed physically, regardless of its timestamp.
  In particular, most drivers send a client-side timestamp with every
  request by default, which T then follows: if the truncating client's
  clock is behind the clocks of the clients or nodes which wrote the data,
  data written shortly before the TRUNCATE survives it.
- Data written **after** the TRUNCATE with a timestamp at or below T is
  dropped. Today it survives.
- Nothing is removed from disk when the statement returns. Disk space is
  reclaimed by compaction, mostly by dropping fully-shadowed sstables whole.
- Counter state is shadowed by T rather than physically reset, and counter
  updates concurrent with the TRUNCATE can resurrect pre-truncate counts;
  see Counters below. As with the physical cut, and unlike a DELETE,
  counters can be updated again after the TRUNCATE.

These follow directly from the DELETE semantics and are the point of the
change, but they need to be documented in the user-facing TRUNCATE
documentation.

## The fact: tablet map record

New static (per-table) columns in `system.tablets`, next to `resize_type`
etc.:

- `truncated_at_timestamp` (bigint, api::timestamp_type)
- `truncated_at_deletion_time` (timestamp, gc_clock)
- GC protocol state (see below).

These are set only by the eventually-consistent TRUNCATE. They are unrelated
to the legacy truncation records in `system.truncated` and to the
`truncated_at` time cached on `replica::table`, which legacy TRUNCATE uses for
commitlog replay filtering; the code must carry a comment saying so.

Static columns are the right scope: T is table-wide and must be set
atomically for all tablets in one group0 write; this also sidesteps
`merge_tablet_info`'s requirement for a per-tablet merge rule on tablet
merge. A static-column change forces a full reread of the table's
`system.tablets` partition in the change-hint path — fine for a rare
operation.

In-memory, T must be keyed by `table_id`, **not** stored as a field of the
shared `locator::tablet_map` object: co-located tables (views with matching
partition key, Paxos state tables, and the CDC log table) share the base
table's `tablet_map` pointer, and T must *not* leak to co-located tables
implicitly — in particular never to Paxos state tables or the CDC log table
(see Paxos and CDC below). T therefore lives
in `tablet_metadata` as a `table_id → truncated_at` map (beside
`_base_table`), persisted in each table's own `system.tablets` partition; the
loader today reads only `base_table` from a co-located table's partition, so
that load path is extended. T is cached on `replica::table`, refreshed in
`tablet_storage_group_manager::update_effective_replication_map`, which
already diffs old/new tablet maps per shard. No per-token tablet lookup is
added to any hot path.

### Execution flow of the statement

1. `truncate_statement` accepts `USING TIMESTAMP`, in addition to
   `USING TIMEOUT` (the grammar uses `usingTimestampTimeoutClause`). The
   timestamped TRUNCATE is used for tablet tables which are not strongly
   consistent once the `EVENTUALLY_CONSISTENT_TRUNCATE` cluster feature is
   enabled; `USING TIMESTAMP` is rejected otherwise. Logstor tables keep the
   physical cut for now, since their read and write paths do not strip
   covered data yet.
2. The statement computes T — `USING TIMESTAMP` if given, else the
   client-side default timestamp sent at the protocol level, else the
   coordinator's clock; the deletion time is the coordinator's `gc_clock` —
   and calls `storage_service::truncate_with_timestamp()`, which, on shard 0,
   writes `truncated_at = max(current, T)` for the table **and for every
   materialized view / secondary index of the table** in one group0 command
   (see Views below), retrying on a group0 conflict. Paxos state tables and
   the CDC log table are excluded.
3. It then waits until the live nodes apply the group0 command, with
   `raft_group0_client::send_group0_read_barrier_to_live_members()`, so that
   a subsequent read through *any* coordinator observes the truncate. Dead
   nodes are skipped without harming correctness: a node that missed the
   command cannot serve reads without first catching up on group0, so the
   barrier is only about prompt visibility of the acknowledgment, never
   about the truncate taking effect.

No flush, no compaction stop, no replica RPC fan-out, no all-replicas-alive
requirement.

There is no auto-snapshot on this path. Unlike the physical cut, the
timestamped TRUNCATE does not remove anything at execution time, and
recovery from a mistake has a first-class path: either reinsert with newer
timestamps, or after T's GC, load-and-stream the original data. Applications
that want a pre-truncate snapshot can take one before issuing the statement.
Two orthogonal future extensions could make that self-service from CQL (out
of scope for the initial implementation):

- exposing snapshot management over CQL (today it requires nodetool or the
  REST API), so an application can take and manage snapshots itself;
- a `WITH SNAPSHOT[=<tag>]` clause on TRUNCATE — and equally on DROP KEYSPACE
  and DROP TABLE — taking a snapshot as part of the statement.

## The observation: stripping covered data

All readers and the write path *strip* covered data: they drop every atom
with `ts <= T.timestamp` — cells, row markers, row tombstones, range
tombstones and partition tombstones — and never emit T itself.

Stripping is sound per source as well as after merging sources: everything
dropped has `ts <= T` and everything kept has `ts > T`, so no dropped
tombstone could have shadowed kept data. The alternative of *injecting* T
into each partition's tombstone was considered and rejected:

- The compactor writes the partition tombstone it receives into its output
  unless it is purgeable, and once T is merged with a real partition
  tombstone the two cannot be told apart, so compaction would materialize T
  as per-partition tombstones in every output sstable.
- Partition tombstones on every partition would force disabling the TWCS
  single-key fast path (guarded by `may_have_partition_tombstones`) and the
  `clustering_order_reader_merger` assumption that there are no partition
  tombstones.
- Repair would see spurious differences: a replica that has compacted a
  partition away emits nothing for it, while one that has not yet emits
  `partition_start(T)`.

Under stripping, consumers see ordinary streams. A partition whose content
is entirely covered becomes an empty `partition_start` / `partition_end`
pair, which `mutation_compactor` does not emit (it emits a partition only
once it has content) and which repair skips (`repair/row_level.cc` ignores a
`partition_start` with no tombstone).

### Read path

Stripping is applied where data leaves each *store* — each sstable and each
memtable — rather than on the merged output of a reader. Besides being
sound per source, this is required for counters: merging counter cells keeps
the shard with the higher logical clock, so merging a covered shard with a
fresh, post-truncate one before stripping would keep the covered shard (see
Counters below). The stripping adapter is
`make_truncation_stripping_reader()` (`readers/truncation_stripping.hh`).
It strips row and range tombstones by their timestamp regardless of their
deletion time, and strips the regular and the shadowable row tombstones
separately, so that T is never merged into what is left of them. It drops
rows that end up empty and range tombstone changes that end up redundant.

**Sampling T.** All the readers of a single read must apply the same T: a
read that strips some of its sources with one T and others with another can
expose a value that was never current (see Row cache below). But the readers
of a read are created at different times, e.g. the per-sstable readers of a
scan are opened lazily as it advances. So T is sampled once per read, in the
read's `reader_permit`: `reader_permit::truncation_tombstone(schema)` samples
`schema::truncation_tombstone()` on its first call for the table, and returns
the same value to every later call. `schema::truncation_tombstone()` returns
the T installed on the table this shard holds for the schema (through the
schema registry entry's link to the table), so it is the same for all
versions of the table's schema, including reversed ones and older versions
used by a lagging coordinator; it is `std::nullopt` for a schema that is not
attached to a table, e.g. in offline tools.

**Resuming saved readers.** A paged query's readers, saved between pages,
keep the T they sampled. When a page resumes them after T changed, they are
dropped and recreated rather than updated in place, since their sources may
already hold buffered fragments stripped with the old T:

- The querier cache (single-partition queries) treats a saved querier whose
  permit sampled a different T as unusable for the page
  (`can_use::no_truncation_tombstone_mismatch`), so the page is read by new
  readers.
- An evictable reader (range scans) that is resumed after T changed closes
  the resumed reader and recreates it from its saved position, as if it was
  evicted. Recreating a reader resamples T.

Long-lived readers that are never parked — compaction, repair, streaming —
keep the T they sampled for their whole lifetime; data covered by a newer T
is handled by the write path and by later compactions.

**Where it is applied.**

- `sstable::make_reader()`, which every per-sstable reader goes through:
  queries, the cache's population source, reads with the cache disabled,
  repair and streaming readers, compaction readers (so compaction output
  holds no covered data, see Compaction below), view building from staging
  sstables, and load-and-stream at the source. An sstable whose
  `min_timestamp` is above T is not wrapped.
- Memtable readers and the memtable flush reader (see Memtables below).
- The cache's output (see Row cache below).
- `mutation_dump` and `scylla-sstable`-class tools read raw stores and show
  covered data; `mutation_dump` should expose T, and the offline tool
  documents the caveat.

When T is not set, nothing is wrapped and there is no cost. An sstable with
`max_timestamp <= T.timestamp` need not be read at all — the read-side twin
of whole-sstable expiry (an optimization, not yet done). A memtable needs
wrapping only if it was created before T was installed, since writes applied
to a memtable are stripped of the data covered by the T it was created with
(see Write path), and the cache's output only while it is being invalidated
(see Row cache).

A replica that has not yet observed T (lagging on group0) may send covered
rows to an up-to-date replica during repair; the receiver strips them on
write. That wastes some transfer but cannot resurrect data, so correctness
does not depend on replicas agreeing on T during repair.

### Write path

While T is set, the replica write path strips covered data from incoming
mutations. The order in `database::do_apply` is view-update generation
(`push_view_replica_updates`), then the commitlog, then `apply_in_memory`.
Stripping happens **before the commitlog**: otherwise a segment holding
covered data could be replayed after T's GC and resurrect it. Stripping
before view-update generation also avoids building view updates for writes
that are dead on arrival. If nothing is left, the write is skipped. All apply
variants are covered: `do_apply`, `do_apply_many` and `apply_counter_update`.
The logstor branch, which keeps its own log, must follow the same rule; it is
not covered yet.

Stripping must also happen where data enters a memtable (`table::apply`),
not only before the commitlog. Commitlog replay inserts mutations through
`database::apply_in_memory`, bypassing `database::do_apply`, and a memtable
stamped with T (see Memtables) is relied upon to hold no data covered by T.
Without stripping at insertion, replay at boot would insert unstripped
pre-truncate writes into such a memtable. Stripping at insertion is done
synchronously with applying the mutation to the active memtable, against the
table's current T, which the active memtable is always stamped with (a new T
switches it, see Memtables), so that no T can be installed in between.

Stripping is implemented by `mutation_partition::strip_truncated()` (and the
`deletable_row` and `row` methods it uses, which the stripping reader uses
too), after `has_truncated()` found that the mutation holds covered data at
all. Mutations written after the TRUNCATE normally hold none, so the common
cost is checking that, which currently requires unfreezing the mutation; a
check on the frozen mutation would avoid that.

Only writes applied before T arrived remain unstripped in the commitlog.
Replaying them strips them at insertion, and T's GC requires a flush, which
retires those segments. So boot ordering between loading the tablet
metadata and replaying the commitlog does not matter.

### Row cache

Leaving covered data in the cache would be correct for reads as long as T is
set, since it would be stripped on the way out. It is nevertheless not left
there, for two reasons:

- **Correctness of T's GC.** T may be cleared only once no covered data
  remains anywhere on the replica (see GC of T). If the cache still held
  covered data at that point, it would be resurrected.
- **Memory.** Since a TRUNCATE typically covers nearly all of a table's
  data, nearly all of its cached data becomes garbage, and should be
  released promptly rather than waiting for LRU eviction.

The cache is therefore kept free of covered data by construction:

1. **Invalidate once, when T changes.** When a replica installs a new T
   for a table, it invalidates the table's cache with
   `row_cache::invalidate(external_updater, full range)`, in the background:
   T itself is installed synchronously, as part of applying the tablet
   metadata change on each shard, which cannot wait. The invalidation is
   preemptible, and population reads started before it are discarded by the
   cache's phase tracking.

   Until the invalidation completes, the cache may still hold covered data,
   so readers created in the meantime strip the cache's output too
   (`table::cache_may_hold_truncated_data()`; the table tracks the T up to
   which the cache is known to be clean). This is not about visibility —
   like a write, a TRUNCATE need not be visible to reads concurrent with it,
   and is visible to reads that start after it returns, since the barrier
   it waits for completes only once every live node has installed T. It is
   about applying T uniformly within a single read: stripping some of a
   read's sources but not others can expose a value that was never current,
   e.g. a cell written at t1 in the cache and overwritten at t2 in a
   memtable, with t1 < t2 <= T: stripping the memtable but not the cache
   returns the t1 value, which is neither the pre-truncate state (t2) nor the
   post-truncate one (nothing).
2. **Populate only stripped data.** The cache populates from
   `table::sstables_as_snapshot_source`, which captures the sstable set when
   the snapshot is taken; it captures T together with it, and its readers
   are stripped. The external updater in step 1 makes the new snapshot take
   effect atomically with the invalidation.
3. **Merge only stripped memtables.** On flush, a memtable is merged into
   the cache (`row_cache::update`) by moving its partitions directly, not
   through a reader, so a memtable that may hold covered data must not be
   merged as is (see Memtables below).

The cost is one full-table invalidation per TRUNCATE, using existing
mechanisms. Reads repopulating the cache afterwards are cheap: they mostly
hit sstables that are entirely below T and are skipped without being read.
Clearing T in the GC protocol needs no invalidation: by then the cache holds
no covered data, and clearing T does not change the visible data.

### Memtables

A memtable created after T arrived holds only stripped data (write-path
stripping), and needs no special handling. A memtable that existed before T
arrived may hold covered data:

- **Reads** from it are stripped (above).
- **Flushing it** writes through the memtable flush reader
  (`memtable::make_flush_reader`), which is stripped, so covered data never
  reaches disk.
- **Merging it into the cache** at the end of the flush
  (`table::update_cache`) uses `row_cache::update_invalidating` instead of
  `row_cache::update`: it invalidates the cache ranges the memtable touched
  rather than moving its raw partitions into the cache. Each memtable
  records the T in effect when it was created, so this applies only to
  memtables older than the current T, and to any memtable while the cache
  is being invalidated and may still hold covered data, which merging
  could combine in memory with the memtable's data (e.g. counter shards).
  The decision is made when the flush completes, so it also covers a flush
  that was already in progress when T was installed.

When a shard installs a new T for a table, it **switches the table's active
memtables** to new, empty ones atomically with installing T, before any
further write is applied to them. Pre-truncate and post-truncate data then
never share a memtable, so no write is ever merged in memory with covered
data before stripping can remove it. This matters for counters (see
Counters below) and is harmless for everything else. An empty active
memtable is not switched, only stamped with the new T.

The switched-out memtables are then **flushed**, in the background. Unlike a
memtable switched out by a regular flush, such a memtable has no flush in
progress until the background flush obtains a flush permit, so:

- `compaction_group::flush()` waits for these background flushes too, so
  that every flush (`nodetool flush`, snapshot, drain, tablet cleanup, etc.)
  still covers all memtable data.
- The background flush skips a memtable that was dropped meanwhile, e.g. by
  `truncate_tablet_locally()`, which takes all flush permits before dropping
  the tablet's memtables; flushing it would resurrect what was dropped.
- It holds the compaction group's flush gate, so the compaction group is not
  destroyed before the flush completes.

Flushing them releases the memory held by covered
data immediately rather than whenever the memtable would next be flushed,
and it retires the commitlog segments holding pre-truncate writes, which
shortens commitlog replay. Its cost is close to zero: covered data is
stripped by the flush reader and never written, so only data written with
timestamps above T is written out, which right after a TRUNCATE is
typically little or nothing. The same flush is a precondition of T's GC
anyway (see GC of T).

### Staging sstables

Staging sstables hold base table data received by repair, streaming or
load-and-stream whose view updates have not been generated yet. They are
not part of the compaction set, so neither regular nor major compaction
cleans them, and they must be handled by the TRUNCATE itself. When a replica
observes a new T for a table with views:

- **Fully-shadowed staging sstables** (`max_timestamp <= T.timestamp`) are
  removed from the view update generator's queue and deleted. Every view
  update they would generate is covered by the T set on the views, so there
  is nothing to generate.
- **Partially-covered staging sstables** are processed as usual, but the view
  update generator reads them through a stripping reader, so no view updates
  are generated for covered data. They then move to the main set, where
  compaction cleans them like any other sstable.

Staging sstables arriving after T is set take the same path. A tablet with a
staging sstable whose `min_timestamp <= T.timestamp` fails the cleanliness
check (see GC of T) until that sstable has been processed and compacted.

## Compaction: purging covered data

- Compaction readers strip covered data (above), so compaction output simply
  omits it. **Covered data is purgeable immediately**, without waiting for
  gc_grace. The grace period exists to keep a *tombstone* around until it
  has provably reached all replicas; the *data under it* may be dropped as
  soon as the tombstone is compacted together with it, provided the
  tombstone itself survives. Here T survives in the tablet map until the GC
  protocol removes it.
- The per-key `max_purgeable` bloom-filter check does not apply to dropping
  data *under* T (T shadows the whole table; there is no older sstable whose
  data must be preserved against it). It continues to apply, unchanged, to
  purging narrower tombstones.
- **Whole-sstable expiry**: an sstable with `max_timestamp <= T.timestamp`
  is fully shadowed and is dropped without rewriting (extend
  `get_fully_expired_sstables`). Since a TRUNCATE timestamp is typically
  larger than all or most write timestamps, this is the common case, and it
  reclaims disk space almost as promptly as the physical cut. Note the
  comparison is on *data timestamps*, never `max_data_age`.
- This is done in `get_fully_expired_sstables()`, which compaction uses to
  find the sstables it can drop without reading them; major compaction
  therefore drops all of them. It uses the table's current T, through
  `schema::truncation_tombstone()`.
- The tombstones in a fully-shadowed sstable are dropped along with it,
  without interpreting the sstable and without applying the tombstone GC
  rules (gc_grace or repair time, overlapping sstables' and memtables' min
  timestamps). Unlike TWCS fully-expired sstables, no such checks are
  needed. Every tombstone in the sstable has a timestamp `<= T.timestamp`,
  so all data it shadows has a timestamp `<= T.timestamp` too and is covered
  by T, wherever it lives: in other sstables, in memtables, on other
  replicas, in hints, or in the commitlog. While T is set, that data is
  stripped on read, on write and on commitlog replay, so it cannot be
  resurrected by the dropped tombstones' absence. T itself is GC'd only once
  no covered data remains anywhere (see GC of T), so the guarantee survives
  T's removal.
- T is **never** written into compaction output. The tablet map record
  remains its single representation until its GC completes.

## GC of T

T may be removed from the tablet map when both hold:

1. **Grace period** (same rule as any tombstone):
   `T.deletion_time < gc_before` for the whole table, per the table's
   `tombstone_gc` mode — `timeout`: gc_grace elapsed; `repair`: every tablet's
   `repair_time >= T.deletion_time` (the per-tablet repair times are already
   in the tablet map); `immediate`/`disabled` as usual. Note what this
   condition protects against: *not* T reaching replicas (group0 guarantees
   that — a replica cannot serve reads without having caught up on group0),
   but late-arriving writes with timestamps `<= T.timestamp` that were in
   flight when the truncate happened — hints, batchlog entries, pending view
   updates — the same role gc_grace plays for partition tombstones. Such a
   write arriving while T is set is stripped; arriving after T's GC it
   resurrects, same as after a partition tombstone's GC.
2. **No covered data remains** on any replica of any tablet: dropping T while
   some sstable, memtable or cache entry still holds cells with
   `ts <= T.timestamp` would resurrect them. This is the table-wide analogue of the local `max_purgeable` check,
   and it must be a positive, per-replica confirmation.

For condition 2 to be checkable at all, per-tablet confirmations must be
**monotonic**: once a replica confirms a tablet clean, no covered data may
appear in it again while T is set. Write-path stripping (above) makes this
an invariant for everything that goes through the write path: hints,
batchlog replay, late view updates, commitlog replay, and explicit
old-timestamp client writes cannot un-confirm a tablet. Paths that attach
sstables directly, bypassing the write path, must either rewrite the sstable
against T on attach or reset the affected tablets' confirmations:

- `nodetool refresh`: with load-and-stream the data arrives at the receiving
  replica as mutations and is stripped; a plain local attach must rewrite or
  reset.
- `restore_tablet`.
- Tablet migration/rebuild file-based streaming. Migration resets the moved
  tablet's confirmation in any case, since confirmations are recorded
  against the current replica set.

**Tablet cleanliness check.** A replica's copy of a tablet is *clean* with
respect to T when, checked from metadata alone, without reading any data:

- every sstable of the tablet — in the main, maintenance and staging sets —
  has `min_timestamp > T.timestamp`, and
- every memtable of the tablet was created after the replica observed T
  (its content was stripped on write).

The cache needs no check once it is invalidated (see Row cache). The check is
implemented by `replica::table::may_hold_truncated_data()`.

**Implemented protocol.** The topology coordinator runs a background fiber
(`garbage_collect_truncation_tombstones()`, every 60 seconds). For each table
with T set whose tablets have no transitions in progress and for which
condition 1 holds, it asks every replica host of the table, over the
`truncation_tombstone_clean` RPC, whether it holds any covered data of the
table, checked as above on all shards. If none does, it clears T in a group0
command, unless another TRUNCATE replaced T meanwhile. Each table (base,
views, indexes) has its own T and is cleared independently.

- Condition 1 is evaluated from the tablet map: in `repair` mode, every
  tablet's `repair_time`, minus the propagation delay, must be after
  `T.deletion_time` (tablets with a single replica need no repair); in
  `timeout` mode, `gc_grace_seconds` must have passed since
  `T.deletion_time`; `immediate` needs nothing, and `disabled` never GCs T.
- There is no persisted per-tablet confirmation, and so no merge rule for
  it: all the replicas of a table are checked at once, and the check is
  retried until it succeeds. This requires all of them to be reachable.
- There is no sweep: the coordinator does not make replicas clean. Writes
  are stripped (see Write path), memtables older than T are flushed when T
  is installed, and compaction strips covered data and drops sstables
  holding only covered data, so a replica becomes clean as its sstables are
  compacted. **Major compaction makes a replica clean**, so running it on
  all nodes (e.g. `nodetool compact`) makes T's GC complete promptly, as the
  restore procedure does. When a major compaction of a table with T set
  completes, the replica logs whether it still holds covered data.
- Footnote: a replica lagging on group0 keeps stripping incoming covered
  writes for the lag window *after* T is cleared. The window is bounded by
  group0 catch-up, and only affects writes that deliberately use timestamps
  older than a GC'd tombstone — the same hazard window exists for partition
  tombstones between compaction purging them on different replicas.

**Not implemented yet.**

- Rewriting or resetting on paths that attach sstables directly (above):
  `nodetool refresh` without load-and-stream and `restore_tablet` can attach
  covered data to a replica after it was checked and before T is cleared.
- Staging sstables are not dropped or stripped when T is installed (see
  Staging sstables); a tablet holding covered staging sstables is just not
  clean until they are processed and compacted.
- The status command (see Verifying T's GC); `system.tablets` is the only
  way to see whether T was cleared.
- Logstor tables (see Write path).

### Alternatives and future work

The following refinements of the protocol were considered and may be added
if needed:

- **Replicas report cleanliness.** When a major compaction completes, the
  replica notifies the topology coordinator, which then checks the table
  without waiting for its next round.
- **Per-tablet confirmations.** The coordinator records, in a per-tablet
  column, which tablets were confirmed clean (which needs a
  `merge_tablet_info` rule: merged tablet = both halves confirmed, otherwise
  not; split copies; migration resets the moved tablet's confirmation), so
  that the protocol makes progress tablet by tablet and does not need all
  the replicas of a table to be reachable at once.
- **A sweep.** Once condition 1 holds, the coordinator asks each tablet
  replica to make the tablet clean: sstables with
  `min_timestamp <= T.timestamp` are rewritten (a targeted compaction that
  drops covered cells) or dropped if fully shadowed, after flushing
  memtables. In the common case, whole-sstable expiry has already removed
  covered data by then, so the sweep mostly checks metadata and rewrites
  little.

After T's GC, loading pre-truncate sstables (restore, load-and-stream with
original timestamps) resurrects their data — by design, identically to
post-GC behavior of partition tombstones.

## Views, indexes, Paxos, CDC, counters

- **Materialized views and secondary indexes**: as today, truncating the
  base table truncates its views and indexes. The same group0 command sets T
  (same timestamp) explicitly, per table_id, on every view and index backing
  table — one atomic group0 write covering base and all views; this is
  *more* atomic than today's TRUNCATE, which removes base and view sstables
  non-atomically (#17635). Hazard note: because co-located tables share the
  base's `tablet_map` object, T must be keyed by table_id (see above) so
  that setting it on the base never implicitly applies it to other members
  of the co-location group. View-building racing the truncate is naturally
  correct: built view rows carry base-derived timestamps `<= T.timestamp`
  and are stripped.
  - Caveat to verify during implementation: view row markers and shadowable
    tombstone timestamp derivation — confirm that stripping by the base
    timestamp removes all view rows derived from covered base rows,
    including rows created by view updates in flight at truncate time.
- **Paxos (LWT)**: T is **never** set on Paxos state tables, even though they
  are co-located with the base. Paxos state rows carry ballot-derived
  timestamps; a TRUNCATE with a future `USING TIMESTAMP` would otherwise
  shadow every promise/accept until wall-clock passes T, silently breaking
  LWT on the table. Today's TRUNCATE likewise never truncates the Paxos
  table — it relies on the `paxos_state::learn` `truncated_at` check instead.
  For the timestamped TRUNCATE no Paxos special-casing is needed at all: a
  decided mutation whose cells have timestamps `<= T.timestamp` is learned
  and applied normally, then stripped like any other write.
- **CDC**: the timestamped TRUNCATE keeps today's CDC behavior.
  - Today neither TRUNCATE implementation touches CDC. The tablets TRUNCATE
    (`truncate_table_on_all_shards`) truncates the base table and its views
    and indexes only; it neither truncates the CDC log table nor writes a
    log entry recording the truncate. This is documented in
    `docs/features/cdc/cdc-log-table.rst`, which recommends a manual
    procedure for keeping the two in sync (stop writes, consume the log,
    truncate both, resume). Strongly-consistent tables do not support CDC
    at all (`cdc/log.cc` rejects creating a CDC log in a strongly-consistent
    keyspace), and #31772 has no CDC handling. #31496 has none either.
  - **No CDC log record** is written for a TRUNCATE. CDC log rows are
    partitioned by stream, and a table-wide event has no natural partition:
    it would have to be written into every stream of every tablet. Existing
    consumers (e.g. the Kafka connector) would also see an operation type
    they do not understand. This would be a separate feature.
  - **T is not applied to the CDC log table.** Doing so would delete log
    entries that consumers have not read yet, which is exactly what the
    manual procedure exists to avoid. On tablets the CDC log table is
    co-located with its base table (`replica/database.cc`,
    `cdc::is_log_schema`) and shares its `tablet_map`; keying T by
    `table_id` keeps it from inheriting T implicitly.
  - Log entries follow DELETE semantics. A base write with a timestamp at or
    below T that arrives after the truncate is stripped on the base
    replicas, but its CDC log entry, generated on the coordinator, is still
    written — the same as for a write shadowed by a partition tombstone: CDC
    logs operations, not their effects. Preimages are computed from base
    reads, which are stripped, so they reflect the post-truncate state.
- **Counters**: counter cells covered by T are stripped like any other cell,
  so a counter reads as absent after the TRUNCATE, and an increment after it
  starts from zero: the counter leader's read-before-write is stripped, so
  the leader writes a fresh shard. Unlike the physical cut, the timestamped
  TRUNCATE does not reset counter state physically before T's GC completes.

  T is **not** a counter tombstone. A counter cannot be used again once
  deleted: any tombstone covers a counter cell, regardless of their
  timestamps (`atomic_cell::is_covered_by()`, see also
  `docs/features/counters.rst`). If T behaved like a DELETE of every
  partition, a truncated counter could not be updated again, at least
  until T's GC. Instead, T strips counter cells by timestamp only, so that
  counters can be updated again after the TRUNCATE, as with the physical
  cut: increments with timestamps above T count from zero. Counter
  tombstones at or below T, written by earlier DELETEs, are stripped too,
  so a counter deleted before the TRUNCATE can be used again after it, as
  with the physical cut, which removes them.

  Counters are the one case where *merging before stripping* is not
  harmless. Regular cells merge by timestamp, so a write above T simply
  replaces an older cell. Counter cells merge shard by shard, keeping the
  shard with the higher logical clock, and the merged cell takes the newer
  timestamp. If a post-truncate increment (a fresh shard with a low clock,
  timestamp above T) is merged with the pre-truncate shard of the same
  counter (higher clock, timestamp at or below T) before the latter is
  stripped, the merge keeps the old shard, the merged cell's timestamp is
  above T so it is never stripped, and the pre-truncate count is resurrected
  while the increment is lost.

  Reads, compaction and repair strip every source before merging, so they
  are safe. The only merge that precedes stripping is applying a write into
  an existing memtable, which the atomic memtable switch at T installation
  (see Memtables) closes for every replica that has installed T. The
  remaining edge cases cannot be solved reliably by an eventually-consistent
  TRUNCATE:

  - A counter update concurrent with the TRUNCATE, reaching a replica
    before that replica has installed T (i.e. before the statement's
    barrier completes), may be merged with pre-truncate shards in that
    replica's memtable and resurrect them.
  - A replica that missed the TRUNCATE (down, or excluded from the barrier)
    and receives counter updates before it catches up on group0 is exposed
    the same way.

  Solving these reliably requires the TRUNCATE to be ordered with counter
  updates, i.e. a strongly-consistent TRUNCATE together with
  strongly-consistent counter updates; strongly-consistent tables do not
  support counters today. The outcome of counter updates concurrent with
  today's physical cut is undefined as well (they are kept or dropped
  depending on which shard they race with), so this is not a regression,
  but it is documented: counter updates should be quiesced while a counter
  table is truncated.

## Backup and restore

Snapshots taken while T is set contain covered-but-not-yet-purged data, so T
is part of the table's logical state and must be captured and restored with
it (not implemented yet):

- Snapshot sealing already records per-tablet `repair_time`/`repaired_at` in
  the manifest and `system_distributed.snapshot_tablets`; T
  (timestamp + deletion_time) is recorded next to them (per table).
- The tablet restore flow (`restore_tablets` global request) applies
  `truncated_at = max(current, snapshot T)` as part of restoring the table,
  so restored sstables' covered data stays dead.
- Restoring a *pre-truncate* snapshot in order to undo a mistaken TRUNCATE
  requires T to be gone (post-GC) — or, before GC, writing the data back with
  new timestamps. This matches partition-tombstone recovery semantics
  exactly.

### Restore and TRUNCATE

The documented restore procedures (`restore.rst`, prerequisites) truncate an
existing table before loading the snapshot, "so that the existing data does
not overwrite the restored data". The native tablet restore
(`restore_tablets`) does not truncate; it attaches the snapshot's sstables on
top of the table's current content.

The timestamped TRUNCATE cannot serve this purpose. Restore means "replace
the table's content with the snapshot's", which is not expressible by a
timestamp-based tombstone: data written after the backup time B has
timestamps above B, so deleting it requires `T >= now`, which also shadows
all restored data (timestamps below B); and `T = B` deletes the pre-backup
data that the snapshot restores while keeping the post-backup data that
restore is meant to discard. Today's physical cut works because it orders by
arrival on the node (`max_data_age`), not by write timestamp. The same
problem arises when restoring into a table that has T set and not yet GC'd,
e.g. to undo a mistaken TRUNCATE: restored data at or below T stays hidden
until T's GC.

Restore therefore uses the timestamped TRUNCATE followed by T's GC, made
prompt by running major compaction and repair, before loading the snapshot.
The restore documentation is updated accordingly, for tablet tables with the
cluster feature enabled:

1. Stop writes to the table. Following the general recommendation for
   restoring tables with views and indexes, drop the table's materialized
   views and secondary indexes now and recreate them only after step 6, so
   they are built from the restored base table rather than updated inline
   while it is loaded. This also leaves only the base table's T to wait
   for in steps 3-5.
2. `TRUNCATE ks.t`. If views or indexes were kept, this also truncates
   them.
3. Run major compaction on **every** node, for the base table and its views
   and index tables (e.g. `nodetool compact ks`, which covers all tables of
   the keyspace). This makes each tablet clean and reports it (condition 2).
4. Satisfy condition 1 of T's GC for the table and its views and indexes:
   - in `repair` mode (the default for tablet tables), run a repair after
     the TRUNCATE (`nodetool cluster repair ks`); it is fast, since the
     tables are nearly empty after step 3;
   - in `timeout` mode, either wait for `gc_grace_seconds`, or temporarily
     switch the tables to `tombstone_gc = {'mode': 'immediate'}` (safe while
     writes are stopped) and switch back after the restore.
5. Verify that T has been cleared for the base table, and for each of its
   views and index tables if they were kept (see Verifying T's GC). All of
   them are needed: if a view's T is still set, view updates generated from
   the restored base data are stripped in the view, and the restored view
   rows stay hidden.
6. Load the snapshot, recreate the views and indexes if they were dropped,
   then resume writes.

A hint or batchlog entry queued before the TRUNCATE and replayed after T is
cleared can still land after the restore. Today's physical cut has the same
exposure (hints are not filtered by the truncation time), so this is not a
regression.

### Alternative: drop and recreate the table

A simpler way to clear a table before a restore, especially for
administrators and management tools (e.g. ScyllaDB Manager), is to drop the
table and recreate it, typically with the same table id
(`CREATE TABLE ... WITH id = <uuid>`):

1. Stop writes to the table.
2. Drop the table's materialized views and secondary indexes, then
   `DROP TABLE ks.t` (`DROP TABLE` is refused while views depend on the
   table; its CDC log table is dropped along with it).
3. Recreate the base table from the backed-up schema, with the same table
   id.
4. Load the snapshot into the base table.
5. Recreate the indexes and views. They are built from the restored base
   table by view building, which is more efficient than generating view
   updates inline while the base table's data is loaded.
6. Resume writes.

This follows the general recommendation for restoring tables with views and
indexes — create them only after the base table is restored — so dropping
them together with the base table adds no extra step.

No major compaction, repair or verification is needed: dropping the table
removes its `system.tablets` partition, and T with it, and the recreated
table starts without T. The procedure works the same with or without the
cluster feature, and for vnode keyspaces. The data itself is removed by
DROP TABLE on each node when it applies the schema change (with the usual
auto-snapshot); it does not require every node to be alive.

Reusing the table id has a hazard that a new id does not: writes made before
the DROP that are still persisted or in flight can land in the recreated
table, since the mechanisms that discard writes to a dropped table rely on
the table no longer existing:

- **Commitlog.** DROP TABLE removes the table's truncation records
  (`database::cleanup_drop_table_on_all_shards`), and commitlog replay skips
  a table's entries only by those records, or drops them when the table is
  unknown. A segment is recycled only once every table with entries in it is
  flushed, so if a node restarts while a segment still holds pre-drop
  entries, they are replayed into the recreated table. This is tracked by
  SCYLLADB-5245, a regression from bc20861afb (#25683), and is fixed
  independently.
- **Batchlog.** Batchlog entries are not removed by DROP TABLE. Replay
  (`batchlog_manager.cc`) discards a mutation only if its table no longer
  exists (`try_find_table`) or if the batch was written at or before the
  table's truncation time, which is `min()` for a newly created table, so
  pre-drop batches are replayed into the recreated table. The fix proposed
  in SCYLLADB-5245 (a truncation record with `truncated_at = now` when a
  table is created with an explicit id) also covers the batchlog and Paxos
  learn.
- **Hints**, including view hints (`storage_proxy` keeps a separate
  `db::hints::manager` for them, with the same sender), are not removed by
  DROP TABLE either. The hint sender discards a hint only if its table does
  not exist when the hint is sent (`hint_sender.cc`, `no_such_column_family`),
  and hints are never checked against a table's truncation time, so pre-drop
  hints are sent to the recreated table. The SCYLLADB-5245 fix does not cover
  them; they need their own fix, tracked by SCYLLADB-5251, independent of
  this design.

With a new table id, all of these are discarded as writes to an unknown
table.

### Verifying T's GC

T is recorded in each table's own `system.tablets` partition, so whether it
has been cleared can be read from there: `truncated_at_timestamp` is null
once T is GC'd. The partition key is the table id, so a user first looks it
up in `system_schema.tables` (base and index backing tables) or
`system_schema.views` (materialized views), e.g.:

```
SELECT id FROM system_schema.tables WHERE keyspace_name = 'ks' AND table_name = 't';
SELECT truncated_at_timestamp FROM system.tablets WHERE table_id = <id>;
```

The `keyspace_name`/`table_name` static columns of `system.tablets` cannot
be used instead, since a co-located view's partition holds only
`base_table`. Because this is cumbersome and must be repeated for every view
and index, a user-facing status command is also provided (see Open
questions), reporting for a table and each of its views and indexes: the
current T, whether condition 1 holds, and how many tablets are confirmed
clean.

## Tablet lifecycle interactions

- **Migration / split / merge**: T is per-table static state, unaffected by
  all three. File-based streaming copies raw sstables; the destination
  strips them against T from its own tablet map. Only the GC-progress
  per-tablet column needs split/merge/migration rules (copy / min / reset).
- **Mixed clusters**: a new `gms` cluster feature,
  `EVENTUALLY_CONSISTENT_TRUNCATE`, gates `USING TIMESTAMP`, the
  `system.tablets` columns, the new TRUNCATE path, and stripping. Until the
  feature is enabled, TRUNCATE on tablet tables uses the existing topology
  operation (the physical cut); once it is enabled, CQL TRUNCATE on
  eventually-consistent tablet tables always takes the new path. The tests
  of the physical cut suppress the feature (the `suppress_features` error
  injection, see `with_physical_truncate()` in `test/cluster/util.py`). Nodes without the feature must never serve reads for a table
  with T set — guaranteed by enabling the path only once the feature is
  cluster-enabled, after which no old node can join.

## Implementation phases (sketch)

1. Tablet map plumbing: cluster feature, `system.tablets` columns,
   `tablet_metadata` map, mutation builder, loader (including the co-located
   branch), change hint.
2. `replica::table` installs T on effective-replication-map updates:
   memtable switch, cache invalidation and memtable flush when T changes;
   `update_invalidating` for memtables older than T.
3. Read-path stripping: `schema::truncation_tombstone()`, sampling T in the
   permit, the stripping reader, stripping per sstable (which covers the
   cache's population source, streaming/repair, compaction, view building
   and load-and-stream), memtable and cache readers; dropping and recreating
   saved readers on resume after T changed. Optimization: skip reading
   sstables entirely below T.
4. Write-path stripping before the commitlog, covering all apply variants.
5. TRUNCATE `USING TIMESTAMP` grammar and the group0 write path, including
   the views/indexes fan-out and the barrier.
6. Whole-sstable expiry under T in compaction (compaction output is already
   stripped by phase 3).
7. GC protocol: tablet cleanliness check, major compaction reporting it,
   coordinator confirmations and background sweep, clearing T; staging
   sstable handling; the T's GC status command.
8. Backup/restore manifest integration.
9. User-facing documentation of the behavior changes (TRUNCATE and the CDC
   note) and of the updated restore procedure; `mutation_dump`/tools
   exposure.

Each phase is independently testable; phases 1–5 already deliver a correct
(if never-GC'd) eventually-consistent TRUNCATE, with 6–7 completing the
tombstone equivalence.

Phases 1–7 are implemented, with the limitations listed in GC of T, and so
is the user-facing documentation of phase 9 (`docs/cql/ddl.rst`, the restore
procedure); phase 8 and the `mutation_dump`/tools exposure are not.

## Open questions

1. The form of the status command for T's GC (see Verifying T's GC): a
   nodetool command backed by a REST API, a virtual table, or both.
2. The drop-and-recreate restore procedure depends on independent fixes for
   writes from before the DROP landing in a table recreated with the same
   id: SCYLLADB-5245 (commitlog, and batchlog and Paxos learn via its
   proposed fix) and SCYLLADB-5251 (hints, including view hints).
