# DELETE ALL DATA: an eventually-consistent, table-wide deletion for tablet tables

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
[SCYLLADB-6494](https://scylladb.atlassian.net/browse/SCYLLADB-6494). This
design answers them, for tablet-based tables, with a separate statement in
the DELETE family, `DELETE ALL DATA`, rather than by changing TRUNCATE. Its
core is one authoritative tombstone timestamp applied on all nodes, sstables
with
`max_timestamp <= T` deleted whole, sstables with `min_timestamp > T` left
alone, the rest rewritten, and the tombstone purged after gc_grace or
repair. Propagating T through the schema was considered as well; here it
lives in the tablet map. There is no auto-snapshot before T becomes visible
(see Execution flow).

[#16411](https://github.com/scylladb/scylladb/issues/16411) ("Make TRUNCATE
TABLE safe with tablets") was resolved by making TRUNCATE with tablets a
topology operation, serialized against migration and repair. It does not
address the lack of a write timestamp; the discussion there also considered
token-range tombstones, see Alternatives below.

`DELETE ALL DATA` is an **eventually-consistent** deletion of the whole
content of a tablet-based table. It carries a write timestamp, provided by
the client or else by the server:

```
DELETE ALL DATA FROM <keyspace>.<table> [USING TIMESTAMP <t> [AND TIMEOUT <d>]]
```

Its semantics are **those of a single-partition `DELETE`, applied to every
partition of the table**: a tombstone `T = {timestamp, deletion_time}` that
shadows every cell with `write_timestamp <= T.timestamp`, applied by
timestamp comparison on read and on compaction, propagated to all replicas,
and eventually garbage-collected under the same rules as partition tombstones
(gc_grace / repair-based tombstone GC).

`TRUNCATE` itself is left as it is: a physical cut, which for tablet tables
is a topology operation that needs all the replicas alive. Giving TRUNCATE
the timestamped semantics on tablet tables was considered and rejected.
Existing tools, runbooks and ScyllaDB Manager truncate a table and then
restore older data into it, and a timestamped TRUNCATE silently breaks them:
the restored data is older than T and stays hidden (see Restore and
TRUNCATE). The one change to TRUNCATE is that, on a tablet table, it also
clears T (see TRUNCATE clears T).

In the code, T is called the *truncation tombstone* (`get_truncated_at()`,
`truncate_with_timestamp()`, `strip_truncated()`, the
`TRUNCATION_TOMBSTONE_CLEAN` verb, etc.). The name is historical, from when
T was designed as the tombstone of a timestamped TRUNCATE, and this document
uses the code's names as they are. T is unrelated to the truncation records
which TRUNCATE keeps in `system.truncated`.

The deletion is cheap in practice, not only correct. Its timestamp is
typically larger than all or most write timestamps in the table, so nearly
every sstable is *fully* shadowed by T and compaction drops it whole without
reading it, the same way TWCS drops fully-expired sstables, including any
tombstones it holds, with no tombstone GC checks (see Compaction below). Disk
space is reclaimed almost as promptly as with TRUNCATE's physical cut.

The GC requirement is essential, not incidental: a table-wide deletion is
sometimes issued by mistake, and one recovery path is to re-add the original
data with its original write timestamps (e.g. `nodetool refresh
--load-and-stream` replaying the original mutations). That only works if the
table-wide tombstone is eventually purged, exactly as a partition tombstone
would be.

### Scope

- **Tablet-based, eventually-consistent tables** only. For any other table
  the statement fails with an invalid-request error which says to use
  TRUNCATE instead:
  - Tables of vnode keyspaces.
  - Strongly-consistent tables. Their TRUNCATE is ordered in each tablet's
    Raft log with a leader-assigned `truncated_at` write timestamp (#31576,
    #31772). With linearizable writes, a physical cut ordered in the log is
    already correct.
  - Tables using the logstor storage engine, for now, since their read and
    write paths do not strip covered data yet.
- The statement is rejected until the
  `EVENTUALLY_CONSISTENT_DELETE_ALL_DATA` cluster feature is enabled.
- TRUNCATE is not changed, for any kind of table and with or without the
  cluster feature, except that it clears T. In particular, TRUNCATE with
  tablets remains a topology operation that makes a physical cut.

## Design summary

The design separates the *authoritative fact* from its *observation*:

- **The fact** — "table X data is deleted as of timestamp T" — is a
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
out that a token-range tombstone makes the table-wide deletion a
*data-plane* operation: the coordinator writes the tombstone for each
tablet's range at a consistency level, like an INSERT, so a deletion at
`LOCAL_QUORUM` succeeds when only the local datacenter is available. It also
need not wait for anything, including memtable flushes. The tablet-map
design shares the latter property (no flush or other replica-side work is on
the statement's critical path), but not the former: writing T requires a
group0 majority, so DELETE ALL DATA is a control-plane operation that cannot
complete while group0 has no quorum, e.g. when only one datacenter of
several is reachable.

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
the statement depends on group0 availability rather than on a per-tablet
consistency level. This design does not pursue token-range tombstones.

### Relation to existing mechanisms

- TRUNCATE with tablets (`TRUNCATE_AS_TOPOLOGY_OPERATION`) coordinates a
  *physical* cut through the topology state machine: barriers, sessions,
  serialization against migrations, and a hard requirement that every
  replica is alive. DELETE ALL DATA needs none of that for correctness: the
  timestamp decides every conflict, so a plain group0 mutation suffices,
  and a replica that is down simply observes T when it comes back and
  catches up on group0. It therefore does not require every replica to be
  alive.
- `table::truncate_tablet_locally` (#31576) is the local half of
  strongly-consistent TRUNCATE (#31772): a physical, per-tablet cut ordered
  by the tablet's Raft log, with a leader-assigned `truncated_at` timestamp.
  It is prior art for per-tablet coordination and for carrying a write
  timestamp with a table-wide deletion, but it is not reusable as the
  mechanism here.
- The per-tablet `repair_time` → `tombstone_gc_state` interval-map flow
  (`replica/tablets.cc`, `tombstone_gc.cc::flush_pending_repair_time_update`)
  is the proven pattern for projecting tablet-map state into per-shard lookup
  structures consulted on read/compaction hot paths; `sstables_repaired_at`
  is the proven pattern for compaction consulting per-tablet metadata.

## Semantics

Let `T = {timestamp, deletion_time}` where `timestamp` follows the same rules
as DML: `USING TIMESTAMP` if given, else the client-side default timestamp
sent at the protocol level if any, else the coordinator's clock; and
`deletion_time` is the coordinator's `gc_clock` time when it executes the
statement.

- A cell (or row marker, or narrower tombstone) with
  `write_timestamp <= T.timestamp` is shadowed, table-wide.
- A write with `write_timestamp > T.timestamp` survives, regardless of
  whether it was applied before or after the statement executed. There is no
  "reordered with truncate" error and no silent drop: commitlog replay,
  batchlog replay, hint replay and Paxos learn need no special-casing — a
  replayed old-timestamped mutation is simply shadowed by T, exactly as it
  would be by a partition tombstone.
- Multiple statements compose like tombstones: the table record keeps
  `max(T_old, T_new)` under `tombstone::apply` ordering (timestamp, then
  deletion_time). A DELETE ALL DATA with a timestamp lower than the current
  T is absorbed, same as applying an older partition tombstone over a newer
  one.
- `USING TIMEOUT` can be given with `USING TIMESTAMP` or alone; the default
  timeout is that of TRUNCATE and of the DROP statements
  (`truncate_request_timeout_in_ms`, 60 seconds), not the write timeout
  (2 seconds): the statement commits a group0 command and waits for a read
  barrier on the live nodes, like a schema change, and group0 operations
  have a 60 seconds timeout of their own. `USING TTL` is a syntax error.
- The statement requires the `MODIFY` permission on the table, like DELETE.
- It is rejected on a materialized view: the data is deleted from the base
  table, which deletes it from the table's views and indexes too (see Views
  below).
- It is supported only for the tables listed in Scope, once the cluster
  feature is enabled.

### Differences from DELETE

The statement deletes what a DELETE of every partition of the table with
the same timestamp would delete, but it is not executed like a DELETE:

- **It goes through group0**, not through the replicas' write path. It
  needs a group0 majority and has no consistency level; the consistency
  level of the request is not used. It does not need the table's replicas
  to be alive: a replica which is down observes T when it comes back (see
  Restarting nodes).
- **No BATCH, no IF.** It cannot be part of a BATCH and takes no IF
  condition; the grammar accepts neither.
- **No CDC.** It writes no CDC log entry, and T is not applied to the
  table's CDC log table (see CDC below).
- **Counters can be used again.** A counter deleted by a DELETE cannot be
  used again. After DELETE ALL DATA counters can be updated again, and count
  from zero, which a real tombstone would not allow (see Counters below).
- **Nothing is written to the data path.** There is no tombstone in the
  commitlog, memtables or sstables; T lives in the tablet metadata until it
  is garbage-collected (see Design summary).

### DELETE ALL DATA compared with TRUNCATE

Both statements remove all the data of a table. They differ in what "all"
means, and in what they need and leave behind:

- **What is deleted.** DELETE ALL DATA deletes by write timestamp. TRUNCATE
  deletes by arrival: whatever each replica shard holds at its cut point,
  regardless of timestamps.
  - Data written with a timestamp **above** T (e.g. a client clock ahead of
    the coordinator's, or an explicit future `USING TIMESTAMP`) survives
    DELETE ALL DATA. TRUNCATE removes it. In particular, most drivers send a
    client-side timestamp with every request by default, which T then
    follows: if the deleting client's clock is behind the clocks of the
    clients or nodes which wrote the data, data written shortly before the
    DELETE ALL DATA survives it.
  - Data written **after** DELETE ALL DATA with a timestamp at or below T is
    dropped, until T is garbage-collected. After TRUNCATE it survives.
- **Concurrent writes.** With DELETE ALL DATA they are resolved by
  timestamp, the same way on all replicas. With TRUNCATE they are kept or
  dropped arbitrarily, per replica shard (see Motivation).
- **Availability.** DELETE ALL DATA needs a group0 majority. TRUNCATE of a
  tablet table needs that and all the table's replicas alive.
- **Disk space.** Nothing is removed from disk when DELETE ALL DATA returns.
  Disk space is reclaimed by compaction, mostly by dropping fully-shadowed
  sstables whole. TRUNCATE deletes the sstables before it returns.
- **Snapshot.** TRUNCATE takes an auto-snapshot when `auto_snapshot` is
  enabled. DELETE ALL DATA takes none (see Execution flow).
- **Counters.** Under DELETE ALL DATA counter state is shadowed by T rather
  than physically reset, and counter updates concurrent with the statement
  can resurrect pre-deletion counts; see Counters below. As with TRUNCATE,
  and unlike a DELETE, counters can be updated again afterwards.
- **Older data afterwards.** After TRUNCATE the table accepts data with any
  timestamp, which is what restoring a snapshot into it needs. After DELETE
  ALL DATA, data at or below T written or loaded into the table is deleted
  as well, until T is garbage-collected or cleared by TRUNCATE.

Which to use:

- DELETE ALL DATA for an application's deletion of a table's content, in
  particular when the table is written to concurrently, when the outcome
  must be the same on all replicas, or when a replica is down.
- TRUNCATE for administrative work on a table which is not written to:
  emptying a table before restoring a snapshot into it (see Restore and
  TRUNCATE), reclaiming its disk space at once, or making it accept data
  older than T.

The user-facing documentation of both statements states these differences.

### TRUNCATE clears T

When a TRUNCATE of a tablet table succeeds, it also clears T of the table
and of its views and indexes. The topology coordinator
(`handle_truncate_table()`) adds the removal of T (`del_truncated_at()`) to
the group0 command which clears the operation's session, and only when the
truncate was sent to the tables' replicas and succeeded on all of them. A
TRUNCATE which fails leaves T as it is.

Clearing T at this point is safe for the reason T's GC is (see GC of T,
condition 2), with the truncate standing in for the cleanliness check:
TRUNCATE removed the tables' data from all their replicas, including the
data covered by T, and a covered write which reached a replica while the
TRUNCATE ran was stripped, since T was still set. So no replica holds
covered data. Covered writes which are still in commitlog segments, from
before T arrived, are not replayed, by TRUNCATE's own truncation record.

It follows that:

- "TRUNCATE, then restore a snapshot into the table" works as it always
  did, also for a table which had T set by an earlier DELETE ALL DATA (see
  Restore and TRUNCATE).
- TRUNCATE is the way to make a table accept data older than T without
  waiting for T's GC.

The trade-off is that the grace period of T's GC (condition 1) is not waited
for. After TRUNCATE clears T, a late write covered by the old T, e.g. a
hint queued before the DELETE ALL DATA, is no longer stripped when it
arrives, and stays in the table. This is how late writes behave after any
TRUNCATE today: hints are not checked against the truncation time, while
batchlog replay discards batches written before it. TRUNCATE is thus as
safe against late writes as it is without T; what ends is the protection
that T gave.

TRUNCATE clears only the T which the table, and each of its views, had
when the TRUNCATE started, after the barrier which made every node observe
it. A T set or raised by a DELETE ALL DATA which commits while the TRUNCATE
is running is kept: data it covers may have been written after the replicas
were truncated and before they observed it, so they may hold covered data.

The T of a view is also kept if the view has replicas on nodes which hold no
replica of the base table: TRUNCATE is sent to the base table's replica
nodes, which truncate the table and its views there, so such view replicas
are not truncated. This happens when the view is not co-located with its
base table.

## The fact: tablet map record

New static (per-table) columns in `system.tablets`, next to `resize_type`
etc.:

- `data_deleted_at_timestamp` (bigint, api::timestamp_type)
- `data_deleted_at_deletion_time` (timestamp, gc_clock)
- GC protocol state (see below).

These are set by DELETE ALL DATA, and by the restore of a snapshot which
recorded T (see Backup and restore). They are cleared by T's GC and by
TRUNCATE. They are unrelated to the truncation records in
`system.truncated` and to the `truncated_at` time cached on
`replica::table`, which TRUNCATE uses for commitlog replay filtering; the
code must carry a comment saying so.

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

1. The `deleteAllDataStatement` rule in `cql3/Cql.g` parses
   `DELETE ALL DATA FROM <table>`, followed by an optional
   `usingTimestampTimeoutClause`, which accepts `TIMESTAMP` and `TIMEOUT`
   but not `TTL`. `DATA` is an unreserved keyword, like `ALL`, so columns
   named `all` or `data` keep working, and `DELETE all, data FROM t WHERE
   ...` still deletes these two columns: the comma tells the two statements
   apart.
2. `delete_all_data_statement`
   (`cql3/statements/delete_all_data_statement.cc`) checks the `MODIFY`
   permission, and, when executed, rejects with an invalid-request error:
   a materialized view; any table while the
   `EVENTUALLY_CONSISTENT_DELETE_ALL_DATA` cluster feature is not enabled;
   and a table which is not in a tablet keyspace, is strongly consistent, or
   uses the logstor storage engine (see Scope).
3. The statement computes T — `USING TIMESTAMP` if given, else the
   client-side default timestamp sent at the protocol level, else the
   coordinator's clock; the deletion time is the coordinator's `gc_clock` —
   and calls `storage_service::truncate_with_timestamp()`, which, on shard 0,
   writes `truncated_at = max(current, T)` for the table **and for every
   materialized view / secondary index of the table** in one group0 command
   (see Views below), retrying on a group0 conflict. Paxos state tables and
   the CDC log table are excluded.
4. It then waits until the live nodes apply the group0 command, with
   `raft_group0_client::send_group0_read_barrier_to_live_members()`, so that
   a subsequent read through *any* coordinator observes the deletion. Dead
   nodes are skipped without harming correctness: a node that missed the
   command catches up on group0 when it restarts, before it serves (see
   Restarting nodes below), so the barrier is only about prompt visibility
   of the acknowledgment, never about the deletion taking effect.
5. Steps 3 and 4 run under the statement's timeout (`USING TIMEOUT`, else
   the truncate timeout). When it expires the statement fails with a server
   error; the deletion may still complete in the background.

No flush, no compaction stop, no replica RPC fan-out, no all-replicas-alive
requirement.

There is no auto-snapshot on this path. Unlike TRUNCATE, DELETE ALL DATA
does not remove anything at execution time, and recovery from a mistake has
a first-class path: reinsert with newer timestamps, or TRUNCATE the table
and restore the original data into it, or, after T's GC, load-and-stream the
original data. Applications that want a snapshot of the table as it was can
take one before issuing the statement. Two orthogonal future extensions
could make that self-service from CQL (out of scope for the initial
implementation):

- exposing snapshot management over CQL (today it requires nodetool or the
  REST API), so an application can take and manage snapshots itself;
- a `WITH SNAPSHOT[=<tag>]` clause on DELETE ALL DATA — and equally on
  TRUNCATE, DROP KEYSPACE and DROP TABLE — taking a snapshot as part of the
  statement.

### Restarting nodes

A node which was down when the statement ran does not have T until it applies
the group0 command it missed, and until then it would serve the covered
data it holds: to its own clients, and to other coordinators reading from
it, since T is not in the data path and nothing shadows its rows when
replica results are merged. A counter update it leads could also read a
covered count and write it back with a new timestamp.

Starting up does not by itself wait for group0. A restarting node therefore
takes a group0 read barrier before it joins the cluster (`main.cc`, before
`join_cluster()`): at that point messaging is listening, which is all group0
needs, but the node does not gossip yet, so other nodes see it as down and
send it no requests, and it serves no clients. With a leader in place this
takes tens of milliseconds.

The wait is bounded, since a node must be able to start and serve the data
it has when group0 is unavailable: it gives up if no group0 leader is found
within 5 seconds, or if the barrier does not complete within 30 seconds, and
logs a warning. No leader is found when group0 has no majority, and also
when the whole cluster starts at once: no leader is elected before the
nodes gossip, and no command was applied while they were all down. The
exposure that remains is a node which starts while group0 has no majority
after missing a DELETE ALL DATA that was committed before the majority was
lost.

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
`partition_start` with no tombstone). The sstable writer, however, is fed
directly on memtable flush, with no compactor in between, and would write
such a pair as an empty partition; see Memtables below.

### Read path

Stripping is applied where data leaves each *store* — each sstable and each
memtable — rather than on the merged output of a reader. Besides being
sound per source, this is required for counters: merging counter cells keeps
the shard with the higher logical clock, so merging a covered shard with a
fresh, post-deletion one before stripping would keep the covered shard (see
Counters below). The stripping adapter is
`make_truncation_stripping_reader()` (`readers/truncation_stripping.hh`).
It strips row and range tombstones by their timestamp regardless of their
deletion time, and strips the regular and the shadowable row tombstones
separately, so that T is never merged into what is left of them. It drops
rows that end up empty and range tombstone changes that end up redundant.

T acts on a row as a **regular** row tombstone, not a shadowable one, also
in views: a base table deletion always reaches the view as a regular
tombstone, and a shadowable one is used only when a view row is replaced
because a base column in the view's key changed. The cells of a row are
therefore stripped together with the row's own marker, as it is after
stripping: in a view whose key includes a base regular column, a row whose
marker is covered loses all its regular cells, including cells with
timestamps above T (`dead_marker_shadows_row()`), exactly as it would under
a row tombstone with T's timestamp. Stripping such a row's cells one by one
would keep them, and with them a view row whose base row is gone.

The reader follows the reader contract in both forwarding modes; in
particular `next_partition()` clears its end-of-stream state, so it can be
called after a forwarded range was exhausted.

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

A write is stripped against the T the table has when the write arrives, and
is written to the commitlog later: generating view updates, and the
commitlog itself, can make it wait. If a new T is installed in between, the
write's commitlog entry may hold data covered by the new T, while the
memtable it is then applied to, which was created after the new T, does not
(see below), and so does not keep the replica from being clean. Each apply
variant therefore compares the table's T, once the write is applied, with
the one it stripped with. If T is newer, the table is marked as possibly
holding covered data and its memtables are flushed
(`table::on_write_raced_with_truncation_tombstone()`), which records their
replay positions so that the entry is not replayed; the mark is cleared
when the flush completes. Until then T is not garbage-collected, and the
entry is stripped if it is replayed.

Stripping must also happen where data enters a memtable (`table::apply`),
not only before the commitlog. Commitlog replay inserts mutations through
`database::apply_in_memory`, bypassing `database::do_apply`, and a memtable
stamped with T (see Memtables) is relied upon to hold no data covered by T.
Without stripping at insertion, replay at boot would insert unstripped
pre-deletion writes into such a memtable. Stripping at insertion is done
synchronously with applying the mutation to the active memtable, against the
table's current T, which the active memtable is always stamped with (a new T
switches it, see Memtables), so that no T can be installed in between.

Stripping is implemented by `mutation_partition::strip_truncated()` (and the
`deletable_row` and `row` methods it uses, which the stripping reader uses
too), after `has_truncated()` found that the mutation holds covered data at
all. Mutations written after the statement normally hold none, so the common
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
- **Memory.** Since T typically covers nearly all of a table's data,
  nearly all of its cached data becomes garbage, and should be released
  promptly rather than waiting for LRU eviction.

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
   like a write, the deletion need not be visible to reads concurrent with
   it, and is visible to reads that start after the statement returns, since
   the barrier it waits for completes only once every live node has
   installed T. It is
   about applying T uniformly within a single read: stripping some of a
   read's sources but not others can expose a value that was never current,
   e.g. a cell written at t1 in the cache and overwritten at t2 in a
   memtable, with t1 < t2 <= T: stripping the memtable but not the cache
   returns the t1 value, which is neither the pre-deletion state (t2) nor
   the post-deletion one (nothing).
2. **Populate only stripped data.** The cache populates from
   `table::sstables_as_snapshot_source`, which captures the sstable set when
   the snapshot is taken; it captures T together with it, and its readers
   are stripped. The external updater in step 1 makes the new snapshot take
   effect atomically with the invalidation.
3. **Merge only stripped memtables.** On flush, a memtable is merged into
   the cache (`row_cache::update`) by moving its partitions directly, not
   through a reader, so a memtable that may hold covered data must not be
   merged as is (see Memtables below).

The cost is one full-table invalidation per new T, using existing
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
  reaches disk. The flush reader also drops partitions that stripping left
  with nothing (`drop_empty_partitions::yes`): nothing compacts the stream
  between the memtable and the sstable writer, so they would be written as
  empty partitions, into an sstable whose statistics have no timestamps
  (`min_timestamp` at its minimum), which the cleanliness check would count
  as possibly holding covered data until it is compacted away. For the same
  reason, a memtable left with nothing at all writes no sstable.
  No sstable then records the memtable's replay position, which is what
  tells the commitlog replay to skip the memtable's entries: replayed after
  T's GC they would bring the covered data back. The flush therefore saves
  a commitlog cleanup record (`system.commitlog_cleanups`, as tablet cleanup
  does) for the compaction group's token range, up to the group's flushed
  replay position, before the memtable is removed; like that position, it
  waits for the older memtables of the group to be flushed first.
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
further write is applied to them. Pre-deletion and post-deletion data then
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
- It runs under the table's gate, which is taken *before* any memtable is
  switched: if the table is already stopping, nothing is switched, so no
  switched-out memtable is left without a flush.
- It does not depend on the cache invalidation: if that fails, the error is
  logged, the cache's output keeps being stripped, and the switched-out
  memtables are flushed all the same.
- A switched-out memtable can be flushed *after* a younger one, since the
  younger one may obtain its flush permit first. The compaction group's
  flushed replay-position watermark (`_highest_flushed_rp`, which lets
  commitlog-based tombstone GC records be discarded) must not move past
  data that is still only in a memtable, so a completed flush advances it
  only when no older non-empty memtable remains unflushed; until then the
  position is kept aside (`_highest_completed_flush_rp`) and applied by the
  flush that removes the last such memtable.

Flushing them releases the memory held by covered
data immediately rather than whenever the memtable would next be flushed,
and it retires the commitlog segments holding pre-deletion writes, which
shortens commitlog replay. Its cost is close to zero: covered data is
stripped by the flush reader and never written, so only data written with
timestamps above T is written out, which right after the statement is
typically little or nothing. The same flush is a precondition of T's GC
anyway (see GC of T).

### Staging sstables

Staging sstables hold base table data received by repair, streaming or
load-and-stream whose view updates have not been generated yet. They are
not part of the compaction set, so neither regular nor major compaction
cleans them, and they must be handled when T is installed. When a replica
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
  `get_fully_expired_sstables`). Since T's timestamp is typically larger
  than all or most write timestamps, this is the common case, and it
  reclaims disk space almost as promptly as TRUNCATE's physical cut. Note
  the comparison is on *data timestamps*, never `max_data_age`.
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
  resurrected by the dropped tombstones' absence.

  Dropping the sstable does garbage-collect its tombstones early, before
  their own grace period or repair, and before the propagation delay. That
  protection is not given up but handed over to T, which shadows everything
  they shadow, and T is removed only under the same rule they would have
  been subject to (see GC of T): no replica holds covered data, and the
  table's tombstone GC condition holds for `T.deletion_time` — in `repair`
  mode a repair after it, preceded by a hints and batchlog flush, minus the
  propagation delay. The dropped tombstones were written before T was
  installed on the replica (a tombstone covered by T that arrives later is
  stripped on write and never reaches an sstable), so their deletion times
  are not later than `T.deletion_time`, except for those written while T
  was on its way to the replica, and T's condition is at least as strict
  as theirs. The propagation delay is thus accounted for once, in T's GC,
  rather than per dropped sstable.
- T is **never** written into compaction output. The tablet map record
  remains its single representation until its GC completes.

## GC of T

Besides the GC described here, T is cleared by a successful TRUNCATE of the
table (see TRUNCATE clears T).

T may be removed from the tablet map when both hold:

1. **Grace period** (same rule as any tombstone):
   `T.deletion_time < gc_before` for the whole table, per the table's
   `tombstone_gc` mode — `timeout`: gc_grace elapsed; `repair`: every tablet's
   `repair_time >= T.deletion_time` (the per-tablet repair times are already
   in the tablet map); `immediate`/`disabled` as usual. Note what this
   condition protects against: *not* T reaching replicas (group0 guarantees
   that — a restarting replica catches up on group0 before it serves, see
   Restarting nodes),
   but late-arriving writes with timestamps `<= T.timestamp` that were in
   flight when the deletion happened — hints, batchlog entries, pending view
   updates — the same role gc_grace plays for partition tombstones. Such a
   write arriving while T is set is stripped; arriving after T's GC it
   resurrects, same as after a partition tombstone's GC.
2. **No covered data remains** on any replica of any tablet: dropping T while
   some sstable, memtable or cache entry still holds cells with
   `ts <= T.timestamp` would resurrect them. This is the table-wide analogue
   of the local `max_purgeable` check, and it must be a positive,
   per-replica confirmation.

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

- every sstable of the tablet — in the main, maintenance and staging sets,
  and those already compacted but not deleted yet, which a restart would
  load again — has `min_timestamp > T.timestamp`, and
- every memtable of the tablet was created after the replica observed T
  (its content was stripped on write), and
- no write raced with the installation of T on the replica without having
  been flushed since (see Write path).

All the compaction groups of the table are checked, including those which
are being stopped: a group which is merged into another on tablet merge is
stopped before its sstables are moved over.

The cache needs no check once it is invalidated (see Row cache). The check is
implemented by `replica::table::may_hold_truncated_data()`.

**Implemented protocol.** The topology coordinator runs a background fiber
(`garbage_collect_truncation_tombstones()`, every 60 seconds). For each table
with T set whose tablets have no transitions in progress and for which
condition 1 holds, it asks every replica host of the table, over the
`truncation_tombstone_clean` RPC, whether it holds any covered data of the
table, checked as above on all shards. If none does, it clears T in a group0
command, unless, as rechecked under the group0 guard, T changed meanwhile
(another DELETE ALL DATA replaced it, or a TRUNCATE cleared it), the table
was dropped, or one of its tablets started a transition: the replies
describe the replica set that was asked, and a migration started since may
be copying covered data to a replica that was not. Such a table is retried
in the next round. Each table (base, views, indexes) has its own T and is
cleared independently.

- Condition 1 is evaluated from the tablet map: in `repair` mode, every
  tablet's `repair_time`, minus the propagation delay, must be after
  `T.deletion_time`. A repair flushes the hints and the batchlog on all
  nodes before it starts, which is what makes its time a safe bound.
  A tablet with a single replica needs no repair, but DELETE ALL DATA
  flushes none of the queues a covered write may be in — hints, the
  batchlog, pending view updates, writes in flight — so such a write can
  reach even a single replica after the statement. For a table with
  single-replica tablets the
  coordinator therefore stands in for the repair: once the propagation
  delay has passed since `T.deletion_time` and the replicas are clean, it
  flushes the hints (regular and view) and the batchlog on all nodes, using
  the flusher it uses for tablet repairs (`service::hints_batchlog_flusher`),
  and clears T only if the flush time, minus the propagation delay, is after
  `T.deletion_time`. If a node is down or the flush fails, T stays and the
  check is retried. A covered write delivered by the flush is stripped by
  the replica, since T is still set, so the replica stays clean. Pending
  view updates that are not hinted and writes in flight are covered by the
  propagation delay only. This is stricter than regular tombstones, which a
  table with RF=1 purges immediately in `repair` mode (SCYLLADB-6538). In
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
  all nodes (e.g. `nodetool compact`) makes T's GC complete promptly, once
  condition 1 holds. When a major compaction of a table with T set
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

After T's GC, loading pre-deletion sstables (restore, load-and-stream with
original timestamps) resurrects their data — by design, identically to
post-GC behavior of partition tombstones.

## Views, indexes, Paxos, CDC, counters

- **Materialized views and secondary indexes**: as with TRUNCATE, deleting
  all the data of the base table deletes that of its views and indexes. The
  same group0 command sets T (same timestamp) explicitly, per table_id, on
  every view and index backing table — one atomic group0 write covering
  base and all views; this is *more* atomic than TRUNCATE, which removes
  base and view sstables non-atomically (#17635). The statement is rejected
  on a view directly. Hazard note: because co-located tables share the
  base's `tablet_map` object, T must be keyed by table_id (see above) so
  that setting it on the base never implicitly applies it to other members
  of the co-location group. View-building racing the deletion is naturally
  correct: built view rows carry base-derived timestamps `<= T.timestamp`
  and are stripped.
  - View rows are stripped as under a regular row tombstone with T's
    timestamp, which is how a base deletion reaches a view; in particular a
    view row whose marker is covered is removed as a whole when the view's
    key includes a base regular column (see Read path). This is covered by
    `test_truncation_stripping_reader_view_with_base_regular_column_in_key`.
- **Paxos (LWT)**: T is **never** set on Paxos state tables, even though they
  are co-located with the base. Paxos state rows carry ballot-derived
  timestamps; a DELETE ALL DATA with a future `USING TIMESTAMP` would
  otherwise shadow every promise/accept until wall-clock passes T, silently
  breaking LWT on the table. TRUNCATE likewise never truncates the Paxos
  table — it relies on the `paxos_state::learn` `truncated_at` check instead.
  For DELETE ALL DATA no Paxos special-casing is needed at all: a
  decided mutation whose cells have timestamps `<= T.timestamp` is learned
  and applied normally, then stripped like any other write.
- **CDC**: DELETE ALL DATA does not touch CDC, as TRUNCATE does not. This
  is unlike a regular DELETE, which is logged.
  - Neither TRUNCATE implementation touches CDC. The tablets TRUNCATE
    (`truncate_table_on_all_shards`) truncates the base table and its views
    and indexes only; it neither truncates the CDC log table nor writes a
    log entry recording the truncate. This is documented in
    `docs/features/cdc/cdc-log-table.rst`, which recommends a manual
    procedure for keeping the two in sync (stop writes, consume the log,
    truncate both, resume). Strongly-consistent tables do not support CDC
    at all (`cdc/log.cc` rejects creating a CDC log in a strongly-consistent
    keyspace), and #31772 has no CDC handling. #31496 has none either.
  - **No CDC log record** is written for a DELETE ALL DATA. CDC log rows are
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
  - Log entries of other writes are not affected. A base write with a
    timestamp at or below T that arrives after the deletion is stripped on
    the base replicas, but its CDC log entry, generated on the coordinator,
    is still written — the same as for a write shadowed by a partition
    tombstone: CDC logs operations, not their effects. Preimages are
    computed from base reads, which are stripped, so they reflect the
    post-deletion state.
- **Counters**: counter cells covered by T are stripped like any other cell,
  so a counter reads as absent after the DELETE ALL DATA, and an increment
  after it starts from zero: the counter leader's read-before-write is
  stripped, so the leader writes a fresh shard. Unlike TRUNCATE, DELETE ALL
  DATA does not reset counter state physically; that is done by compaction,
  before T's GC completes.

  T is **not** a counter tombstone. A counter cannot be used again once
  deleted: any tombstone covers a counter cell, regardless of their
  timestamps (`atomic_cell::is_covered_by()`, see also
  `docs/features/counters.rst`). If T behaved exactly like a DELETE of
  every partition, a counter it covers could not be updated again, at least
  until T's GC. Instead, T strips counter cells by timestamp only, so that
  counters can be updated again after the DELETE ALL DATA, as they can after
  TRUNCATE: increments with timestamps above T count from zero. Counter
  tombstones at or below T, written by earlier DELETEs, are stripped too,
  so a counter deleted before the DELETE ALL DATA can be used again after
  it, as after TRUNCATE, which removes them.

  Counters are the one case where *merging before stripping* is not
  harmless. Regular cells merge by timestamp, so a write above T simply
  replaces an older cell. Counter cells merge shard by shard, keeping the
  shard with the higher logical clock, and the merged cell takes the newer
  timestamp. If a post-deletion increment (a fresh shard with a low clock,
  timestamp above T) is merged with the pre-deletion shard of the same
  counter (higher clock, timestamp at or below T) before the latter is
  stripped, the merge keeps the old shard, the merged cell's timestamp is
  above T so it is never stripped, and the pre-deletion count is resurrected
  while the increment is lost.

  Reads, compaction and repair strip every source before merging, so they
  are safe. The only merge that precedes stripping is applying a write into
  an existing memtable, which the atomic memtable switch at T installation
  (see Memtables) closes for every replica that has installed T. The
  remaining edge cases cannot be solved reliably by an eventually-consistent
  deletion:

  - A counter update concurrent with the DELETE ALL DATA, reaching a replica
    before that replica has installed T (i.e. before the statement's
    barrier completes), may be merged with pre-deletion shards in that
    replica's memtable and resurrect them.
  - A replica that missed the DELETE ALL DATA (down, or excluded from the
    barrier) and receives counter updates before it catches up on group0 is
    exposed the same way. A restarting node catches up before it joins the
    cluster (see Restarting nodes), so this is left only for a node which
    could not.

  Solving these reliably requires the deletion to be ordered with counter
  updates, i.e. a strongly-consistent operation together with
  strongly-consistent counter updates; strongly-consistent tables do not
  support counters today. The outcome of counter updates concurrent with
  TRUNCATE is undefined as well (they are kept or dropped depending on which
  shard they race with), so DELETE ALL DATA is no worse in this respect, but
  it is documented: counter updates should be quiesced while all the data of
  a counter table is deleted.

## Backup and restore

T deletes data logically, and lives in the tablet metadata, not in the
sstables. A snapshot taken while T is set may therefore hold covered data
that compaction did not purge yet, and nothing in its sstables says that
this data is deleted. T is part of the table's logical state: it must be
captured with the snapshot and applied again wherever the snapshot is
restored, or the covered data reappears.

**Capture.** When a snapshot of a tablet table is taken, each shard samples
T together with its list of sstables (`table::snapshot_sstables()`), and the
latest of them is recorded in the snapshot's `manifest.json`, as
`table.data_deleted_at_timestamp` and `table.data_deleted_at_deletion_time` (see
`docs/dev/object_storage.md`). They are omitted when T is not set. Sampling
T with the sstable list is what makes it right: if T was already
garbage-collected, no listed sstable holds covered data, and a DELETE ALL
DATA issued after the list was taken covers only data which the snapshot is
free to keep. The node-level backup (`/storage_service/backup`) uploads the
manifest as it is.

**Restore.** The tablet-aware restore (`/storage_service/tablets/restore`,
the `restore_tablets` topology request) reads T from the manifests it is
given, takes the latest, and stores it in the request
(`system.topology_requests`, `restore_data_deleted_at_timestamp` and
`restore_data_deleted_at_deletion_time`). The topology coordinator applies
`truncated_at = max(current, snapshot T)` to the destination table in the
same group0 command which puts the table's tablets in the `restore`
transition, i.e. before any sstable is attached. T's GC skips tables with
tablets in transition, so T cannot be cleared before the sstables it covers
are attached; afterwards the replicas hold covered data and are not clean
until it is compacted away. T keeps its original deletion time, like a
tombstone restored in an sstable. The restore is refused if the snapshot
has T and the cluster feature is not enabled, since nothing would keep the
covered data deleted.

Restoring a *pre-deletion* snapshot in order to undo a mistaken DELETE ALL
DATA requires T to be gone: cleared by a TRUNCATE of the table, which is
what the restore procedure starts with (see Restore and TRUNCATE), or
garbage-collected. The alternative is to write the data back with new
timestamps. Other than TRUNCATE clearing T, this matches partition-tombstone
recovery semantics exactly.

**Not implemented yet.**

- The cluster snapshot catalog (`system_distributed.snapshot_*`) and the
  datacenter-scope manifest that the cluster backup
  (`/storage_service/tablets/backup`) builds from it do not record T.
- Restore paths which do not read a manifest cannot apply T: `nodetool
  refresh` and `nodetool restore` (`/storage_service/restore`), and with
  them Scylla Manager. Whoever drives them must run `DELETE ALL DATA FROM
  <table> USING TIMESTAMP <data_deleted_at_timestamp>` on the destination table
  before loading the snapshot, and after truncating the table, if it is
  truncated, since TRUNCATE clears T. Most of the data they load is streamed
  and stripped at the source against the destination's T, but `scope=node`
  attaches fully-contained sstables as they are.
- Views are not restored from snapshots; they are rebuilt from the restored
  base table, which is read through stripping.

### Restore and TRUNCATE

The documented restore procedures (`restore.rst`, prerequisites) truncate an
existing table before loading the snapshot, "so that the existing data does
not overwrite the restored data". The native tablet restore
(`restore_tablets`) does not truncate; it attaches the snapshot's sstables on
top of the table's current content.

This procedure is unchanged: to restore into an existing table, TRUNCATE it
and load the snapshot. TRUNCATE removes the table's content physically and
clears T (see TRUNCATE clears T), so the table accepts the snapshot's data
whatever its timestamps, also when the table had T set by an earlier DELETE
ALL DATA, e.g. when the restore is meant to undo a mistaken one. No major
compaction, repair or waiting for T's GC is involved.

DELETE ALL DATA must not be used in place of TRUNCATE here. Restore means
"replace the table's content with the snapshot's", which is not expressible
by a timestamp-based tombstone: data written after the backup time B has
timestamps above B, so deleting it requires `T >= now`, which also shadows
all restored data (timestamps below B); and `T = B` deletes the pre-backup
data that the snapshot restores while keeping the post-backup data that
restore is meant to discard. TRUNCATE works because it orders by arrival on
the node (`max_data_age`), not by write timestamp. This is why TRUNCATE was
not given timestamped semantics (see Motivation): tools which truncate and
then restore would load data that stays deleted.

The same happens when a snapshot is loaded into a table that has T set,
without truncating the table first: loaded data at or below T is covered by
T, and is stripped and purged like any other covered data. The user-facing
restore documentation says so, and says to truncate the table first.

The exposure to late writes is that of any TRUNCATE (see TRUNCATE clears T):
a hint queued before the TRUNCATE can be replayed after it, and land in the
table after the restore.

### Alternative: drop and recreate the table

Another way to clear a table before a restore, used by administrators and
management tools (e.g. ScyllaDB Manager), is to drop the table and recreate
it, typically with the same table id (`CREATE TABLE ... WITH id = <uuid>`):

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

Like TRUNCATE, this leaves no T behind: dropping the table removes its
`system.tablets` partition, and T with it, and the recreated table starts
without T. Unlike TRUNCATE of a tablet table, it does not require every
replica to be alive: the data is removed by DROP TABLE on each node when it
applies the schema change (with the usual auto-snapshot). The procedure
works the same with or without the cluster feature, and for vnode keyspaces.

Reusing the table id has a hazard that a new id does not, and which T does
not change: writes made before the DROP that are still persisted or in
flight can land in the recreated table, since the mechanisms that discard
writes to a dropped table rely on the table no longer existing. This is
independent of this design:

- **Commitlog** entries from before the DROP are replayed into the recreated
  table if a node restarts while a segment still holds them, since DROP
  TABLE removes the table's truncation records. Tracked by SCYLLADB-5245, a
  regression from bc20861afb (#25683).
- **Batchlog** entries are not removed by DROP TABLE, and replay discards a
  mutation only if its table does not exist or the batch was written at or
  before the table's truncation time, which is `min()` for a newly created
  table. The fix proposed in SCYLLADB-5245 (a truncation record with
  `truncated_at = now` when a table is created with an explicit id) also
  covers the batchlog and Paxos learn.
- **Hints**, including view hints, are not removed by DROP TABLE either, and
  are discarded only if their table does not exist when they are sent; they
  are never checked against a table's truncation time. Tracked by
  SCYLLADB-5251.

With a new table id, all of these are discarded as writes to an unknown
table.

### Verifying T's GC

T is recorded in each table's own `system.tablets` partition, so whether it
has been cleared, by its GC or by a TRUNCATE, can be read from there:
`data_deleted_at_timestamp` is null when T is not set. The partition key is the
table id, so a user first looks it up in `system_schema.tables` (base and
index backing tables) or `system_schema.views` (materialized views), e.g.:

```
SELECT id FROM system_schema.tables
    WHERE keyspace_name = 'ks' AND table_name = 't';
SELECT data_deleted_at_timestamp FROM system.tablets WHERE table_id = <id>;
```

The `keyspace_name`/`table_name` static columns of `system.tablets` cannot
be used instead, since a co-located view's partition holds only
`base_table`. Because this is cumbersome and must be repeated for every view
and index, a user-facing status command is planned (see Open questions),
reporting for a table and each of its views and indexes: the current T,
whether condition 1 holds, and whether its replicas are clean.

## Tablet lifecycle interactions

- **Migration / split / merge**: T is per-table static state, unaffected by
  all three. File-based streaming copies raw sstables; the destination
  strips them against T from its own tablet map. Only the GC-progress
  per-tablet column needs split/merge/migration rules (copy / min / reset).
- **Mixed clusters**: a new `gms` cluster feature,
  `EVENTUALLY_CONSISTENT_DELETE_ALL_DATA`, gates the DELETE ALL DATA
  statement, and with it the `system.tablets` columns and stripping, which
  have nothing to act on while no T is set. Until the feature is enabled the
  statement is rejected, and so is the restore of a snapshot which recorded
  T. Nodes without the feature must never serve reads for a table with T
  set — guaranteed by setting T only once the feature is cluster-enabled,
  after which no old node can join. TRUNCATE does not depend on the
  feature: it is the same physical operation before and after, and has a T
  to clear only after the feature is enabled.

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
5. The `DELETE ALL DATA` grammar and statement, and the group0 write path,
   including the views/indexes fan-out and the barrier.
6. Whole-sstable expiry under T in compaction (compaction output is already
   stripped by phase 3).
7. GC protocol: tablet cleanliness check, major compaction reporting it,
   coordinator confirmations and background sweep, clearing T; staging
   sstable handling; the T's GC status command. TRUNCATE clearing T.
8. Backup/restore manifest integration.
9. User-facing documentation of the statement, of TRUNCATE clearing T and
   of restoring into a table with T set; `mutation_dump`/tools exposure.

Each phase is independently testable; phases 1–5 already deliver a correct
(if never-GC'd) DELETE ALL DATA, with 6–7 completing the tombstone
equivalence.

Phases 1–7 are implemented, with the limitations listed in GC of T. Phase 8
is implemented for snapshot manifests and the tablet-aware restore, with the
limitations listed in Backup and restore. Of phase 9, the user-facing
documentation is written (`docs/cql/dml/delete.rst`, `docs/cql/ddl.rst`, the
restore procedure); the `mutation_dump`/tools exposure is not done. The
tests are in `test/cluster/test_delete_all_data.py`,
`test/boost/tablets_test.cc` and `test/cluster/object_store/test_backup.py`.

## Open questions

1. The form of the status command for T's GC (see Verifying T's GC): a
   nodetool command backed by a REST API, a virtual table, or both.
2. The drop-and-recreate restore procedure depends on independent fixes for
   writes from before the DROP landing in a table recreated with the same
   id: SCYLLADB-5245 (commitlog, and batchlog and Paxos learn via its
   proposed fix) and SCYLLADB-5251 (hints, including view hints).
