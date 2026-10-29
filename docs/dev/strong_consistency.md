# Introduction

This document describes the implementation details and design choices for the
strongly-consistent tables feature.

The feature is heavily based on the existing implementation of Raft in Scylla, which
is described in [docs/dev/raft-in-scylla.md](raft-in-scylla.md).

The persistence layer has two parts:
- CQL system tables for infrequently-updated metadata (term, vote, snapshots)
- The shared database commitlog for Raft log entries (high-frequency, fsync-batched)

# Raft metadata persistence

## Group0 persistence context

The Raft groups for strongly consistent tables differ from Raft group0 particularly
in the extent of where their Raft group members can be located. For group0, all
group members (Raft servers) are on shard 0. For groups for strongly consistent tablets,
the group members may be located on any shard. In the future, they will even be able
to move alongside their corresponding tablets.

That's why, when adding the Raft metadata persistence layer for strongly consistent tables,
we can't reuse the existing approach for group 0. Group0's persistence stores all Raft state
on shard 0. This approach can't be used for strongly consistent tables, because raft groups
for strongly consistent tables can occupy many different shards and their metadata may be
updated often. Storing all data on a single shard would at the same time make this shard
a bottleneck and it would require performing cross-shard operations for most strongly
consistent writes, which would also diminish their performance on its own.

Instead, we want to store the metadata for a Raft group on the same shard where this group's
server is located, avoiding any cross-shard operations and evenly distributing the work
related to writing metadata to all shards.

## CQL system tables for Raft metadata

Strongly consistent tablets keep their Raft metadata in one table,
`system.raft_groups`, with a composite partition key `(shard, group_id)` rather than
just `group_id`.

It has its own schema, separate from group 0's `system.raft`: there are no log
columns, because the log lives in the commitlog, and therefore no clustering key,
so each group is one ordinary row. It holds `vote`/`vote_term` and the whole
snapshot descriptor: `snapshot_idx`, `snapshot_term`, `snapshot_config` and the
`truncations` history that replay needs, plus the last truncate the group applied:
`truncated_at`, `last_truncate_request_id`, `truncate_index` (see Truncate below).
`snapshot_idx` is committed by definition,
since a segment's record is released only once every index it holds is committed, so it is
what a restart resumes from, even though the group had usually committed further than
its last release.

The table is marked `wait_for_sync_to_commitlog`, because votes and the initial
descriptor are written by CQL and must be durable before the write that depends
on them is acknowledged (SCYLLADB-3828).

To make “(shard, group_id) belongs to shard X” true at the storage layer, we use:

- a dedicated partitioner (`dht::fixed_shard_partitioner`) which encodes the shard
	into the token, and
- a dedicated sharder (`dht::fixed_shard_sharder`) which extracts that shard from the
	token.

As a result, reads and writes for a given group’s persistence are routed to the same shard
where the Raft server instance runs.

## Token encoding

The partitioner encodes the destination shard in the token’s high bits:

- token layout: `[shard: 16 bits][group_id_hash: 48 bits]`
- the shard value is constrained to fit the `smallint` column used in the schema.
  it also needs to be non-negative, so it's effectively limited to range `[0, 32767]`
- the lower 48 bits come from murmur3 over the whole partition key in its legacy
  form, shard component included, not over the `group_id` alone

The key property is that shard extraction is a pure bit operation and does not depend on
the cluster’s shard count.

## No direct migration support

`dht::fixed_shard_sharder::shard_for_writes()` returns up to one shard - it does not support
migrations using double writes. Instead, for a given Raft group, when a tablet is migrated,
the Raft metadata needs to be erased from the former location and added in the new location.

## Commit log based persistence for Raft log entries

### Motivation

Raft log entries for tablet groups need to be persisted durably before they can be
acknowledged. Using CQL tables for this (as group0 does) would be expensive: each
write would require serialization, schema lookup, and a full CQL write path.

Instead, we store Raft log entries directly in the **shared database commitlog** —
the same commitlog already used for mutation persistence. This gives us:

- **Fsync batching**: multiple Raft entries (potentially from different groups) share
  the same disk fsync, amortizing the cost.
- **No CQL overhead**: entries are serialized in a compact binary format directly into
  commitlog segments.
- **Shared infrastructure**: no additional files or background tasks — the existing
  commitlog recycling and segment management is reused.

Only the Raft log entries themselves go into the commitlog. The rest of the metadata
(vote, snapshot descriptor, truncation history) stays in `system.raft_groups`, since it
changes far less often.

### What goes where

```
┌─────────────────────────────────────────────────────────┐
│                   Persistence split                     │
├─────────────────────────┬───────────────────────────────┤
│     Commitlog           │     system.raft_groups        │
│  (fast, fsync-batched)  │  (infrequent updates)         │
├─────────────────────────┼───────────────────────────────┤
│  Raft log entries,      │  vote_term / vote             │
│  one entry per batch    │  snapshot_idx / snapshot_term │
│  (mutations wrapped in  │  snapshot_config              │
│   raft metadata)        │  truncations                  │
└─────────────────────────┴───────────────────────────────┘
```

### Write path

Raft hands the io fiber a batch of entries to persist. The batch becomes one commitlog
entry, or, when it is too large for one, as many whole batches as it takes:

```
Raft engine
    │
    ▼
raft_groups_storage::store_log_entries()
    │
    ▼
raft_commitlog::store_log_entries()
    │
    │  measure the batch once; if it fits, serialize
    │  {group_id, commit_idx, entries} as one raft_commitlog_batch
    │  and add() it (force_sync = yes). If it does not fit,
    │  split_raft_batch() cuts it into runs that each do.
    │
    ▼
one commitlog entry, one replay position, one rp_handle
    │
    ▼
account_batch() folds the handle into the group's queue
```

Key points:

- One commitlog entry per batch, not per raft entry: the group id and the entry
  envelope are written once per batch instead of once per entry, and the batch has a
  single position. A batch too large for one commitlog entry is split into whole
  batches by `split_raft_batch()`, the same way replay rewrites a recovered tail,
  because the commitlog would otherwise fragment it and a copy of an entry has to
  live in exactly one segment.
  A single *command* entry always fits: `check_commitlog_can_hold_a_raft_entry()`
  compares `max_single_entry_batch_size(max_command_size)` against
  `commitlog::max_record_size()` before any group starts, and again before replay
  rewrites a recovered tail, and the node refuses to start if it would not fit.
- The batch header carries the group's `commit_idx` at write time. Replay uses it as a
  floor to decide which of the entries it reads are already committed.
- The group does not keep a handle per index. It keeps a queue of `segment_record`s,
  one per commitlog segment its entries landed in. A later batch in the same
  segment only advances that record's `max_index`, because retention is accounted
  per segment, not per position.
- Each record holds two references to its segment: `pin_user_table` under the target
  table, which is what the memtable pins are cloned from, and `pin_raft_groups` under
  `system.raft_groups`, taken at record creation. That order matters: a segment
  holding only raft entries is `raft_groups`-dirty because of `pin_raft_groups`
  alone, and the closed signal a release waits for arrives only in flush requests
  carrying that table's id.

### Apply path (connecting Raft commit to memtable flush)

When the state machine applies a committed entry, the commitlog segment has to stay
alive until the memtable holding that data is flushed. The record's own reference is
cloned for each applied mutation:

```
State machine applies entry at index N
    │
    ▼
pin_for_apply(N)
    │
    │  clone pin_user_table of the record covering N
    │  (another reference to the same segment, no bytes written)
    │
    └──► handle → attached to memtable
         (keeps segment alive until flush)
```

This reuses the same mechanism normal mutations use to tie segment lifetime to
memtable flush, with no new GC logic.

### Releasing a segment

A record is the unit of release. It can go once nothing more can be needed from it:

- every index it covers is committed (`max_index <= commit_idx`),
- every *command* it covers is applied (`last_cmd() <= apply_idx`): dummies and
  configurations never reach `apply()`, so waiting for `max_index` would wedge the
  queue,
- and no further entry of this group can land in its segment. Either a later record
  exists in the queue, which means the commitlog already moved on to a newer segment,
  or the commitlog's flush rounds have got past this segment's position, which they do
  only for segments that stopped allocating.

Two things answer whether the rounds have got past a segment, and both are needed.
`groups_manager`'s flush handler forwards each round's position to every group, which
is the fresher of the two while a round is being reported: `flush_segments()` runs the
handlers *before* it advances its own position, and a release happens inside that
report. And the group reads `commitlog::flush_position()` itself, which covers a round
nobody forwarded, one that ran before `groups_manager::start()` registered the handler,
which is normal, because replay writes a group's recovered tail well before then. A
round never repeats for the same segment, so without that read those segments would
wait for a signal that cannot come.

Releasing writes the record's `(max_index, max_term())` and its newest
configuration to `system.raft_groups` as the group's snapshot descriptor, by an
in-memory mutation applied straight to that table's memtable, moving
`pin_raft_groups` into it. The segment then lives at least until the `raft_groups`
memtable flush that makes the descriptor durable. A segment holding a group's
entries is reclaimed only once a durable descriptor covers them.

A leader change that discards an uncommitted tail cannot be expressed by the
descriptor alone, so it appends a `truncation_record {segment, from, to}` to the
`truncations` column. Replay needs it to tell a discarded copy from a live one. Records
are dropped from the column once the commitlog no longer lists their segment. The
segment file is removed a moment later, in the background, so a crash in between
leaves copies on disk with no record covering them. SCYLLADB-4094 tracks closing
that window.

### Asking for the flush a release missed

The commitlog asks a closed segment's dirty tables to flush exactly once, tracked by
`segment_manager::_flush_position`. That round is what normally reclaims the segment:
it names both the tablet table and `system.raft_groups`, and a flush request seals the
whole memtable, so everything the segment holds goes with it.

For a raft group, that one round can be too early. Its insertions for a segment are
spread over time: a command reaches the tablet table's memtable when it is applied,
which follows the commit, and the descriptor reaches `system.raft_groups` only at the
release. Whatever lands
after the round is dirty in a segment nothing will ask about again, and the segment
stays on disk.

So a release that happens *after* its segment's round asks for the flush itself, at
`replay_position(segment + 1, 0)`:

```
segment S closes
    │
    ├── round: flush(tablet table, high), flush(raft_groups, high)   ── once, ever
    │
    ├── later: entry committed → apply → clone into tablet memtable
    │
    └── later: record released → descriptor into raft_groups memtable
                   │
                   └── round already ran?  ──yes──► ask for flush(S+1)
                                            ──no───► the round ahead covers it
```

The release is the right place because it comes after every insertion that can
reference the segment: the gate waits for the last command to be applied, and the
descriptor is written right there. The request carries a replay position at the start of
the *next* segment. That position is not a bound on what gets written: a table skips the
request when the position is below everything its memtables hold, and otherwise seals them
whole. A position at the start of the next segment is above everything this one
contributed, so the request always stands.

A record holding no command asks only `system.raft_groups`. `apply()` never sees
dummies or configurations, so no clone into the tablet table's memtable was ever made,
and the pop gives up the batch's own reference.

This is only about reclaiming the segment, not about durability. The entries stay in
the segment, and the record holds it until the release, so a crash before any of this
is recovered by replay.

### Truncate

`TRUNCATE TABLE` on a strongly consistent table is serialized in each tablet's Raft log as
a `truncate_command`, the alternative of `raft_command` next to `write_mutation`. The
group's leader stamps it with a write timestamp from the same generator as the writes
(`truncated_at`). The entry's position in the log is the linearization point: applying it
drops every write ordered before it and keeps every write ordered after it, which `apply()`
applies to the emptied tablet in log order like any other entry of the batch.

`apply()` handles the command with `apply_truncate_command()`:

1. If the group's record in `system.raft_groups` already carries the command's
   `request_id`, the entry is a retry of a truncate this replica has applied and is a no-op.
   The topology coordinator retries a `TRUNCATE` until every tablet has committed its entry,
   and a retry can land a second entry in the same log. Dropping the tablet twice is not the
   same as dropping it once when a read can observe the state in between, so the writes
   committed between the two entries survive. The id is the topology request's id, so a new
   `TRUNCATE` statement is a new truncate.
2. Otherwise `table::truncate_tablet_locally()` drops the tablet's data on this shard:
   memtables without flushing them, sstables by deleting them. The storage group stays
   writable for the entries that follow.
3. Then the record is written: `truncated_at`, `last_truncate_request_id` and the entry's
   log index, `truncate_index`. It is written after the drop because it is a receipt, not the
   intent; the intent is the entry in the log. A crash between the two leaves no record, so
   the entry is applied again onto an already empty tablet.

   The record is written through the commitlog with a forced sync, like the entries
   themselves, and the entry's apply returns only once it is on disk; the entry is reported
   applied (`note_applied()`) after that, so its segment's record is not released before. So
   no entry after the truncate is applied until the record is durable: a crash finds either
   no record and no data written after the truncate (the entry is replayed), or the record,
   which tells the replay which entries the truncate discarded.

No `system.truncated` record is written. It would raise the minimum replay position of the
shard and make the commitlog replay skip whole segments, together with the Raft entries of
every other group stored in them.

The topology coordinator drives the operation, over the same
`global_topology_request::truncate_table` the eventually consistent truncate uses. For a
table whose tablet map carries Raft groups (`tablet_map::has_raft_info()`),
`handle_truncate_table()` sends `truncate_strongly_consistent_tablet` to the replicas of every tablet instead of
`truncate_with_tablets` to every host. A replica answers whether it has committed the entry
as the group's leader. The coordinator asks the tablet's live, non-excluded replicas at once,
and repeats the round after a pause until one of them commits the entry. A client-side
timeout does not end this: the request stays queued and the coordinator keeps retrying, so
a `TRUNCATE` that timed out on the client can still take effect later.
The RPC phase needs only a Raft quorum per tablet: dead and excluded replicas are skipped,
and a tablet without a quorum keeps the request waiting. The topology barrier which fences
the operation still has to reach every node which is not ignored (see the FIXME in
`global_tablet_token_metadata_barrier()`), so a node which is merely down holds the truncate
at the barrier until it is back or ignored, as it holds every topology operation.

Success means that every tablet's group has committed its `truncate_command`. A replica
which missed the entry still holds the data until it catches up on the log, and a read
which is not linearizable may return pre-truncate data from it meanwhile; a linearizable
read runs a read barrier first and never does.

There is no single cut across the tablets: each tablet's group commits its entry at its own
moment, so a write running concurrently with the `TRUNCATE` may land before the entry on one
tablet and after it on another, and so be dropped on the first and survive on the second.
The same holds for a batch across tablets.

The commitlog replay (`apply_committed()`) honours the record: a committed entry at or
below `truncate_index` is dropped, since the record is durable only if the drop it stands
for happened, and a committed `truncate_command` above it is applied with
`apply_truncate_command()`, which drops the entries replayed before it.

Not implemented yet, planned as a follow-up:

- A snapshot at the truncate index, which is what would let the commitlog reclaim the
  segments holding the tablet's pre-truncate entries. Snapshots are not implemented for
  these groups.
- Snapshot transfer has to carry the truncate record, and a replica installing a snapshot
  whose `truncate_index` is above its applied index has to drop all its local data for the
  tablet first: the truncate entry is below the snapshot, so the replica never applies it,
  and without the drop the data it holds from before the truncate would survive. The
  snapshot-transfer tickets do not mention this; it is written down only here.

### Crash recovery (commitlog replay)

The replayer reads segments in id order and meets both normal mutation entries and
raft batches:

```
Commitlog replayer (startup)
    │
    ├── mutation entry → apply to memtable (existing path)
    │
    └── raft_commitlog_batch → raft_commitlog_replay_buffer
```

Raft replay is a single pass. Nothing is collected and sorted afterwards; each batch is
processed as it is read, which is what bounds how much is buffered: at most a group's
uncommitted window, rather than everything the surviving segments hold.

Startup first replays the same segments restricted to the system tables, so
`system.raft_groups` holds its final row when `resolve_group()` reads it: the floor and
truncation records that reached disk, or no row at all. The restricted pass skips raft
batches, so each batch reaches `add_batch()` once, in the full pass.

```
raft batch for group G, carrying commit_idx in its header
    │
    ▼
first batch of this group: read its row from system.raft_groups
(snapshot_idx is the floor; snapshot_term, snapshot_config, truncations,
and the truncate record)
    │
    ▼
raise the floor to the header's commit_idx (it only ever advances)
and drain everything buffered at or below it: a header is written
only after the final copy of every index it covers, so what is below
the floor is final and can be applied now
    │
    ▼
drop_truncated_copies(): drop the copies a truncation superseded. Each
truncation record of this segment is a cursor; the copy is claimed by
the oldest cursor *waiting for that index*, not simply by the oldest
one, because truncations of one segment can reach back past each other
    │
    ▼
supersede: an entry at index N in a different term replaces anything
still buffered at or above N, and what that dropped is written into
the group's own truncation records. Without those records a second
replay of the same segments would see the dropped copies below the
new floor and apply an entry no leader committed. A copy in the same
term is the same entry, so the buffer keeps its tail
    │
    ├── entries at or below truncate_index: dropped by the recorded truncate
    │
    ├── entries at or below the floor (committed):
    │     apply to the memtable in-memory, or run the truncate
    │     (they may not have been flushed before the crash)
    │
    └── entries above the floor (uncommitted):
          buffer as the group's tail
```

At the end of the pass, `finish_replay()` walks the groups and, for each one, writes
its recomputed row and then rewrites its buffered tail to the new commitlog. The
tail can outgrow one commitlog entry, so `split_raft_batch()` cuts it into runs and
each run is written as its own batch. The order matters within a group, not across
them: the row goes through CQL, and `system.raft_groups` carries
`wait_for_sync_to_commitlog`, so the row is in the commitlog before the write returns.
A rewritten batch on disk therefore always has a durable base row. The rewrite is also
what gives the tail fresh references in the new session, through the same
`account_batch()` the write path uses.

The pass refuses to finish when a group's unbroken run of read indexes stops below
the highest `commit_idx` a batch header declared. The floor it writes says every
index at or below `commit_idx` reached a memtable, so a gap under the floor would
persist a claim about entries that never did. A copy a truncation superseded does
not extend the run: nothing applies it, so counting it would cover an index whose
current copy went down with a lost segment. A supersede also takes back the indexes
it pops out of the buffer, which an earlier batch had already counted, so the run
falls back to just below the batch that superseded them.

A segment the replay could not read in full refuses the boot too, once this shard hosts
a strongly consistent group. The index checks above catch a gap they can see, and a lost
tail is the case they cannot: the batch declaring the higher `commit_idx` went down with
the unread bytes, so raft gets a log shorter than the one the node acknowledged. Replay
names the segments and stops. `strongly_consistent_tables_start_on_damaged_commitlog`
starts the node without them, which is the way out while the commitlog still reports an
intact segment as damaged (SCYLLADB-4853). The node that starts that way runs a log that
may be short of entries the quorum committed.

A group this shard no longer hosts is discarded rather than recovered: its entries
belong to whoever holds the tablet now. The test is `hosts_raft_group()`, the same one
that decides whether the running group is torn down, and it is stage-aware - a leaving
replica stops hosting the group at `cleanup`, while it is still listed in
`tablet_info::replicas`. Deciding by the replica list instead would recover a log whose
segments the teardown has already given up.

### Shutdown behavior

On clean shutdown a group is dropped with `log_disposition::keep`, and
`~raft_commitlog()` *detaches* its references: `rp_handle::release()` abandons the
decrement, so the segments survive the object and the uncommitted entries are still
there to replay.

`log_disposition::release` is the other case: a group that is genuinely gone, its
tablet migrated away or its table dropped, will never be replayed. `release_all()`
destroys the handles instead of detaching them, so the use counts drop and the
segments can be reclaimed (SCYLLADB-3827).

# Metrics

The raft servers and RPC modules of a table's tablets on a shard share one set of
counters, exported under the `scylla_strong_consistency_raft_` prefix with the `ks` and
`cf` labels. They follow the flags of the other per-table metrics: summed over the shards
of the node by default (`enable_node_aggregated_table_metrics`), per shard with
`enable_keyspace_column_family_metrics`, and not exported when both are off. The counters
belong to the table rather than to a group: they keep their values when a tablet migrates
away, stay on a shard after its last tablet has left it, and are dropped with the table.
Gauges such as `leaders`, `uncommitted_entries` or `blocked_followers` are summed over the
running servers of the table instead.
