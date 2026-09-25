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

We introduce a separate set of Raft system tables for strongly consistent tablets:

- `system.raft_groups`
- `system.raft_groups_snapshots`
- `system.raft_groups_snapshot_config`

`system.raft_groups` stores only non-log Raft metadata (term, vote, commit_idx, and the last
truncate the group applied: `truncated_at`, `last_truncate_request_id`) — unlike
`system.raft`, it does not contain log entries (those are stored in the commitlog).
`system.raft_groups_snapshots` and `system.raft_groups_snapshot_config` mirror the logical
contents of `system.raft_snapshots` and `system.raft_snapshot_config` respectively.
All these tables use a composite partition key `(shard, group_id)` rather than just `group_id`.

To make “(shard, group_id) belongs to shard X” true at the storage layer, we use:

- a dedicated partitioner (`service::strong_consistency::raft_groups_partitioner`)
	which encodes the shard into the token, and
- a dedicated sharder (`service::strong_consistency::raft_groups_sharder`) which extracts
	that shard from the token.

As a result, reads and writes for a given group’s persistence are routed to the same shard
where the Raft server instance runs.

## Token encoding

The partitioner encodes the destination shard in the token’s high bits:

- token layout: `[shard: 16 bits][group_id_hash: 48 bits]`
- the shard value is constrained to fit the `smallint` column used in the schema.
  it also needs to be non-negative, so it's effectively limited to range `[0, 32767]`
- the lower 48 bits are derived by hashing the `group_id` (timeuuid)

The key property is that shard extraction is a pure bit operation and does not depend on
the cluster’s shard count.

## No direct migration support

`raft_groups_sharder::shard_for_writes()` returns up to one shard - it does not support
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

Only the Raft log entries themselves go into the commitlog. Other metadata (term, vote,
commit index, snapshot descriptors) are still stored in the CQL system tables described
above, since they are updated less frequently.

### What goes where

```
┌─────────────────────────────────────────────────────────┐
│                  Persistence split                       │
├─────────────────────────┬───────────────────────────────┤
│     Commitlog           │     CQL system tables         │
│  (fast, fsync-batched)  │  (infrequent updates)         │
├─────────────────────────┼───────────────────────────────┤
│  Raft log entries       │  term / voted_for             │
│  (mutations wrapped in  │  commit_idx                   │
│   raft metadata)        │  snapshot descriptor          │
│                         │  snapshot configuration       │
└─────────────────────────┴───────────────────────────────┘
```

### Write path

When the Raft leader replicates log entries to a follower (or persists its own),
the following happens:

```
Raft engine
    │
    ▼
raft_groups_storage::store_log_entries()
    │
    ▼
commitlog_persistence::store_log_entries()
    │
    │  for each entry:
    │    serialize as raft_commitlog_entry variant
    │    write to shared commitlog (force_sync = yes)
    │    store resulting rp_handle in map[raft_index]
    │
    ▼
commitlog segment on disk
```

Key points:

- Each Raft log entry is written as a `raft_commitlog_entry` — a new variant type
  in the commitlog entry format (alongside the existing mutation variant).
- The write returns an `rp_handle` (replay position handle). This handle keeps the
  commitlog segment alive as long as the handle exists.
- All handles are stored in a map keyed by Raft log index inside `commitlog_persistence`.

### Apply path (connecting Raft commit to memtable flush)

When the state machine applies a committed entry (turning the Raft log entry's mutation
into actual table data), we need to ensure the commitlog segment stays alive until the
memtable containing that data is flushed to sstable. This is done by moving the
`rp_handle`:

```
State machine applies entry at index N
    │
    ▼
acquire_replay_position_handles_for(N)
    │
    │  move rp_handle for index N
    │
    └──► handle → attached to memtable
         (keeps segment alive until flush)
```

This reuses the same mechanism that normal mutations use to tie commitlog segment
lifetime to memtable flush — no new GC logic needed.

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
3. Then the record is written: `truncated_at` and `last_truncate_request_id`. It is written
   after the drop because it is a receipt, not the intent; the intent is the entry in the
   log. A crash between the two leaves no record, so the entry is applied again onto an
   already empty tablet.

No `system.truncated` record is written. It would raise the minimum replay position of the
shard and make the commitlog replay skip whole segments, together with the Raft entries of
every other group stored in them.

The topology coordinator drives the operation, over the same
`global_topology_request::truncate_table` the eventually consistent truncate uses. For a
table whose tablet map carries Raft groups (`tablet_map::has_raft_info()`),
`handle_truncate_table()` sends `truncate_tablet` to the replicas of every tablet instead of
`truncate_with_tablets` to every host. A replica answers whether it has committed the entry
as the group's leader; a replica which is not the leader names the leader it knows. The
coordinator asks the tablet's live, non-excluded replicas in turn, the hinted leader first,
and repeats the round after a pause until one of them commits the entry.
The RPC phase needs only a Raft quorum per tablet: dead and excluded replicas are skipped,
and a tablet without a quorum keeps the request waiting. The topology barrier which fences
the operation still has to reach every node which is not ignored (see the FIXME in
`global_tablet_token_metadata_barrier()`), so a node which is merely down holds the truncate
at the barrier until it is back or ignored, as it holds every topology operation.

Success means that every tablet's group has committed its `truncate_command`. A replica
which missed the entry still holds the data until it catches up on the log, and a read
which is not linearizable may return pre-truncate data from it meanwhile; a linearizable
read runs a read barrier first and never does.

There is no cluster feature for the command or the verb: strongly consistent tables are
experimental, and the feature which enables them is advertised only by nodes started with
the experimental flag, so no mixed cluster holds such a table. Before the feature ships, a
feature of its own has to gate the strongly consistent branch of `handle_truncate_table()`.

Not implemented yet, planned as a follow-up:

- The commitlog replay skips a `truncate_command` with a warning. Until it applies one, a
  crash after a truncate can bring pre-truncate data back: the entries below the truncate
  are applied again while the truncate itself is not. The replay needs the index of the
  truncate entry to filter them, and the entry has to stay in the commitlog until the
  record is durable, so `apply()` will stop releasing that entry's replay position handle
  at the same time. Today it releases it with the batch, like a write's.
- A snapshot at the truncate index, which is what would let the commitlog reclaim the
  segments holding the tablet's pre-truncate entries. Snapshots are not implemented for
  these groups.

### Crash recovery (commitlog replay)

On startup, the commitlog replayer reads all segments and encounters both normal
mutation entries and Raft log entries. The flow is:

```
Commitlog replayer (startup)
    │
    ├── mutation entry → apply to memtable (existing path)
    │
    └── raft_commitlog_entry → route to raft_commitlog_replay_buffer
                                (per-shard collection)
```

After replay completes, each group's collected entries are processed by
`process_raft_replayed_items()`:

```
Replayed entries for group G
    │
    ▼
Load commit_idx and snapshot from CQL tables
    │
    ▼
Sort and deduplicate entries, handle leader changes
(higher term + lower index → discard old uncommitted tail)
    │
    ├── entries with idx ≤ commit_idx (committed):
    │     deserialize mutations
    │     apply to memtable in-memory
    │     (they may not have been flushed before crash)
    │
    └── entries with idx > commit_idx (uncommitted):
          rewrite to NEW commitlog
          (to get fresh rp_handles for the new session)
```

After processing, the recovered log entries (with valid `rp_handle`s) are handed
to `commitlog_persistence` when the Raft group starts up.

### Snapshotting and truncation

As the Raft log grows, old entries are no longer needed once a snapshot covers them.
Truncation releases the `rp_handle`s, which allows commitlog segment GC:

```
Snapshot taken at index S
    │
    ▼
store_snapshot_descriptor() → CQL tables
    │
    ▼
commitlog_persistence::truncate_log_tail(S - trailing)
    │
    │  erase all handles with index ≤ (S - trailing)
    │  handles are destroyed → segment dirty count decremented
    │  → commitlog can recycle those segments
    │
    ▼
Old segments become eligible for deletion
```

There is also `truncate_log(idx)` which discards the **tail** (entries with index ≥ idx).
This is used when a leader change invalidates uncommitted entries.

### Shutdown behavior

On clean shutdown, remaining `rp_handle`s in the map are **released without decrementing**
the segment dirty count (via `handle.release()`). This ensures the commitlog segments
survive on disk, so uncommitted entries are still available for replay on the next startup.

This is important: if we decremented the dirty count on shutdown, the commitlog might
delete segments containing uncommitted Raft entries that we still need.
