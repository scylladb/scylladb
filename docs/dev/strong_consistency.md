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

`system.raft_groups` stores only non-log Raft metadata (term, vote, commit_idx, and the resize
markers of a parent group being replaced by a tablet split or merge) — unlike
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

# Tablet split

A strongly consistent tablet keeps its data in a Raft group of its own, so splitting one is
not just a matter of rewriting the tablet map: the group of the tablet being split has to be
replaced by the groups of the two tablets it is split into. Three properties have to hold
across the replacement:

- no acknowledged write may be lost,
- no read may observe a state older than a write already acknowledged,
- no write may be accepted by a group which the new tablet map no longer refers to.

This section describes how the split achieves that. Throughout it, and in the code, the group
being replaced is called the **parent** and the groups replacing it the **children**. The same
machinery is meant to serve a tablet merge, where a child has several parents, hence the
neutral name *resize* in the identifiers; merging is not implemented yet and no merge decision
is emitted for a strongly consistent table.

Two different components are called a coordinator here. The **topology coordinator**
(`service::topology_coordinator`) is the cluster-wide one which drives the split; the **request
coordinator** (`service::strong_consistency::coordinator`) is the per-request, replica-side one
which serves a read or a write. Both are named explicitly wherever the difference matters.

## Sealing the parent

Sealing is driven from outside the group, by the topology coordinator, over the
`process_raft_resize` verb: the markers have to be committed in the parent's log, which only
its current leader can do, and every replica has to have applied the second one before the
tablet map is replaced. It commits two markers:

- `start_resize` - from here on the parent no longer accepts writes, and the request
  coordinator hands the writes of its token range off to the child covering the token,
- `end_resize` - from here on the parent's log is final, and the appliers of the children
  are released.

The verb has no cluster feature of its own, which holds only while strongly consistent tables
stay experimental. A node advertises `STRONGLY_CONSISTENT_TABLES` only if it was started with
the experimental flag (`init.cc`), and the cluster feature is enabled only once every node
advertises it, so no cluster which upgrades into this version has it on and no table whose
split could send the verb can exist next to a node predating this series. That argument expires
when the feature ships: the verb has to be gated by a cluster feature of its own before
strongly consistent tablets are supported, or a mixed cluster would fail the seal with an
unknown verb and never finish the finalization.

The markers are ordinary entries in the parent's log, so they are replicated and applied in
log order like any write. They are *not* carried as mutations, though: each marker is recorded
as a static column of the group's own row in `system.raft_groups`, whose partition key contains
the shard hosting the group, and replicas may host the same group on different shards. A single
mutation could therefore not serve all of them, so `raft_command` is a variant
(`write_mutation`, `resize_marker`, `no_op`) and every replica builds its own mutation when it
applies the entry. Only the presence of a marker is ever read back; the kind of resize in
progress comes from the tablet metadata.

Building the mutation locally also puts the row in a different table than the tablet's own, and a
flush credits a position to the table being flushed - so an entry is accounted to the table which
receives what applying it writes rather than to the tablet's table. Every entry is classified
before it is appended (`detail::command_target_table()`), which names `system.raft_groups` for a
marker and the tablet's table for a write, and the applier moves the handle into the memtable
receiving the row exactly as it does for a write.

`service::strong_consistency::raft_resize_tracker` holds the per-shard state of every
resize a replica takes part in, tracks which markers have been applied, and owns the promise
the child appliers wait on. It is reloaded from `system.raft_groups` before the parent's Raft
server is created, because after a restart the parent's committed entries are applied by
commitlog replay rather than by `state_machine::apply()`, so nothing would fulfil the promise
otherwise. Only the parent's start loads it: a child's applier merely needs the state to
exist by the time it runs, which `groups_manager::update()` guarantees on its own,
synchronously with recording the resize. The tracker deliberately holds nothing but that
state: it is started before `groups_manager` so that the state machines and the commitlog
replay can use it without the Raft servers being up, which anything driving those servers
would undo.

The state is dropped by the teardown of the parent's Raft server, which every ending of a resize
this replica has observed goes through: the resize is only ever ended by the tablet map being
replaced, and that takes the parent's tablet away. Nothing else takes it away in the meantime -
the resize can no longer be revoked, and the tablet is not migrated while its replacement ids are
recorded. The teardown may find a child's applier still
parked on the promise - a node shutting down or a table being dropped gets there - so the promise
is broken with an `abort_requested_exception`, which the state machine treats as a clean end of
its fiber rather than as a background error. Each child's mapping is dropped by the child's own
teardown, or by `update()` when it observes that the group now serves a tablet of its own.

That the parent's teardown is what releases a parked applier is also why the wait needs no abort
source of its own, which matters because it could not have one that works: `raft::server::abort()`
joins the applier fiber *before* it aborts the state machine, the RPC and the persistence, so none
of those aborts can reach a fiber which is already inside `apply()`. A child is never torn down
while its parent survives - both hold their tablets only while the resize is in the tablet
metadata, and the write which clears it takes the parent's tablet away in the same breath - so a
child being torn down always has its wait ended by its parent's teardown, either just before or
just after its own. Every other suspension in `apply()` completes on its own.

The co-location fiber is detached when the parent group is deleted, which is the only ending a
resize recorded on a replica has - the tablet map replacement takes the parent's tablet away, and
nothing can undo a resize whose replacement ids are already recorded. It then drains in the
background, joined only by `groups_manager::stop()`; the gates of the raft servers guard any
access the aborted fiber still makes on its way out.

## Handing the token range over without a gap

Two things have to hold for the hand-over to be seamless.

**A write must either land in the parent's log or be handed off, with nothing in between.**
The request coordinator checks the `start_resize` flag immediately before entering `add_entry()`,
without suspending in between, so a write which saw the flag unset is submitted before the
flag was set. That is only enough because `add_entry()` appends entries in the order in which
callers entered it, rather than in the order in which they happen to win the memory permit -
which is why the FIFO admission in `raft::server_impl` exists. A write which observed
`!start_resize` is therefore appended ahead of the `end_resize` marker, and so ends up in the
final log of the parent.

A linearizable read checks the flag before *and* after its read barrier, since the barrier may
be what applies the marker.

**A handed-off write has to be ordered after everything already committed in the parent.**
Timestamps are handed out by the leader, so the handed-off writes have to come from the same
clock as the ones already in the parent's log: the leaders of the parent and of the children
have to sit on the same node. They are elected together to begin with - a child derives its
fast bootstrap seed from the current leader of its parent, which every replica knows, so they
all pick the same node - and a fiber per resize (`groups_manager::leader_colocator()`) transfers a child's
leadership back to the parent's leader whenever the two drift apart, using the targeted
`raft::server::stepdown()`. Until they are co-located the child bounces the request back to the
parent, where the request coordinator retries it, since it has nowhere else to go. On top of that, a handed-off write
carries the timestamp it got from the parent, and the child's clock is advanced past it before
a new one is handed out.

## Reads on a child before the parent is done

A child group must not apply anything until the parent has applied everything it committed;
otherwise a read served by the child could observe a state older than a write already
committed in the parent. The child's applier therefore blocks on `end_resize`.

## Recovery

Commitlog replay writes to the memtables directly, bypassing `state_machine::apply()` and
therefore the wait above, so it has to reproduce the ordering itself. It replays in two passes:
first every group which is not replacing another one, which puts the whole log of a parent in
place, then the children. For each child, the persisted resize state of its parent is loaded -
correct by then, since the first pass has already applied the parent's own `end_resize`, if
any - and:

- if the parent has finished resizing, the child's entries are applied normally,
- if it has not, they must not be applied at all, since more entries may still arrive in the
  parent and would have to go first. They are only rewritten to the new commitlog, to get
  fresh `rp_handle`s, and left in the Raft log to be applied once the parent is sealed. The
  snapshot index is not advanced, which is correct because such a group had not applied
  anything before the restart either. The persisted commit index of such a group is dropped
  as well: its Raft server would otherwise not finish starting until everything it believes
  to be committed has been applied, which cannot happen until the parent is sealed, while the
  sealing itself needs the server to be up. The index is re-established from the leader, or
  recomputed by the next one from a quorum, once the group is running again.
