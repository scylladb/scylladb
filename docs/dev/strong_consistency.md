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
neutral name *resize* in the identifiers; merging is not implemented yet and is rejected
explicitly.

## Outline

```
topology coordinator                        replicas
────────────────────                        ────────
split decision emitted
  + child group ids  ──── group0 write ───► create the child groups, hold their
                                            appliers back, keep their leaders
                                            co-located with the parent's

  (tablets compact, the balancer waits)

finalization:
  global barrier
  seal the parent    ──── process_raft_resize ──► commit start_resize:
    on every replica                              writes go to the children
                                                  commit end_resize:
                                                  the parent's log is final,
                                                  the child appliers are released
  replace tablet map ──── group0 write ───► the child tablets take the child group
                                            ids as their own, the resize state is
                                            dropped
```

## The child group ids travel with the split decision

The children have to exist, and every replica has to agree on which groups they are, before
the split is carried out - otherwise there would be nowhere to send a write the parent has
stopped accepting. The ids are therefore generated when the split decision is emitted and
recorded in the tablet metadata (`locator::raft_resize_info`, persisted in the
`transition_raft_group_ids` column of `system.tablets`), so that both reach the replicas in the
same group0 write. They are cleared again if the decision is revoked. When the split is finally
applied, the new tablets take those ids as their own group ids rather than getting fresh ones.

The kind of the resize in progress is not recorded per tablet: it is the kind of the resize
decision the ids were emitted with, which is already in the tablet metadata (the static
`resize_type` column), so the two can never disagree.

A replica observing the resize info starts the child groups next to the parent, which keeps
serving reads and writes in the meantime.

## Sealing the parent

Sealing is driven from outside the group, by the topology coordinator, over the
`process_raft_resize` verb: the markers have to be committed in the parent's log, which only
its current leader can do, and every replica has to have applied the second one before the
tablet map is replaced. It commits two markers:

- `start_resize` - from here on the parent no longer accepts writes, and the coordinator
  hands the writes of its token range off to the child covering the token,
- `end_resize` - from here on the parent's log is final, and the appliers of the children
  are released.

The markers are ordinary entries in the parent's log, so they are replicated and applied in
log order like any write. They are *not* carried as mutations, though: each marker is recorded
as a static column of the group's own row in `system.raft_groups`, whose partition key contains
the shard hosting the group, and replicas may host the same group on different shards. A single
mutation could therefore not serve all of them, so `raft_command` is a variant
(`write_mutation`, `resize_marker`, `no_op`) and every replica builds its own mutation when it
applies the entry. Only the presence of a marker is ever read back; the kind of resize in
progress comes from the tablet metadata.

The coordinator drives every replica until it reports success, retrying: a failure only ever
means that the replica cannot make progress *yet* - it has not seen the new tablet metadata,
does not host the groups, an election is in progress, or the leaders are not co-located - and
all of those resolve on their own. Nodes being excluded from the cluster are skipped. Once any
replica has committed the markers the rest only have to be waited for, so they are driven in
`wait_only` mode; the last round is always a `wait_only` one, because committing a marker only
establishes that it is committed, not that it has been applied.

`service::strong_consistency::raft_resize_tracker` holds the per-shard state of every
resize a replica takes part in, tracks which markers have been applied, and owns the promise
the child appliers wait on. It is reloaded from `system.raft_groups` before a group's Raft
server is created, because after a restart the parent's committed entries are applied by
commitlog replay rather than by `state_machine::apply()`, so nothing would fulfil the promise
otherwise. It deliberately holds nothing but that state: it is started before `groups_manager`
so that the state machines and the commitlog replay can use it without the Raft servers being
up, which anything driving those servers would undo.

## Handing the token range over without a gap

Two things have to hold for the hand-over to be seamless.

**A write must either land in the parent's log or be handed off, with nothing in between.**
The coordinator checks the `start_resize` flag immediately before entering `add_entry()`,
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
`raft::server::stepdown()`. Until they are co-located the request bounces back to the
coordinator and is retried, since it has nowhere else to go. On top of that, a handed-off write
carries the timestamp it got from the parent, and the child's clock is advanced past it before
a new one is handed out.

## Reads on a child before the parent is done

A child group must not apply anything until the parent has applied everything it committed;
otherwise a read served by the child could observe a state older than a write already
committed in the parent. The child's applier therefore blocks on `end_resize`.

This also makes linearizable reads on a child wait, which they must: sealing puts a `no_op`
entry into each child before committing `start_resize`, so from that point a read barrier
in a child has an unapplied entry to wait for, and is released by the same thing which releases
the applier. Writes do not have to wait - a handed-off write is acknowledged as soon as the
child commits it, because it already carries a timestamp above every one the parent handed out
and cannot be applied out of order.

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
  anything before the restart either.
