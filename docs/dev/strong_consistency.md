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

`system.raft_groups` stores only non-log Raft metadata (term, vote, commit_idx) — unlike
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

# Leader placement

Reads and writes for a strongly consistent tablet go through the leader of its Raft
group, so which replica leads decides how many hops a request takes. A caller can name
the replicas it would rather see leading: the sections below bias elections towards the
best of them, bootstrap a fresh group on it, and move the leadership back once it has
ended up elsewhere.

## Preferred replicas

`fsm_config::get_priority_members` names the servers which should lead, best first. The
first of them which can vote in the current configuration is the preferred leader
(`fsm::preferred_leader()`): `fsm::reset_election_timeout()` gives it a reserved slot, one
tick after `ELECTION_TIMEOUT`, while every other server randomizes over the slots behind
it. The preferred leader therefore starts campaigning before anybody else and, alone in
its slot, cannot split the vote. It is a bias rather than a guarantee: election timers run
independently on each node, so a slow node can still be beaten to it.

Only the head of the list gets a slot. The rest of the list matters for the handover
described below.

Two things to keep in mind when building such a list:

- a server which cannot vote in the group is skipped rather than given the slot: it cannot
  win an election, so a slot for it would only delay the one which can;
- a server whose own clock is unusable while leases are on declines the slot, and the
  fast bootstrap below, and randomizes with everybody else: it could serve no lease
  reads, and the group would only hand the leadership away from it again;
- the list has to be a property of the group, identical on all its replicas. If two
  replicas disagree about its head they both take the reserved slot and split a vote,
  which costs an election round. If they disagree about the order, a leader may hand
  the leadership to a server which ranks it higher and hands it straight back; the
  handover below only ever moves the leadership up the list, so with one list there is
  no such cycle.

The callback is invoked whenever the election timer is armed and whenever a handover
target is asked for, so its answer may follow state which changes while the group runs.
In exchange it runs in the fsm's synchronous context and must neither block nor throw.

## The dedicated rack

`groups_manager::preferred_leaders()` is the callback for tablet groups. Today it has a
single rule: if the keyspace sets `dedicated_rack` (see the `consistency` option in the
CQL documentation), it names the tablet's replica in that rack, which is the cheapest one
to serve a strongly consistent read from. There is at most one, because a rack list places
one replica per rack. It is rebuilt on every call, so a migrated tablet or a changed
topology is picked up without restarting the group, and it names nobody for a group the
tablet map no longer describes - the stale side of a split or a merge - which it tells by
the group id. Keyspaces without a dedicated rack name nobody and keep ordinary Raft,
which spreads leadership over all the replicas.

## Bootstrapping a fresh group

A fresh group has no leader and no log, and waiting out an election timeout to get one is
pure delay, so `raft::server` starts a single member as a candidate right away
(`fsm_config::fast_bootstrap_seed`). That member is the preferred leader when the group
has one - bootstrapping the leadership anywhere else would only make the group hand it
over afterwards. Without a preference it is the voter at rank `seed % voters` in ascending
`server_id` order, and `groups_manager` derives the seed from the group id, so that a
table starting with many tablets spreads their initial leadership over its replicas
instead of electing the smallest-id node everywhere.

Note that a group is bootstrapped this way whenever its log is empty, which is also the
case for a replica whose entries have all been applied and which has taken no snapshot -
so such a replica may call an election on restart and take the leadership from a healthy
leader.

## Handing the leadership over

The slot steers elections, and a group holds an election only while it has no leader. Once
it has one, nothing moves the leadership on its own: a follower which can see a live leader
keeps resetting its election timer (`has_stable_leader()` in `fsm::tick()`) and never times
out. A group which elected a leader outside the dedicated rack - because the replica there
was down, behind, or just slower to campaign - would keep it for as long as it lives, since
a healthy leader is never re-elected.

Raft tells whom the leadership should go to, and the groups manager moves it. A leader
answers `server::preferred_handover_target()` with the first server on the list ahead of
it which can vote, which the failure detector considers alive and - while leases are in
use - whose clock works (`clock_ok` in its progress). Passing over the ones which are gone
matters once a caller names more than one server: the second choice should lead while the
first is down. A server whose clock does not work could hold no lease, so the group would
serve no local read from it - and LeaseGuard, which hands the leadership away from a
leader whose clock has failed, would give it straight back.

`groups_manager::leader_placement_fiber()` runs on every shard once the strongly
consistent tables feature is on, which on a fresh node happens after `start()`, during the
join. Every `leader_placement_interval`, stretched by a random part of
`leader_placement_jitter`, it
walks the groups the shard leads and transfers the leadership of each one which names a
target there, through `server::stepdown()` aimed at that target. Aiming it is what keeps
the handover from making things worse:

- nobody else is sent a `timeout_now`, so a group whose preferred server cannot take over
  keeps its current leader instead of passing it to a third one - which would find the
  same answer and pass it on again, leaving the group churning;
- a server which doesn't catch up before `leader_placement_timeout` is given up on, as in
  any other stepdown, and asked again at the next check.

Like any stepdown it holds the group's writes back while it waits for the target to catch
up - which is also what lets the target catch up while the group is busy. That is why both
the interval and the timeout are coarse, and why the check lives in the groups manager
rather than in the Raft state machine: a group led from the wrong place is a latency
problem, not a correctness one, and when to pay for moving it - how often, how many groups
at once, whether to wait for a quiet moment - is policy which may change without touching
Raft. The jitter also keeps the nodes which restarted together from handing over in
lockstep. The fiber is stopped before `stepdown_leaders()` hands everything away on
shutdown, since only one stepdown may be in progress per group.
