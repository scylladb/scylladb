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
snapshot descriptor — `snapshot_idx`, `snapshot_term`, `snapshot_config` and the
`truncations` history that replay needs. `snapshot_idx` *is* the commit index: a
segment's record is released only once every index it holds is committed, so what
the row says is committed by definition.

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
  form, shard component included — not over the `group_id` alone

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

Raft hands the io fiber a batch of entries to persist. The whole batch becomes one
commitlog entry:

```
Raft engine
    │
    ▼
raft_groups_storage::store_log_entries()
    │
    ▼
write_raft_batch()
    │
    │  serialize {group_id, commit_idx, entries} as one
    │  raft_commitlog_batch and add() it (force_sync = yes)
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
  single position. A batch that does not fit one commitlog entry is an internal error,
  not something to fragment — a copy of an entry has to live in exactly one segment.
- The batch header carries the group's `commit_idx` at write time. Replay uses it as a
  floor to decide which of the entries it reads are already committed.
- The group does not keep a handle per index. It keeps a queue of `segment_record`s,
  one per commitlog segment its entries landed in. A later batch in the same segment
  only advances that record's `max`, because retention is accounted per segment, not
  per position.
- Each record holds two references to its segment: `pin_table` under the target table,
  which is what the memtable pins are cloned from, and `pin_rg` under
  `system.raft_groups`, taken at record creation. That order matters: a segment holding
  only raft entries is `raft_groups`-dirty because of `pin_rg` alone, and the closed
  signal a release waits for arrives only in flush requests carrying that table's id.

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
    │  clone pin_table of the record covering N
    │  (another reference at the same position, no bytes written)
    │
    └──► handle → attached to memtable
         (keeps segment alive until flush)
```

This reuses the same mechanism normal mutations use to tie segment lifetime to
memtable flush — no new GC logic.

### Releasing a segment

A record is the unit of release. It can go once nothing more can be needed from it:

- every index it covers is committed (`max <= commit_idx`),
- every *command* it covers is applied (`last_cmd() <= apply_idx`) — dummies and
  configurations never reach `apply()`, so waiting for `max` would wedge the queue,
- and no further entry of this group can land in its segment. Either a later record
  exists in the queue, which means the commitlog already moved on to a newer segment,
  or the commitlog's flush rounds have reported this segment's position, which they do
  only for segments that stopped allocating.

Releasing writes the record's `(max, max_term())` and its newest configuration to
`system.raft_groups` as the group's snapshot descriptor, moving `pin_rg` into that
mutation. The segment then lives exactly until the `raft_groups` memtable flush that
makes the descriptor durable — which is the core invariant: a segment holding a
group's entries is reclaimed only once a durable descriptor covers them.

A leader change that discards an uncommitted tail cannot be expressed by the
descriptor alone, so it appends a `truncation_record {segment, from, to}` to the
`truncations` column. Replay needs it to tell a discarded copy from a live one. Records
are dropped from the column once their segment can never be replayed again.

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

Replay is a single pass. Nothing is collected and sorted afterwards; each batch is
processed as it is read, which is what bounds how much is buffered — at most a group's
uncommitted window, rather than everything the surviving segments hold.

```
raft batch for group G, carrying commit_idx in its header
    │
    ▼
first batch of this group: read its row from system.raft_groups
(snapshot_idx is the floor; snapshot_term, snapshot_config, truncations)
    │
    ▼
raise the floor to the header's commit_idx — it only ever advances —
and drain everything buffered at or below it: a header is written
only after the final copy of every index it covers, so what is below
the floor is final and can be applied now
    │
    ▼
drop_stale_copies(): drop the copies a truncation superseded. Each
truncation record of this segment is a cursor; the copy is claimed by
the oldest cursor *waiting for that index*, not simply by the oldest
one — truncations of one segment can reach back past each other
    │
    ▼
supersede: a surviving entry at index N replaces anything still
buffered at or above N, and what that dropped is written into the
group's own truncation records — otherwise a second replay of the
same segments would see those copies below the new floor and apply
an entry no leader committed
    │
    ├── entries at or below the floor (committed):
    │     apply to the memtable in-memory
    │     (they may not have been flushed before the crash)
    │
    └── entries above the floor (uncommitted):
          buffer as the group's tail
```

At the end of the pass, `finish_replay()` walks the groups and, for each one, writes
its recomputed row and then rewrites its buffered tail to the new commitlog as one
batch. The order matters within a group, not across them: the row goes through CQL,
and `system.raft_groups` carries `wait_for_sync_to_commitlog`, so the row is in the
commitlog before the write returns. A rewritten batch on disk therefore always has a
durable base row. The rewrite is also what gives the tail fresh references in the new
session, through the same `account_batch()` the write path uses.

A group that is no longer a replica of the tablet is discarded rather than recovered:
its entries belong to whoever holds the tablet now.

### Shutdown behavior

On clean shutdown a group is dropped with `log_disposition::keep`, and
`~raft_commitlog()` *detaches* its references: `rp_handle::release()` abandons the
decrement, so the segments survive the object and the uncommitted entries are still
there to replay.

`log_disposition::release` is the other case — a group that is genuinely gone, its
tablet migrated away or its table dropped, will never be replayed. `release_all()`
destroys the handles instead of detaching them, so the use counts drop and the
segments can be reclaimed (SCYLLADB-3827).
