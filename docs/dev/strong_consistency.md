# Introduction

This document describes the implementation details and design choices for the
strongly-consistent tables feature

The feature is heavily based on the existing implementation of Raft in scylla, which
is described in [docs/dev/raft-in-scylla.md](raft-in-scylla.md).

# Raft metadata persistence

## Group0 persistence context

The Raft groups for strongly consistent tables differ from Raft group0 particularly
in the extend of where their Raft group members can be located. For group0, all
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

## Strongly consistent table persistence

We introduce a separate set of Raft system tables for strongly consistent tablets:

- `system.raft_groups`
- `system.raft_groups_snapshots`
- `system.raft_groups_snapshot_config`

These tables mirror the logical contents of the existing `system.raft`, `system.raft_snapshots`,
`system.raft_snapshot_config` tables, but their partition key is a composite `(shard, group_id)`
rather than just `group_id`.

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
