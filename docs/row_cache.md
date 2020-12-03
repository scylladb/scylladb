# Row Cache

## Introduction

This document assumes familiarity with the mutation model and [MVCC](https://github.com/scylladb/scylla/blob/master/partition_version.hh).

Cache is always paired with its underlying mutation source which it mirrors. That means that from the outside it appears as containing the same set of writes. Internally, it keeps a subset of data in memory, together with information about which parts are missing. Elements which are fully represented are called "complete". Complete ranges of elements are called "continuous".

## Eviction

Eviction is about removing parts of the data from memory and recording the fact that information about those parts is missing. Eviction doesn't change the set of writes represented by cache as part of its `mutation_source` interface.

The smallest object which can be evicted, called eviction unit, is currently a single row (`rows_entry`). Eviction units are linked in an LRU owned by a `cache_tracker`. The LRU determines eviction order. The LRU is shared among many tables. Currently, there is one per `database`.

All `rows_entry` objects which are owned by a `cache_tracker` are assumed to be either contained in a cache (in some `row_cache::partitions_type`) or
be owned by a (detached) `partition_snapshot`. When the last row from a `partition_entry` is evicted, the containing `cache_entry` is evicted from the cache.

We never evict individual `partition_version` objects independently of the containing `partition_entry`. When the latest version becomes fully evicted, we evict the whole `partition_entry` together with all unreferenced versions. Snapshots become detached. `partition_snapshots` go away on their own, but eviction can make them contain no rows. Snapshots can undergo eviction even after they were detached from its original `partition_entry`.

The static row is not evictable, it goes away together with the partition. Partition reads which only read from the static row keep it alive by touching the last dummy `rows_entry`.

Every `partition_version` has a dummy entry after all rows (`position_in_partition::after_all_clustering_rows()`) so that the partition can be tracked in the LRU even if it doesn't have any rows and so that it can be marked as fully discontinuous when all of its rows get evicted.

`rows_entry` objects in memtables are not owned by a `cache_tracker`, they are not evictable. Data referenced by `partition_snapshots` created on non-evictable partition entries is not transferred to cache, so unevictable snapshots are not made evictable.

### Maintaining snapshot consistency on eviction

When removing a `rows_entry` (=r1), we need to record the fact that the range to which this row belongs is now discontinuous. For a single `mutation_partition` that would be done by going to the successor of r1 (=r2) and setting its `continuous` flag to `false`, which would indicate that the range between r1's predecessor and r2 is incomplete. With many partition versions, in order for the snapshot's logical `mutation_partition` to remain correct, special constraints on version contents and merging rules must apply as described below.

When `rows_entry` is selected for eviction, we only have a reference to that object. We want to be able to evict it without having to update entries in other versions, to avoid associated overheads of locating sibling versions and lookups inside them. To support that, each `partition_version` has its own continuity, fully specified in that version, independent of continuity of other versions. Row continuity of the snapshot is a union of continuous sets from all versions. This is the **independent-continuity** rule.

We also need continuous row intervals in different versions to be non-overlapping, except for points corresponding to complete rows. A row may overlap with another row, in which case the row from the later version completely overrides the one from the older version. A later version may have a row which falls into a continuous interval in the older version. A newer version cannot have a continuous interval with no rows which covers a row in the older version. This is the **no-overlap** rule. We make use of this rule to make calculating the union of intervals easier on version merge. Merging of newer version into older has time complexity of only O(size(new_version)) then. If we allowed for overlaps, we might have to walk over all entries in the old version to mark ranges between them as continuous.

Another rule is that row entries in **older versions are evicted first**, before any row in the newer version is evicted. This is needed so that we don't appear to loose writes in case we have the same row in more than one version, and the one from the newer version gets evicted first. To achieve this, we only move to the front of the LRU (marking as more recently used, evicted last) row entries which belong to the latest `partition_version` in a given `partition_entry`. This implies that detached snapshots never update the LRU.

The above rules have consequences for range population. When populating a discontinuous range which is adjacent to an existing row entry in an older version, we need to insert an entry for the bound (due to the independent-continuity rule), and need to satisfy the no-overlap rule in one of the following ways:
  1) copy complete row entry from older version into the latest version
  2) insert a dummy entry in the latest version for position before(key).
  3) add support for incomplete row entries, and insert an incomplete entry in the latest version

Options (3) and (2) are more efficient than (1), but (1) is the simplest to implement, so it was chosen. With option (2) we have an additional problem of cleaning up the extra dummy entries when the versions are finally merged. Option (3) makes cache reader more complicated.

Each `partition_version` always has a dummy entry at `position_in_partition::after_all_clustering_rows()`, so that its row range can be marked as fully discontinuous when all of its rows get evicted. Note that we can't remove fully evicted non-latest versions, because they may contain range tombstones and static row versions, which are needed to calculate snapshot's view on those elements. We can't merge them into newer versions in reclamation context due to no-allocation requirement, and because they could be referenced by snapshots.
