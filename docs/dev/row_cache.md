# Row Cache

## Introduction

This document assumes familiarity with the mutation model and [MVCC](mvcc.md).

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
