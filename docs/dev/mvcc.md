# MVCC

## Introduction

This document assumes familiarity with basic [MVCC](https://github.com/scylladb/scylla/blob/master/partition_version.hh) concepts:
snapshot, partition entry, partition version.

Partition versions in MVCC are represented with the `mutation_partition_v2` model. It differs from the `mutation_partition` model, which is used by transient mutation objects, because of different design requirements. The `mutation_partition_v2` model:

  - is optimized for low memory footprint

  - needs to support efficient incremental eviction

One of the differences between `mutation_partition_v2` and `mutation_partition` is how range tombstones are represented. In mutation_partition, range tombstones are kept in a separate data structure, a set of range_tombstone objects. Those objects are not easily evictable. It's difficult to keep them consistent with continuity information which is encoded in the rows tree. Every continuous interval has to have complete information about writes in that interval, so evicting a range tombstone would have to mark all affected intervals as discontinuous, which would be inefficient with mutation_partition. In `mutation_partition_v2`, range tombstone information is kept in the rows tree, and is directly linked with continuity information, which makes eviction of range tombstones automatic. See the "range tombstone representation" section for more details.

When `mutation_partition_v2` is part of an MVCC snapshot, extra constraints apply to its content. There are invariants which need to be preserved so that the snapshot which owns the version remains consistent. Those rules are described later in this document.

There are two kinds of MVCC snapshots: evictable and non-evictable. Any given partition entry is either evictable, or non-evictable, and produces only such snapshots. Evictable entries are present in cache and non-evictable entries are present in memtables. The rules concerning MVCC differ slightly between evictable and non-evictable snapshots. The differences are described in sections below where the rules diverge between the two.

## Evictable snapshots

When removing a `rows_entry` (=r1), we need to record the fact that the range to which this row belongs is now discontinuous. For a single `mutation_partition_v2` that would be done by going to the successor of r1 (=r2) and setting its `continuous` flag to `false`, which would indicate that the range between r1's predecessor and r2 is incomplete. With many partition versions, in order for the snapshot's logical `mutation_partition` to remain correct, special constraints on version contents and merging rules must apply as described below.

When `rows_entry` is selected for eviction, we only have a reference to that object. We want to be able to evict it without having to update entries in other versions, to avoid associated overheads of locating sibling versions and lookups inside them. To support that, each `partition_version` has its own continuity, fully specified in that version, independent of continuity of other versions. Row continuity of the snapshot is a union of continuous sets from all versions. This is the **independent-continuity** rule.

Continuous intervals in different versions can overlap. The rules for merging state are different for evictable and non-evictable snapshots. In evictable snapshots, if the interval in newer version is marked as continuous, it overwrites anything which is in older version in that interval. This includes information about range tombstone for the interval. All information in older version can be disregarded. This way, anything in the older version in that interval can be evicted without affecting logical value of the partition. We can do this by relying on the **information monotonicity** rule (see below).

In non-evictable snapshots, all intervals are continuous, and do not override information in older versions but are adding information instead. A range tombstone in a newer version in memtable will be merged with all overlapping range tombstones in older versions.

Another rule is **older versions are evicted first**. This rule means that for any clustering range R, information about writes in version V can only be removed after all information in versions older than V in range R is removed and the range is marked as discontinuous in those versions. This is needed so that we don't appear to lose writes. For example, suppose we have the same row in more than one version, and the one from the newer version would get evicted first. If the row is present in older version, the reader which uses the latest snapshots would pick up the old state of the row, which would be incorrect.

Also, when removing information from R, the range must be marked as discontinuous in older versions, otherwise the reader would assume that the range is complete (continuity of a snapshot is a union of continuity of each version) and would return no row.

To keep **older versions are evicted first**, we only move to the front of the LRU (marking as more recently used, evicted last) row entries which belong to the latest version. This way rows in the latest version will be evicted after rows in older versions are evicted. Removing information from the tail of versions (oldest) is safe due to **information monotonicity**.

### Last dummy entry

All partition versions in evictable snapshots have to have a dummy entry at position_in_partition::after_all_clustered_rows().

This is needed so that eviction can mark the range as discontinuous. Without this, evicting last entry would mark the range as continuous. That's the default continuity value for the key range after the last entry.

It is also needed so that versions can be tracked in the LRU even if they have no other row entries. They can still have tombstones and a static row.

### Information monotonicity

There is an invariant called **information monotonicity**, which applies only to evictable snapshots, which says that for any given continuous element (row, key interval) in a given version, it must reflect a set of writes which includes all writes contained in earlier versions. In other words, newer versions don't lose writes.

Compacting (without garbage-collecting) preserves information monotonicity, and can be performed locally within the version without looking at other versions. It's incorrect to garbage-collect tombstones from some version without applying it to all older versions first.

For example, a later version may contain just a row tombstone which covers all live cells in that row in older versions, because such row tombstone supersedes all earlier writes to that row. But it's not allowed for the later version to not contain any information for that row if there are older versions which contain writes to the row.

This rule allows version merging to discard information from middle versions by replacing it with information from a later version. This rule does not apply to non-evictable snapshots, and there such action would be incorrect. Without this rule, merging middle versions could resurrect an older version of a given partition element.

### Breaking continuity

It's safe to break continuity at any version, but only if there is no information about writes attached to it. For example, it's not safe to break continuity if there is range tombstone information attached to the interval (see the "Range tombstone representation" section). It's only safe to mark the interval as discontinuous while it has write information attached if this is the oldest version (in all snapshots). Doing otherwise would violate the "older versions are evicted first" rule, and could expose old information in the affected range leading to apparent loss of writes.

All versions in non-evictable snapshots have all intervals marked as continuous.

## Population

Versions can be concurrently merged and populated. Partition version merging must not assume that there is no external mutator.

Versions can be concurrently populated and read.

The **insertion only in latest** rule: Entries in MVCC versions can only be inserted into the latest version. Readers rely on this when refreshing their state. They assume that when iterators are not invalidated, no entries were inserted ahead of the cursor's position in older versions. If this is violated, it could lead to inconsistent cursor's view on the state of the snapshot.

Insertion of entries does not invalidate iterators.

Change of attributes of existing cursors always invalidates iterators. This happens on eviction, which may clear continuity of
a cursor. This can also happen after preempted version merging, when sentinel entries are inserted.

## Range tombstone representation

This section uses a simple notation for partition versions. Entries are represented like this:

```
   {key}
```

The key interval between entries can be marked as **continuous**:

```
   {key1} ==== {key2}
```

or **discontinuous**:

```
   {key1} ---- {key2}
```

An entry may have range tombstone information attached to it:

```
   {key, range_tombstone}
```

The `range_tombstone` object attached to the entry is a `tombstone` which represents deletion of all writes in the key range up to the entry's key (inclusive). It applies to the preceding interval if that interval is continuous, and only to the entry itself otherwise.

Below are several examples of how deletions of various key ranges are represented in `mutation_partition_v2`. In all examples, `x` and `y` are clustering keys.

```
  Deletion of (x, y] @ t:
  
      --- {x} === {y, t}
      
  [x, y] @ t:
  
      --- {before(x)} === {y, t}

  [x, y) @ t:
  
      --- {before(x)} === {before(y), t}

  [x] @ t:
  
      --- {x, t}

  [-inf, x] @ t:
  
      === {x, t}

  [x, +inf] @ t:

      --- {x, t} === {after_all_clustered_rows(), t}

  (x, y] @ t0 + (y, z] @ t1:
  
      --- {x} === {y, t0} === {z, t1}
```

range tombstone on a dummy entry which has its preceding interval discontinuous, carries no information about write deletion. It applies to an empty range so can be discarded:

```
   --- {before(x), t0} ==== {y, t1}
```

can be replaced with:

```
   --- {before(x)} ==== {y, t1}
```

### Range tombstones in MVCC

This section describes how information from individual partition versions of a snapshot is combined to produce the final single version which the snapshot represents. Those rules apply both to the actual process of merging versions in order to reduce the number of versions after they are no longer referenced, and to the way readers combine information (on-the-fly merging).

In non-evictable snapshots, the final range tombstone information for any given key interval is the sum of range tombstone information from all versions in that interval.

For example:

```
  v2: ============================= {5, t0} ===
  v1: ----------- {2} === {3, t2} =============
  v0: === {1, t1} =============================
```

has the following intervals:

```
   (-inf, 1), [1], (1, 2), [2], (2, 3), [3], (3, 5), [5], (5, +inf)
```

For the interval (2, 3), each version has the following range tombstone information:

```
  v2: t0
  v1: t2
  v0: null
```

The resulting range tombstone information is t0 + t2 + null = t2.

Merging of all intervals gives:

```
   === {1, t1} === {2, t0} === {3, t2} === {5, t0} ===
```

In evictable snapshots, the same rule also works, but we can rely on **information monotonicity**, and hence use a more relaxed rule. For any key interval, we take the write information from the latest interval that is continuous.

The **information monotonicity** rule has an impact on how evictable snapshots are populated. To preserve this rule,
when inserting an empty rows entry in the latest version which falls into a continuous range in the snapshot, we must set the range tombstone on that entry, even if the range to the left is marked as discontinuous.

### Single-row tombstones on top of older range tombstone

There is a rule called **no singular tombstones** which prohibits a configuration of mvcc versions such that in the newer version there is an entry with a range tombstone set but with discontinuous interval, and that entry falls into a continuous interval in some older version which has an older range tombstone in it.

For example:

```
    v1: -------- {3, t2} --------------
    v0: --- {1} ========= {7, t1} -----
```

In this case, merging v1 into v0 is problematic because we cannot just move the `{3, t2}` before `{7, t1}` and mark it continuous as that would incorrectly extend t2 until the preceding `{1}` entry. We cannot break continuity of `{7, t1}` in the range preceding `{3, t2}` because that would lose information about t1 in the key range `(1, 3)`. This could violate **information monotonicity** if there are older versions with information in this range, and in case of non-evictable snapshots we simply cannot break continuity (and thus lose information).

We could insert a dummy entry `{before(3), t1}` before `{3, t2}`. However, we chose to rather prohibit such configurations so that partition version merging code is simpler.

If **no singular tombstones** is preserved by the update path, it will hold as long as **older versions are evicted first** is also preserved. This way continuity will be broken only in the oldest version, where such action can't cause violation of **no singular-tombstones**.
