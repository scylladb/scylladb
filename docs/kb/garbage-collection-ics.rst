==============================================
Efficient Tombstone Garbage Collection in ICS
==============================================

Background
------------

Accumulating droppable tombstones is a known problem in LSM-tree, which can severely impact read latency.

To avoid accumulating droppable tombstones, the compaction process must purge them in a timely manner. However, a droppable
tombstone cannot be purged unless it's compacted with the data it shadows. For example, let's assume that a droppable
tombstone sits on level 1, whereas its shadowed data is on level 2. You'll only be able to purge that tombstone once compaction
promotes it to level 2.

Garbage Collection Efficient in ICS
---------------------------------------------

The process inherits the cross-tier compaction idea from SAG, but instead of using a space-amplification-based trigger, 
it uses a tombstone-density trigger instead. It can co-exist with SAG, if enabled.

ICS picks a tier where any of the runs meet 
the tombstone density threshold and compacts that tier with the next bigger tier (or the next smaller one if the largest tier 
was picked), resulting in cross-tier compaction.

When the cross-tier approach is used, it’s a matter of time for the droppable tombstones to reach the data they shadow in the LSM tree.
As a result, ICS can promptly purge droppable tombstones.

To maintain overall compaction efficiency, GC is only considered when same-tier compactions have been completed, as efficiency 
is a function of # of files and their relative sizes. That’s also why ICS picks tiers closest in size for the cross-tier 
procedure.

How to Use It
---------------

ICS garbage collection is enabled by default.
You can use the compaction options ``tombstone_threshold`` and ``tombstone_compaction_interval`` to tweak the behavior 
of the GC process.

See the ScyllaDB documentation for a full description of :doc:`Compaction </cql/compaction>`.


