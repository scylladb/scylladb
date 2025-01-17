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

Size-tiered compaction inefficiently handles this problem with a process known as tombstone compaction. The process works as follows:

#. First, it searches for an SSTable with droppable tombstone density higher than N% (estimates density using tombstone histogram, which lives in Statistics.db).

     * The search starts from the highest level, as droppable tombstones in high levels are less likely to find shadowed data.
     * The threshold can be configured through the ``tombstone_threshold`` option (the default is 0.2). 
#. If it finds such an SSTable, it compacts that single file individually, hoping that the droppable tombstones can be purged.

     * The ``tombstone_compaction_interval`` option prevents the data in an SSTable from going through the procedure multiple times in a short interval.


The process above has several drawbacks; for example:

* It doesn't take into account out-of-order writes, so shadowed data may be sitting in lower levels rather than higher ones.
* It can take an unpredictable amount of time for a droppable tombstone to reach the shadowed data counterpart in the LSM tree.

Making Garbage Collection Efficient in ICS
---------------------------------------------

As a remedy to the known problem described above, a new process was introduced to ScyllaDB with `this commit <https://github.com/scylladb/scylladb/commit/c97325436237516fcec97eeb1f283674ea1fef1c>`_.
The process inherits the cross-tier compaction idea from SAG, but instead of using a space-amplification-based trigger, 
it uses a tombstone-density trigger instead. It can co-exist with SAG, if enabled.

The only similarity to STCS 'tombstone compaction' is the trigger, i.e., **when** to start the garbage collection procedure. The difference is **how** it is performed.

Instead of individually compacting an SSTable run (as it happens in STCS), ICS picks a tier where any of the runs meet 
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

As in STCS, you can use the compaction options ``tombstone_threshold`` and ``tombstone_compaction_interval`` to tweak the behavior 
of the GC process.

See the ScyllaDB documentation for a full description of :doc:`Compaction </cql/compaction>`.


