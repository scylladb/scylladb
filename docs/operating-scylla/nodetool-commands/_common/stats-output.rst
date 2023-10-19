
Example output:

.. code-block:: shell

   Keyspace: keyspace1
   Read Count: 0
   Read Latency: NaN ms.
   Write Count: 41656
   Write Latency: 0.016199467063568274 ms.
   Pending Flushes: 0
   Table: standard1
   SSTable count: 17
   SSTables in each level: [ ]
   Space used (live): 28331544
   Space used (total): 38060888
   Space used by snapshots (total): 0
   Off heap memory used (total): 5505072
   SSTable Compression Ratio: 0.0
   Number of partitions (estimate): 26829
   Memtable cell count: 4868
   Memtable data size: 4128083
   Memtable off heap memory used: 4980736
   Memtable switch count: 0
   Local read count: 0
   Local read latency: NaN ms
   Local write count: 41656
   Local write latency: 0.014 ms
   Pending flushes: 0
   Bloom filter false positives: 0
   Bloom filter false ratio: 0.00000
   Bloom filter space used: 0
   Bloom filter off heap memory used: 524336
   Index summary off heap memory used: 0
   Compression metadata off heap memory used: 0
   Compacted partition minimum bytes: 259
   Compacted partition maximum bytes: 310
   Compacted partition mean bytes: 310
   Average live cells per slice (last five minutes): 0.0
   Maximum live cells per slice (last five minutes): 0.0
   Average tombstones per slice (last five minutes): 0.0
   Maximum tombstones per slice (last five minutes): 0.0

================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
Keyspace                                          The keyspace name
------------------------------------------------  ---------------------------------------------------------------------------------
Read Count                                        The total number of reads count since last boot
------------------------------------------------  ---------------------------------------------------------------------------------
Read Latency                                      The average read latency
------------------------------------------------  ---------------------------------------------------------------------------------
Write Count                                       The total number of writes since last boot
------------------------------------------------  ---------------------------------------------------------------------------------
Write Latency                                     The average write latency
------------------------------------------------  ---------------------------------------------------------------------------------
Pending Flushes                                   Number of flushes that start but did not finished yet
------------------------------------------------  ---------------------------------------------------------------------------------
Table                                             table name
------------------------------------------------  ---------------------------------------------------------------------------------
SSTable count                                     The number of live sstable
------------------------------------------------  ---------------------------------------------------------------------------------
SSTables in each level                            In leveled compaction report the number of sstables and size per level
------------------------------------------------  ---------------------------------------------------------------------------------
Space used (live)                                 The disk space of the live sstables
------------------------------------------------  ---------------------------------------------------------------------------------
Space used (total)                                The disk space of all sstables
------------------------------------------------  ---------------------------------------------------------------------------------
Space used by snapshots (total)                   The space on disk used by snapshots
------------------------------------------------  ---------------------------------------------------------------------------------
Off heap memory used (total)                      The memory used by memtable, bloom filter, index summary and compression metadata
------------------------------------------------  ---------------------------------------------------------------------------------
SSTable Compression Ratio                         The ratio of the SSTable compression
------------------------------------------------  ---------------------------------------------------------------------------------
Number of keys (estimate)                         The estimated row count (based on the estimated histogram)
------------------------------------------------  ---------------------------------------------------------------------------------
Memtable cell count                               memtable column count
------------------------------------------------  ---------------------------------------------------------------------------------
Memtable data size                                Used space of lived memtable
------------------------------------------------  ---------------------------------------------------------------------------------
Memtable off heap memory used                     Memory taken by all memtable. In syclla, all memory is off heap
------------------------------------------------  ---------------------------------------------------------------------------------
Memtable switch count                             The number sstable was switched (flushed)
------------------------------------------------  ---------------------------------------------------------------------------------
Local read count                                  Number of read operations
------------------------------------------------  ---------------------------------------------------------------------------------
Local read latency                                Mean read operation time
------------------------------------------------  ---------------------------------------------------------------------------------
Local write count                                 Number of write operation
------------------------------------------------  ---------------------------------------------------------------------------------
Local write latency                               Mean write operation time
------------------------------------------------  ---------------------------------------------------------------------------------
Pending flushes                                   Number of sstables waiting to be flushed
------------------------------------------------  ---------------------------------------------------------------------------------
Bloom filter false positives                      Number of false positives made by the bloom filter
------------------------------------------------  ---------------------------------------------------------------------------------
Bloom filter false ratio                          The ratio between the false positive and overall attempts
------------------------------------------------  ---------------------------------------------------------------------------------
Bloom filter space used                           Memory taken by the bloom filter
------------------------------------------------  ---------------------------------------------------------------------------------
Bloom filter off heap memory used                 In scylla all memory is off heap
------------------------------------------------  ---------------------------------------------------------------------------------
Index summary off heap memory used                Memory taken by the Index summary
------------------------------------------------  ---------------------------------------------------------------------------------
Compression metadata off heap memory used         Off heap memory used for metadata compression
------------------------------------------------  ---------------------------------------------------------------------------------
Compacted partition minimum bytes                 Minimum bytes used for partition compression
------------------------------------------------  ---------------------------------------------------------------------------------
Compacted partition maximum bytes                 Maximum bytes used for partition compression
------------------------------------------------  ---------------------------------------------------------------------------------
Compacted partition mean bytes                    Mean bytes used for partition compression
------------------------------------------------  ---------------------------------------------------------------------------------
Average live cells per slice (last five minutes)  Average live cells per slice
------------------------------------------------  ---------------------------------------------------------------------------------
Maximum live cells per slice (last five minutes)  Maximum live cells per slice
------------------------------------------------  ---------------------------------------------------------------------------------
Average tombstones per slice (last five minutes)  Average tombstones per slice
------------------------------------------------  ---------------------------------------------------------------------------------
Maximum tombstones per slice (last five minutes)  Maximum tombstones per slice
================================================  =================================================================================

.. include:: nodetool-index.rst
