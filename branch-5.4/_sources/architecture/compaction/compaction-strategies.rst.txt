============================
Choose a Compaction Strategy
============================


Scylla implements the following compaction strategies in order to reduce :term:`read amplification<Read Amplification>`, :term:`write amplification<Write Amplification>`, and :term:`space amplification<Space Amplification>`, which causes bottlenecks and poor performance. These strategies include:

* `Size-tiered compaction strategy (STCS)`_ - triggered when the system has enough (four by default) similarly sized SSTables.
* `Leveled compaction strategy (LCS)`_ - the system uses small, fixed-size (by default 160 MB) SSTables distributed across different levels.
* `Incremental Compaction Strategy (ICS)`_ - shares the same read and write amplification factors as STCS, but it fixes its 2x temporary space amplification issue by breaking huge sstables into SSTable runs, which are comprised of a sorted set of smaller (1 GB by default), non-overlapping SSTables. 
* `Time-window compaction strategy (TWCS)`_ - designed for time series data.

This document covers how to choose a compaction strategy and presents the benefits and disadvantages of each one. If you want more information on compaction in general or on any of these strategies, refer to the :doc:`Compaction Overview </kb/compaction>`. If you want an explanation of the CQL commands used to create a compaction strategy, refer to :doc:`Compaction CQL Reference </cql/compaction>` .

Learn more in the `Compaction Strategies lesson <https://university.scylladb.com/courses/scylla-operations/lessons/compaction-strategies/>`_ on Scylla University

.. _STCS1:

Size-tiered Compaction Strategy (STCS)
======================================

The premise of :term:`Size-tiered Compaction Strategy (STCS)<Size-tiered Compaction Strategy>` is to merge SSTables of approximately the same size. 

Size-tiered compaction benefits
-------------------------------
This is a popular strategy for LSM workloads.  It results in a low and logarithmic (in size of data) number of SSTables, and the same data is copied during compaction a fairly low number of times. Use the table in `Which strategy is best`_ to determine if this is the right strategy for your needs. 

Size-tiered compaction disadvantages
------------------------------------
This strategy has the following drawbacks (particularly with writes):

* Continuously modifying existing rows results in each row being split across several SSTables, making reads slow, which doesn’t happen in :ref:`Leveled compaction <LCS1>`.

* Obsolete data (overwritten or deleted columns) in a very large SSTable remains, wasting space, for a long time, until it is finally merged.  In overwrite-intensive loads for example, the overhead can be as much as 400%, as data will be duplicated 4X within a tier. On the other hand, the output SSTable will be the size of a single input SSTable. As a result, you will need 5X the amount of space (4 input SSTables plus one output SSTable), so 400% over the amount of data currently being stored. The allocated space will have to be checked and evaluated as your data set increases in size.

* Compaction requires a lot of temporary space as the new larger SSTable is written before the duplicates are purged. In the worst case up to half the disk space needs to be empty to allow this to happen.

**To implement this strategy**

Set the parameters for :ref:`Size-tiered compaction <size-tiered-compaction-strategy-stcs>`.

.. _LCS1:

Leveled Compaction Strategy (LCS)
=================================

:term:`Leveled Compaction Strategy<Leveled compaction strategy (LCS)>` (LCS) uses small, fixed-size (by default 160 MB) SSTables divided into different levels. Each level represents a run of a number of SSTables.

Leveled Compaction benefits
---------------------------
With the leveled compaction strategy, the following benefits are noteworthy:

* SSTable reads are efficient. The great number of small SSTables doesn’t mean we need to look up a key in that many SSTables, because we know the SSTables in each level have disjoint ranges, so we only need to look in one SSTable in each level. In the typical case, only one SSTable needs to be read.
* The other factors making this compaction strategy efficient are that at most 10% of space will be wasted by obsolete rows, and only enough space for ~10x the small SSTable size needs to be reserved for temporary use by compaction.

Use the table in `Which strategy is best`_ to determine if this is the right strategy for your needs. 

Leveled Compaction disadvantages
--------------------------------
The downside of this method is there is two times more I/O on writes, so it is not as good for workloads which focus on writing mostly new data.

Only one compaction operation on the same table can run at a time, so compaction may be postponed if there is a compaction already in progress. As the size of the files is not too large, this is not really an issue. 

**To implement this strategy**

Set the parameters for :ref:`Leveled Compaction <leveled-compaction-strategy-lcs>`.

.. _ICS1:

Incremental Compaction Strategy (ICS)
=====================================

ICS is only available in ScyllaDB Enterprise. See the `ScyllaDB Enetrpise documentation <https://enterprise.docs.scylladb.com/stable/architecture/compaction/compaction-strategies.html>`_ for details.

.. _TWCS1:

Time-window Compaction Strategy (TWCS)
======================================

Time-Window Compaction Strategy compacts SSTables within each time window using `Size-tiered Compaction Strategy (STCS)`_.
SSTables from different time windows are never compacted together. You set the :ref:`TimeWindowCompactionStrategy <time-window-compactionstrategy-twcs>` parameters when you create a table using a CQL command.

.. include:: /rst_include/warning-ttl-twcs.rst

Time-window Compaction benefits
-------------------------------

* Keeps entries according to a time range, making searches for data within a given range easy to do, resulting in better read performance.
* Allows you to expire an entire SSTable at once (using a TTL) as the data is already organized within a time frame.

Time-window Compaction deficits
-------------------------------

* Time-window compaction is **only** ideal for time-series workloads

**To implement this strategy**

Set the parameters for :ref:`Time-window Compaction <time-window-compactionstrategy-twcs>`.

Use the table in `Which strategy is best`_ to determine if this is the right strategy for your needs. 

.. _which-strategy-is-best:

Which strategy is best
======================

Every workload type may not work well with every compaction strategy. Unfortunately, the more mixed your workload, the harder it is to pick the correct strategy. This table presents what can be expected depending on the strategy you use for the workload indicated, allowing you to make a more informed decision. Keep in mind that the best choice for our testing may not be the best choice for your environment. You may have to experiment to find which strategy works best for you. 

.. _CSM1:

Compaction Strategy Matrix
--------------------------

The table presents which workload works best with which compaction strategy. In cases where you have the ability to use either STCS or ICS, always choose ICS.

.. list-table::
   :widths: 20 15 15 15 15 20
   :header-rows: 1

   * - Workload/Compaction Strategy
     - Size-tiered 
     - Leveled
     - Incremental
     - Time-Window 
     - Comments
   * - Write-only
     - |v|
     - |x|  
     - |v|
     - |x|
     - [1]_ and [2]_
   * - :abbr:`Overwrite (Same data cells overwritten many times)`
     - |v|
     - |x|
     - |v|
     - |x| 
     - [3]_ and [4]_
   * - Read-mostly, with few updates
     - |x| 
     - |v|
     - |x| 
     - |x|
     - [5]_
   * - Read-mostly, with many updates 
     - |v|
     - |x|  
     - |v|
     - |x|
     - [6]_ 
   * - Time Series 
     - |x|
     - |x|
     - |x|
     - |v|
     - [7]_ and [8]_


The comments below describe the type of amplification each compaction strategy create on each use case, using the following abbreviations:

* SA - Size Amplification
* WA - Write Amplification
* RA - Read Amplification

.. _1: 

:sup:`1` When using Size-tiered with write-only loads it will use approximately 2x peak space - :abbr:`SA (Size Amplification)` with Incremental, the SA is much less

.. _2: 

:sup:`2` When using Leveled Compaction with write only loads you will experience high Write Amplification - :abbr:`WA (Write Amplification)`

.. _3: 

:sup:`3` When using Size-tired or Incremental with Overwrite loads, :abbr:`SA (Size Amplification)` occurs 

.. _4:

:sup:`4` When using Leveled Compaction with overwrite loads, :abbr:`WA (Write Amplification)` occurs

.. _5:

:sup:`5` When using Size-tiered with mostly read loads with little updates, :abbr:`SA (Size Amplification)` and :abbr:`RA (Read Amplification)` occurs

.. _6:

:sup:`6` When using Leveled with mostly read loads with many updates, :abbr:`WA (Write Amplification)` occurs in excess 

.. _7:

:sup:`7` When using Size-tiered or Incremental with Time Series workloads, :abbr:`SA (Size Amplification)`, :abbr:`RA (Read Amplification)`, and  :abbr:`WA (Write Amplification)` occurs.

.. _8:

:sup:`8` When using Leveled with Time Series workloads, :abbr:`SA (Size Amplification)` and  :abbr:`WA (Write Amplification)` occurs.

References
----------
* :doc:`Compaction Overview </kb/compaction>` - contains in depth information on all of the strategies
* :doc:`Compaction CQL Reference </cql/compaction>` - covers the CQL parameters used for implementing compaction
* Scylla Summit Tech Talk: `How to Ruin Performance by Choosing the Wrong Compaction Strategy <https://www.scylladb.com/tech-talk/ruin-performance-choosing-wrong-compaction-strategy-scylla-summit-2017/>`_


