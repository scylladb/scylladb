============================
Choose a Compaction Strategy
============================


ScyllaDB implements the following compaction strategies in order to reduce :term:`read amplification<Read Amplification>`, :term:`write amplification<Write Amplification>`, and :term:`space amplification<Space Amplification>`, which causes bottlenecks and poor performance. These strategies include:

* `Leveled compaction strategy (LCS)`_ - the system uses small, fixed-size (by default 160 MB) SSTables distributed across different levels.
* `Incremental Compaction Strategy (ICS)`_ - triggered when the system has enough (four by default) similarly sized SSTables; breaks huge SStables into SSTable runs, which are comprised of a sorted set of smaller (1 GB by default), non-overlapping SSTables. 
* `Time-window compaction strategy (TWCS)`_ - designed for time series data.

This document covers how to choose a compaction strategy and presents the benefits and disadvantages of each one. If you want more information on compaction in general or on any of these strategies, refer to the :doc:`Compaction Overview </kb/compaction>`. If you want an explanation of the CQL commands used to create a compaction strategy, refer to :doc:`Compaction CQL Reference </cql/compaction>` .

Learn more in the `Compaction Strategies lesson <https://university.scylladb.com/courses/scylla-operations/lessons/compaction-strategies/>`_ on ScyllaDB University

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

With ICS, SSTables of approximately the same size are merged.
Icreasingly larger SSTables are replaced in each tier by increasingly longer
SSTable runs, modeled after LCS runs, but using larger fragment size of 1 GB
(by default).

Compaction is triggered when there are two or more runs of roughly the same
size. These runs are incrementally compacted with each other, producing a new
SSTable run, while incrementally releasing space as soon as each SSTable in
the input run is processed and compacted. 

Incremental Compaction Strategy benefits
----------------------------------------

* It's a popular strategy for LSM workloads.  It results in a low and
  logarithmic (in size of data) number of SSTables, and the same data is copied
  during compaction a fairly low number of times. Use the table in
  `Which strategy is best`_ to determine if this is the right strategy for your
  needs. 
* The space requirement for a major compaction with ICS is almost non-existent
  given that the operation can release fragments at roughly same rate it
  produces new ones.

If you look at the following screenshot the green line shows how disk usage behaves under ICS when major compaction is issued.

.. image:: /architecture/compaction/screenshot.png

Incremental Compaction Strategy disadvantages
----------------------------------------------

* Continuously modifying existing rows results in each row being split across several SSTables, making reads slow, which doesn’t happen in Leveled compaction.
* Obsolete data (overwritten or deleted columns) may accumulate across tiers, wasting space, for a long time, until it is finally merged. This can be mitigated by running major compaction from time to time.

**To implement this strategy**

Set the parameters for :ref:`Incremental Compaction <incremental-compaction-strategy-ics>`.

For more information, see the :ref:`Compaction KB Article <incremental-compaction-strategy-ics>`.

.. _TWCS1:

Time-window Compaction Strategy (TWCS)
======================================

Time-Window Compaction Strategy compacts SSTables within each time window using `Incremental Compaction Strategy (ICS)`_.
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

The table presents which workload works best with which compaction strategy.

.. list-table::
   :widths: 20 20 20 20 20
   :header-rows: 1

   * - Workload/Compaction Strategy 
     - Leveled
     - Incremental
     - Time-Window 
     - Comments
   * - Write-only
     - |x|  
     - |v|
     - |x|
     - [1]_
   * - :abbr:`Overwrite (Same data cells overwritten many times)`
     - |x|
     - |v|
     - |x| 
     - [2]_ and [3]_
   * - Read-mostly, with few updates
     - |v|
     - |x| 
     - |x|
     - 
   * - Read-mostly, with many updates 
     - |x|  
     - |v|
     - |x|
     - [4]_ 
   * - Time Series 
     - |x|
     - |x|
     - |v|
     - [5]_ and [6]_


The comments below describe the type of amplification each compaction strategy create on each use case, using the following abbreviations:

* SA - Size Amplification
* WA - Write Amplification
* RA - Read Amplification

.. _1: 

:sup:`1` When using Leveled Compaction with write only loads you will experience high Write Amplification - :abbr:`WA (Write Amplification)`

.. _2: 

:sup:`2` When Incremental with Overwrite loads, :abbr:`SA (Size Amplification)` occurs 

.. _3:

:sup:`3` When using Leveled Compaction with overwrite loads, :abbr:`WA (Write Amplification)` occurs

.. _4:

:sup:`4` When using Leveled with mostly read loads with many updates, :abbr:`WA (Write Amplification)` occurs in excess 

.. _5:

:sup:`5` When using Incremental with Time Series workloads, :abbr:`SA (Size Amplification)`, :abbr:`RA (Read Amplification)`, and  :abbr:`WA (Write Amplification)` occurs.

.. _6:

:sup:`6` When using Leveled with Time Series workloads, :abbr:`SA (Size Amplification)` and  :abbr:`WA (Write Amplification)` occurs.

References
----------
* :doc:`Compaction Overview </kb/compaction>` - contains in depth information on all of the strategies
* :doc:`Compaction CQL Reference </cql/compaction>` - covers the CQL parameters used for implementing compaction
* ScyllaDB Summit Tech Talk: `How to Ruin Performance by Choosing the Wrong Compaction Strategy <https://www.scylladb.com/tech-talk/ruin-performance-choosing-wrong-compaction-strategy-scylla-summit-2017/>`_


