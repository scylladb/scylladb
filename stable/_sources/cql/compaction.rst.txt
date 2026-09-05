
.. _compaction:

Compaction 
----------


This document describes the compaction strategy options available when creating a table. For more information about creating a table in ScyllaDB, refer to the :ref:`CQL Reference <create-table-statement>`.

By default, ScyllaDB starts a compaction task whenever a new SSTable is written. Compaction merges several SSTables into a new SSTable, which contains only the live data from the input SSTables. Merging several sorted files to get a sorted result is an efficient process, and this is the main reason why SSTables are kept sorted. 

The following compaction strategies are supported by ScyllaDB: 

* Size-tiered Compaction Strategy (`STCS`_)

* Leveled Compaction Strategy (`LCS`_)

* Incremental Compaction Strategy (`ICS`_)

* Time-window Compaction Strategy (`TWCS`_)

This page concentrates on the parameters to use when creating a table with a compaction strategy. If you are unsure which strategy to use or want general information on the compaction strategies which are available to ScyllaDB, refer to :doc:`Compaction Strategies </architecture/compaction/compaction-strategies>`.

Common options
^^^^^^^^^^^^^^
The following options are available for all compaction strategies. 

.. code-block:: cql

   compaction = { 
     'class' : 'compaction_strategy_name', 
     'enabled' : (true | false),
     'tombstone_threshold' : ratio,
     'tombstone_compaction_interval' : sec,
     'unchecked_tombstone_compaction' : (true | false)}



``class`` (default: SizeTieredCompactionStrategy)
   Selects the compaction strategy. 

   It can be one of the following. If you are unsure which one to choose, refer to :doc:`Compaction Strategies </architecture/compaction/compaction-strategies>` :

   * SizeTieredCompactionStrategy
   * TimeWindowCompactionStrategy
   * LeveledCompactionStrategy


=====

``enabled`` (default: true)
   Runs background compaction (known as “minor compaction”). Can be one of the following:

   * true - runs minor compaction
   * false - disable minor compaction

=====

``tombstone_threshold`` (default: 0.2)
  The ratio (expressed as a decimal) of garbage-collectable tombstones compared to the data. When this threshold is exceeded on a specific table, a single SSTable compaction begins. Acceptable values are numbers in the range 0 -1. 

=====

``tombstone_compaction_interval`` (default: 86400s (1 day))
  *tombstone_compaction_interval* is lower-bound for when a new tombstone compaction can start. If an SSTable was compacted at a time *X*, the earliest time it will be considered for tombstone compaction again is *X + tombstone_compaction_interval*. This does not guarantee that sstables will be considered for compaction immediately after tombstone_compaction_interval time has elapsed after the last compaction.

=====

``unchecked_tombstone_compaction`` (default: false)
   If unchecked_tombstone_compaction is set to true, tombstone_threshold will be ignored, and an SSTable will be compacted according to tombstone_compaction_interval.

=====

.. _STCS:

Size Tiered Compaction Strategy (STCS)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
When using STCS, SSTables are put in different buckets depending on their size. When an SSTable is bucketed, the average size of the tables is compared to the new table as well as the high and low threshold levels. 

The database compares each SSTable size to the average of all SSTable sizes on the node. It calculates ``bucket_low * avg_bucket_size`` and ``bucket_high * avg_bucket_size`` and then compares the result with the average SSTable size. The conditions set for ``bucket_high`` and ``bucket_low`` dictate if successive tables will be added to the same bucket. When compaction begins it merges SSTables whose size in KB are within ``[average-size * bucket_low]`` and ``[average-size * bucket_high]``.

Once the ``min_threashold`` is reached, minor compaction begins. 

.. _stcs-options:

STCS options
~~~~~~~~~~~~

The following options only apply to SizeTieredCompactionStrategy:

.. code-block:: cql

   compaction = { 
     'class' : 'SizeTieredCompactionStrategy', 
     'bucket_high' : factor,
     'bucket_low' : factor, 
     'min_sstable_size' : int,
     'min_threshold' : num_sstables,
     'max_threshold' : num_sstables}

``bucket_high`` (default: 1.5)
   A new SSTable is added to the bucket if the SSTable size is less than bucket_high * the average size of that bucket (and if the bucket_low condition also holds). 
   
   For example, if **'bucket_high = 1.5'** and the **SSTable size = 14MB**, does it belong to a bucket with an average size of 10MB? 

   Yes, because the **SSTable size = 14`**, which is **less** than **'bucket_high' * average bucket size = 15**. 

   So, the SSTable will be added to the bucket, and the bucket’s average size will be recalculated. 

=====

``bucket_low`` (default: 0.5)
   A new SSTable is added to the bucket if the SSTable size is more than bucket_low* the average size of that bucket (and if the bucket_high condition also holds). 

   For example, if **'bucket_high = 0.5'** and the **SSTable size is 10MB**, does it belong to a bucket with an average size is 15MB? 

   Yes, because the **SSTable size = 10** which is **more** than **'bucket_low' * average bucket size = 7.5**.

   So, the SSTable will be added to the bucket, and the bucket’s average size will be recalculated.

=====

``min_sstable_size`` (default: 52,428,800)
   All SSTables smaller than this number of bytes are put into the same bucket.

=====

``min_threshold`` (default: 4)
   Minimum number of SSTables that need to belong to the same size bucket before compaction is triggered on that bucket.
   If your SSTables are small, use *min_sstable_size* to define a size threshold (in bytes) below which all SSTables belong to one unique bucket.

   .. note:: Enforcement of ``min_threshold`` is controlled by the ``compaction_enforce_min_threshold`` configuration option in the scylla.yaml configuration settings.
      By default, ``compaction_enforce_min_threshold: false``, meaning the Size-Tiered Compaction Strategy will compact any bucket containing 2 or more SSTables.
      Otherwise, if ``compaction_enforce_min_threshold: true``, the value of ``min_threshold`` is considered and only those buckets that contain at
      least ``min_threshold`` SSTables will be compacted.

=====

``max_threshold`` (default: 32)
   Maximum number of SSTables that will be compacted together in one compaction step.



.. _LCS:

Leveled Compaction Strategy (LCS)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The compaction class LeveledCompactionStrategy (LCS) creates SSTables of a fixed, relatively small size (160 MB by default) that are grouped into levels. Within each level, SSTables are guaranteed to be non-overlapping. Each level (L0, L1, L2 and so on) is ten times as large as the previous level.

.. _lcs-options:

LCS options
~~~~~~~~~~~

.. code-block:: cql

   compaction = { 
     'class' : 'LeveledCompactionStrategy', 
     'sstable_size_in_mb' : int}

``sstable_size_in_mb`` (default: 160)
   This is the target size in megabytes, that will be used as the goal for an SSTable size following a compression. 
   Although SSTable sizes should be less or equal to sstable_size_in_mb, it is possible that compaction could produce a larger SSTable during compaction. This occurs when data for a given partition key is exceptionally large.

=====

.. _ICS:

Incremental Compaction Strategy (ICS)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ICS is only available in ScyllaDB Enterprise. See the `ScyllaDB Enetrpise documentation <https://enterprise.docs.scylladb.com/stable/cql/compaction.html>`_ for details.

=====

.. _TWCS:

Time Window CompactionStrategy (TWCS)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The basic concept is that TimeWindowCompactionStrategy will create 1 SSTable per file for a given time window.

.. include:: /rst_include/warning-ttl-twcs.rst

.. _twcs-options:

TWCS options
~~~~~~~~~~~~

.. code-block:: cql

   compaction = { 
     'class' : 'TimeWindowCompactionStrategy', 
     'compaction_window_unit' : string,
     'compaction_window_size' : int,
     'expired_sstable_check_frequency_seconds' : int,
     'min_threshold' : num_sstables,
     'max_threshold' : num_sstables}

``compaction_window_unit`` (default: DAYS)
  A time unit used to determine the window size which can be one of the following:

  * ``'MINUTES'``

  * ``'HOURS'``

  * ``'DAYS'``

=====

``compaction_window_size`` (default: 1)
  The number of units which will make up a window.

=====

``expired_sstable_check_frequency_seconds`` (default: 600)
  Specifies (in seconds) how often ScyllaDB will check for fully expired SSTables, which can be immediately dropped.

=====

``min_threshold`` (default: 4)
  Minimum number of SSTables that need to belong to the same size bucket before compaction is triggered on that bucket. 

=====

``max_threshold`` (default: 32)
  Maximum number of SSTables that will be compacted together in one compaction step.

=====

See Also
^^^^^^^^^

* :doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`

* :doc:`Compaction Strategies </architecture/compaction/compaction-strategies/>`

* :doc:`Compaction Overview </kb/compaction>`

.. include:: /rst_include/apache-copyrights.rst


.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none
