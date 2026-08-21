ScyllaDB Large Partitions Table
================================

This document describes how to work with ScyllaDB's large partitions table.
The large partitions table can be used to trace large partitions in a cluster.
The table is updated every time a partition is written and/or deleted,and includes a compaction process which flushes MemTables to SSTables.

Large Partitions can cause any of the following symptoms:

* Longer latencies on a single shard (look at the "ScyllaDB Overview Metrics" dashboard of `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_).
* Oversized allocation warning messages in the log (e.g. ``seastar_memory - oversized allocation: 2842624 bytes, please report``)

If you are experiencing any of the above, search to see if you have large partitions. 

Note that large partitions are detected only when they are stored in a single SSTable.
ScyllaDB does not account for data belonging to the same logical partition, but spread across multiple SSTables, as long as any single partition in each SSTable does not cross the large partitions warning threshold.
However, note that over time, compaction, and Size-Tiered Compaction Strategy in particular, may collect the dispersed partition data from several SSTables and store it in a single SSTable, thus crossing the large partitions threshold.

Viewing - Find Large Partitions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Search for large partitions.

For example:

.. code-block:: console

   SELECT * FROM system.large_partitions;

Example output:

.. code-block:: console

   keyspace_name | table_name | sstable_name     | partition_size | partition_key                                       | compaction_time     | dead_rows | range_tombstones | rows
   --------------+------------+------------------+----------------+-----------------------------------------------------+---------------------+-----------+------------------+--------
          demodb |       tmcr | md-6-big-Data.db |     1188716932 | {key: pk{000400000001}, token:-4069959284402364209} | 2018-07-23 08:10:34 |         0 |                0 |    100
          testdb |       tmcr | md-7-big-Data.db |        1234567 | {key: pk{000400000001}, token:-3169959284402457813} | 2018-07-23 08:10:34 |         0 |                0 | 100101
  
================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
keyspace_name                                     The name of a keyspace holding the large partition
------------------------------------------------  ---------------------------------------------------------------------------------
table_name                                        The name of a table containing the large partition
------------------------------------------------  ---------------------------------------------------------------------------------
sstable_name                                      The name of SSTable containing the large partition
------------------------------------------------  ---------------------------------------------------------------------------------
partition_size                                    The size of the partition in this sstable
------------------------------------------------  ---------------------------------------------------------------------------------
partition_key                                     The value of a partition key that identifies the large partition
------------------------------------------------  ---------------------------------------------------------------------------------
dead_rows                                         The number of dead rows in the partition in this sstable
------------------------------------------------  ---------------------------------------------------------------------------------
range_tombstones                                  The number of range tombstones in the partition in this sstable
------------------------------------------------  ---------------------------------------------------------------------------------
rows                                              The number of rows including range tombstones in the partition in this sstable
------------------------------------------------  ---------------------------------------------------------------------------------
compaction_time                                   Time when compaction last occurred
================================================  =================================================================================

.. note::

   The large partitions table is **local** per node. Querying the table returns results for the specific node you query.


* Search within all the large partitions for a specific keyspace and or table.
 
For example we are looking for the keyspace ``demodb`` and table ``tmcr``:

.. code-block:: console

   SELECT * FROM system.large_partitions WHERE keyspace_name = 'demodb' AND table_name = 'tmcr';

Example output:

.. code-block:: console

   keyspace_name | table_name | sstable_name     | partition_size | partition_key                                       | compaction_time     | dead_rows | range_tombstones | rows
   --------------+------------+------------------+----------------+-----------------------------------------------------+---------------------+-----------+------------------+------
          demodb |       tmcr | md-6-big-Data.db |     1188716932 | {key: pk{000400000001}, token:-4069959284402364209} | 2018-07-23 08:10:34 |         0 |                0 | 1942


.. _large-partition-table-configure:

Configure
^^^^^^^^^

Configure the detection thresholds of large partitions with the ``compaction_large_partition_warning_threshold_mb`` parameter (default: 1000MB)
and the ``compaction_rows_count_warning_threshold`` parameter (default 100000)
in the scylla.yaml configuration file.
Partitions that are bigger than the size threshold and/or hold more than the rows count threshold are reported in the ``system.large_partitions`` table and generate a warning in the ScyllaDB log (refer to :doc:`log </getting-started/logging/>`).

For example (set to 500MB / 50000, respectively):

.. code-block:: console

   compaction_large_partition_warning_threshold_mb: 500
   compaction_rows_count_warning_threshold: 50000


Storing
^^^^^^^

Large partitions are stored in a system table with the following schema:

.. code-block:: console

   DESCRIBE TABLE system.large_partitions;
   
   CREATE TABLE system.large_partitions (
       keyspace_name text,
       table_name text,
       sstable_name text,
       partition_size bigint,
       partition_key text,
       compaction_time timestamp,
       dead_rows bigint,
       range_tombstones bigint,
       rows bigint,
       PRIMARY KEY ((keyspace_name, table_name), sstable_name, partition_size, partition_key)
   ) WITH CLUSTERING ORDER BY (sstable_name ASC, partition_size DESC, partition_key ASC)




Expiring Data
^^^^^^^^^^^^^

In order to prevent stale data from appearing, all rows in ``system.large_partitions`` table are inserted with Time To Live (TTL) equal to 30 days.


.. _large-data-virtual-tables:

Virtual Tables (``LARGE_DATA_VIRTUAL_TABLES`` Feature)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Starting with ScyllaDB 2026.2, the ``system.large_partitions``,
``system.large_rows``, and ``system.large_cells`` tables are implemented as
**virtual tables** that read directly from metadata stored inside each SSTable
rather than from separate physical system tables.

How it works
""""""""""""

When an SSTable is written (during flush or compaction), the SSTable writer
records the top-N above-threshold large data entries into a
``LargeDataRecords`` component in the SSTable's ``.Scylla`` metadata file.
The virtual tables query this metadata from all live SSTables at read time,
so large data records automatically follow their SSTables when they are
migrated between shards or nodes (e.g. during tablet migration or repair).

The maximum number of records stored per large data type per SSTable is
controlled by the ``compaction_large_data_records_per_sstable`` configuration
option (default: 10).

Activation
""""""""""

The transition is gated by the ``LARGE_DATA_VIRTUAL_TABLES`` cluster feature
flag, which is automatically enabled once all nodes in the cluster are
upgraded to a version that supports it.

When the feature is enabled on a node:

1. The old physical ``system.large_partitions``, ``system.large_rows``, and
   ``system.large_cells`` tables are dropped.
2. Virtual table replacements are registered in their place.
3. New SSTable writes stop populating the old physical tables.

During a rolling upgrade, as long as any node in the cluster has not been
upgraded, the feature remains disabled and all nodes continue writing to
the old physical tables. This ensures safe rollback if the upgrade needs
to be reverted.

Upgrade considerations
""""""""""""""""""""""

After the ``LARGE_DATA_VIRTUAL_TABLES`` feature is enabled, the virtual tables
derive their content from ``LargeDataRecords`` metadata stored in SSTables.
SSTables written by older versions do not contain this metadata.  As a result,
**the virtual tables may appear empty until those older SSTables are rewritten**
by compaction.

To populate the virtual tables promptly after upgrade, run one of the following
on each node:

* **Upgrade SSTables compaction** (recommended -- rewrites SSTables without
  waiting for normal compaction triggers):

  .. code-block:: console

     nodetool upgradesstables --include-all-sstables

* **Major compaction** (rewrites all SSTables into a single set, but may
  temporarily increase disk usage):

  .. code-block:: console

     nodetool compact

Once the SSTables have been rewritten, the virtual tables will show the
current large data records.

.. include:: /troubleshooting/_common/ts-return.rst

Additional Resources
^^^^^^^^^^^^^^^^^^^^
:doc:`Large Partitions Hunting </troubleshooting/debugging-large-partition/>`
