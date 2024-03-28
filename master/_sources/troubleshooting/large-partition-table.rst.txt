ScyllaDB Large Partitions Table
================================

This document describes how to work with Scylla's large partitions table.
The large partitions table can be used to trace large partitions in a cluster.
The table is updated every time a partition is written and/or deleted,and includes a compaction process which flushes MemTables to SSTables.

Large Partitions can cause any of the following symptoms:

* Longer latencies on a single shard (look at the "Scylla Overview Metrics" dashboard of `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_).
* Oversized allocation warning messages in the log (e.g. ``seastar_memory - oversized allocation: 2842624 bytes, please report``)

If you are experiencing any of the above, search to see if you have large partitions. 

Note that large partitions are detected only when they are stored in a single SSTable.
Scylla does not account for data belonging to the same logical partition, but spread across multiple SSTables, as long as any single partition in each SSTable does not cross the large partitions warning threshold.
However, note that over time, compaction, and Size-Tiered Compaction Strategy in particular, may collect the dispersed partition data from several SSTables and store it in a single SSTable, thus crossing the large partitions threshold.

Viewing - Find Large Partitions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Search for large partitions.

For example:

.. code-block:: console

   SELECT * FROM system.large_partitions;

Example output:

.. code-block:: console

   keyspace_name | table_name | sstable_name     | partition_size | partition_key                                       | rows   | compaction_time
   --------------+------------+------------------+----------------+-----------------------------------------------------+--------+--------------------------
          demodb |       tmcr | md-6-big-Data.db |     1188716932 | {key: pk{000400000001}, token:-4069959284402364209} |    100 | 2018-07-23 08:10:34
          testdb |       tmcr | md-7-big-Data.db |        1234567 | {key: pk{000400000001}, token:-3169959284402457813} | 100101 | 2018-07-23 08:10:34
  
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
rows                                              The number of rows in the partition in this sstable
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

   keyspace_name | table_name | sstable_name     | partition_size | partition_key                                       | rows | compaction_time
   --------------+------------+------------------+----------------+-----------------------------------------------------+------+--------------------------
          demodb |       tmcr | md-6-big-Data.db |     1188716932 | {key: pk{000400000001}, token:-4069959284402364209} | 1942 | 2018-07-23 08:10:34


.. _large-partition-table-configure:

Configure
^^^^^^^^^

Configure the detection thresholds of large partitions with the ``compaction_large_partition_warning_threshold_mb`` parameter (default: 1000MB)
and the ``compaction_rows_count_warning_threshold`` parameter (default 100000)
in the scylla.yaml configuration file.
Partitions that are bigger than the size threshold and/or hold more than the rows count threshold are reported in the ``system.large_partitions`` table and generate a warning in the Scylla log (refer to :doc:`log </getting-started/logging/>`).

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
       rows bigint,
       compaction_time timestamp,
       PRIMARY KEY ((keyspace_name, table_name), sstable_name, partition_size, partition_key)
   ) WITH CLUSTERING ORDER BY (sstable_name ASC, partition_size DESC, partition_key ASC)




Expiring Data
^^^^^^^^^^^^^

In order to prevent stale data from appearing, all rows in ``system.large_partitions`` table are inserted with Time To Live (TTL) equal to 30 days.

.. include:: /troubleshooting/_common/ts-return.rst

Additional Resources
^^^^^^^^^^^^^^^^^^^^
:doc:`Large Partitions Hunting </troubleshooting/debugging-large-partition/>`
