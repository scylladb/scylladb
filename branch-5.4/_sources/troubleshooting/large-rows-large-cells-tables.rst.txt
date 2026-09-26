ScyllaDB Large Rows and Large Cells Tables
===========================================

This document describes how to detect large rows and large cells in Scylla.
Scylla is not optimized for very large rows or large cells. They require allocation of large, contiguous memory areas and therefore may increase latency.
Rows may also grow over time. For example, many insert operations may add elements to the same collection, or a large blob can be inserted in a single operation.

Similar to the :doc:`large partitions table <large-partition-table>`, the large rows and large cells tables are updated when sstables are written or deleted, for example, on memtable flush or during compaction.

.. note:: The procedures described below require you to be using SSTables in :doc:`MC Format (SSTable 3.0) </architecture/sstable/sstable3/sstable-format>` or higher.

Find Large Rows
^^^^^^^^^^^^^^^^^^^^^^^^^

* Search for large rows.

For example:

.. code-block:: cql

   > SELECT * FROM system.large_rows;

      keyspace_name | table_name | sstable_name     | row_size | partition_key | clustering_key | compaction_time
      --------------+------------+------------------+----------+---------------+----------------+---------------------------------
      mykeyspace    |         gr | md-1-big-Data.db |  1206130 |             1 |              1 | 2019-06-14 13:03:24.039000+0000

  
================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
keyspace_name                                     The keyspace name that holds the large row
------------------------------------------------  ---------------------------------------------------------------------------------
table_name                                        The table name that holds the large row
------------------------------------------------  ---------------------------------------------------------------------------------
sstable_name                                      The SSTable name that holds the large row
------------------------------------------------  ---------------------------------------------------------------------------------
row_size                                          The size of the row in bytes
------------------------------------------------  ---------------------------------------------------------------------------------
clustering_key                                    The clustering key that holds the large row
------------------------------------------------  ---------------------------------------------------------------------------------
compaction_time                                   Time when compaction occurred
================================================  =================================================================================

* Search within all the large rows for a specific keyspace and or table.
 
For example we are looking for the keyspace ``demodb`` and table ``tmcr``:

.. code-block:: cql

   SELECT * FROM system.large_rows WHERE keyspace_name = 'demodb' AND table_name = 'tmcr;


Find Large Cells
^^^^^^^^^^^^^^^^

* Search for large cells.

For example:

.. code-block:: cql

    > SELECT * FROM system.large_cells;


    keyspace_name | table_name | sstable_name     | cell_size | partition_key | clustering_key | column_name | collection_elements | compaction_time                
    --------------+------------+------------------+-----------+---------------+----------------+-------------+---------------------+---------------------------------
    mykeyspace    |         gr | md-1-big-Data.db |   1206115 |             1 |              1 |        link |               17042 | 2019-06-14 13:03:24.034000+0000
   
  
================================================  =================================================================================
Parameter                                         Description
================================================  =================================================================================
keyspace_name                                     The keyspace name that holds the large cell
------------------------------------------------  ---------------------------------------------------------------------------------
table_name                                        The table name that holds the large cell
------------------------------------------------  ---------------------------------------------------------------------------------
sstable_name                                      The SSTable name that holds the large cell
------------------------------------------------  ---------------------------------------------------------------------------------
cell_size                                         The size of the cell, in bytes
------------------------------------------------  ---------------------------------------------------------------------------------
clustering_key                                    The clustering key of the row that holds the large cell
------------------------------------------------  ---------------------------------------------------------------------------------
column_name                                       The column of the large cell 
------------------------------------------------  ---------------------------------------------------------------------------------
collection_elements                               The number of elements in the large collection cell
------------------------------------------------  ---------------------------------------------------------------------------------
compaction_time                                   Time when compaction occurred
================================================  =================================================================================

* Search within all the large cells for a specific keyspace and or table.
 
For example we are looking for the keyspace ``demodb`` and table ``tmcr``:

.. code-block:: cql

   SELECT * FROM system.large_cells WHERE keyspace_name = 'demodb' AND table_name = 'tmcr;
          

Configure
^^^^^^^^^

Configure the detection threshold of large rows and large cells with the corresponding parameters in the ``scylla.yaml`` configuration file:

* ``compaction_large_row_warning_threshold_mb`` parameter (default: 10MB).
* ``compaction_large_cell_warning_threshold_mb`` parameter (default: 1MB).
* ``compaction_collection_elements_count_warning_threshold`` parameter (default: 10000).

Once the threshold is reached, the relevant information is captured in the ``system.large_rows`` / ``system.large_cells`` tables.
In addition,  a warning message is logged in the Scylla log (refer to :doc:`logging </getting-started/logging>`).


Storing
^^^^^^^
Large rows and large cells are stored in system tables with the following schemas:

.. code-block:: cql

  CREATE TABLE system.large_rows (
      keyspace_name text,
      table_name text,
      sstable_name text,
      row_size bigint,
      partition_key text,
      clustering_key text,
      compaction_time timestamp,
      PRIMARY KEY ((keyspace_name, table_name), sstable_name, row_size, partition_key, clustering_key)
  ) WITH CLUSTERING ORDER BY (sstable_name ASC, row_size DESC, partition_key ASC, clustering_key ASC)
      AND bloom_filter_fp_chance = 0.01
      AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
      AND comment = 'rows larger than specified threshold'
      AND compaction = {'class': 'SizeTieredCompactionStrategy'}
      AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
      AND crc_check_chance = 1.0
      AND dclocal_read_repair_chance = 0.1
      AND default_time_to_live = 0
      AND gc_grace_seconds = 0
      AND max_index_interval = 2048
      AND memtable_flush_period_in_ms = 0
      AND min_index_interval = 128
      AND read_repair_chance = 0.0
      AND speculative_retry = '99.0PERCENTILE';

  CREATE TABLE system.large_cells (
      keyspace_name text,
      table_name text,
      sstable_name text,
      cell_size bigint,
      partition_key text,
      clustering_key text,
      column_name text,
      collection_elements bigint,
      compaction_time timestamp,
      PRIMARY KEY ((keyspace_name, table_name), sstable_name, cell_size, partition_key, clustering_key, column_name)
  ) WITH CLUSTERING ORDER BY (sstable_name ASC, cell_size DESC, partition_key ASC, clustering_key ASC, column_name ASC)
      AND bloom_filter_fp_chance = 0.01
      AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
      AND comment = 'cells larger than specified threshold'
      AND compaction = {'class': 'SizeTieredCompactionStrategy'}
      AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
      AND crc_check_chance = 1.0
      AND dclocal_read_repair_chance = 0.1
      AND default_time_to_live = 0
      AND gc_grace_seconds = 0
      AND max_index_interval = 2048
      AND memtable_flush_period_in_ms = 0
      AND min_index_interval = 128
      AND read_repair_chance = 0.0
      AND speculative_retry = '99.0PERCENTILE';




Expiring Data
^^^^^^^^^^^^^
In order to prevent stale data from appearing, all rows in the ``system.large_rows`` and ``system.large_cells`` tables are inserted with Time To Live (TTL) equal to 30 days.

.. include:: /troubleshooting/_common/ts-return.rst
