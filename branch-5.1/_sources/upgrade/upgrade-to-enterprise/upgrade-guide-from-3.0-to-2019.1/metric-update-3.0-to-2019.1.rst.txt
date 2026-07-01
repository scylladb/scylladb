=============================================================
Scylla Metric Update - Scylla 3.0 to Scylla Enterprise 2019.1
=============================================================


The following metrics are new in Scylla Enterprise 2019.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_database_paused_reads
* scylla_database_paused_reads_permit_based_evictions
* scylla_database_total_view_updates_failed_local
* scylla_database_total_view_updates_failed_remote
* scylla_database_total_view_updates_pushed_local
* scylla_database_total_view_updates_pushed_remote
* scylla_database_view_building_paused
* scylla_hints_for_views_manager_corrupted_files
* scylla_hints_for_views_manager_discarded
* scylla_hints_manager_corrupted_files
* scylla_hints_manager_discarded
* scylla_query_processor_queries
* scylla_reactor_aio_errors
* scylla_sstables_capped_local_deletion_time
* scylla_sstables_capped_tombstone_deletion_time
* scylla_sstables_cell_tombstone_writes
* scylla_sstables_cell_writes
* scylla_sstables_partition_reads
* scylla_sstables_partition_seeks
* scylla_sstables_partition_writes
* scylla_sstables_range_partition_reads
* scylla_sstables_range_tombstone_writes
* scylla_sstables_row_reads
* scylla_sstables_row_writes
* scylla_sstables_single_partition_reads
* scylla_sstables_sstable_partition_reads
* scylla_sstables_static_row_writes
* scylla_sstables_tombstone_writes
* scylla_storage_proxy_coordinator_last_mv_flow_control_delay

The following metrics names changes from Scylla 3.0 to Scylla Enterprise 2019.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 30
   :header-rows: 1
                 
   * - Scylla 3.0 Name
     - Scylla 2019.1 Name
   * - scylla_io_queue_commitlog_delay
     - scylla_io_queue_delay
   * - scylla_io_queue_commitlog_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_commitlog_shares
     - scylla_io_queue_shares
   * - scylla_io_queue_commitlog_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_commitlog_total_operations
     - scylla_io_queue_total_operations
   * - scylla_io_queue_compaction_delay
     - scylla_io_queue_delay
   * - scylla_io_queue_compaction_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_compaction_shares
     - scylla_io_queue_shares
   * - scylla_io_queue_compaction_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_compaction_total_operations
     - scylla_io_queue_total_operations
   * - scylla_io_queue_default_delay
     - scylla_io_queue_delay
   * - scylla_io_queue_default_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_default_shares
     - scylla_io_queue_shares
   * - scylla_io_queue_default_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_default_total_operations
     - scylla_io_queue_total_operations
   * - scylla_io_queue_memtable_flush_delay
     - scylla_io_queue_delay
   * - scylla_io_queue_memtable_flush_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_memtable_flush_shares
     - scylla_io_queue_shares
   * - scylla_io_queue_memtable_flush_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_memtable_flush_total_operations
     - scylla_io_queue_total_operations
