====================================================================
Scylla Enterprise Metric Update - Scylla Enterprise 2018.1 to 2019.1
====================================================================


New Metrics
~~~~~~~~~~~

The following metrics are new in 2019.1 compare to 2018.1

* scylla_alien_receive_batch_queue_length
* scylla_alien_total_received_messages
* scylla_alien_total_sent_messages
* scylla_cql_authorized_prepared_statements_cache_evictions
* scylla_cql_authorized_prepared_statements_cache_size
* scylla_cql_filtered_read_requests
* scylla_cql_filtered_rows_dropped_total
* scylla_cql_filtered_rows_matched_total
* scylla_cql_filtered_rows_read_total
* scylla_cql_rows_read
* scylla_cql_secondary_index_creates
* scylla_cql_secondary_index_drops
* scylla_cql_secondary_index_reads
* scylla_cql_secondary_index_rows_read
* scylla_cql_unpaged_select_queries
* scylla_cql_user_prepared_auth_cache_footprint
* scylla_database_dropped_view_updates
* scylla_database_large_partition_exceeding_threshold
* scylla_database_multishard_query_failed_reader_saves
* scylla_database_multishard_query_failed_reader_stops
* scylla_database_multishard_query_unpopped_bytes
* scylla_database_multishard_query_unpopped_fragments
* scylla_database_paused_reads
* scylla_database_paused_reads_permit_based_evictions
* scylla_database_total_view_updates_failed_local
* scylla_database_total_view_updates_failed_remote
* scylla_database_total_view_updates_pushed_local
* scylla_database_total_view_updates_pushed_remote
* scylla_database_view_building_paused
* scylla_database_view_update_backlog
* scylla_hints_for_views_manager_corrupted_files
* scylla_hints_for_views_manager_discarded
* scylla_hints_for_views_manager_dropped
* scylla_hints_for_views_manager_errors
* scylla_hints_for_views_manager_sent
* scylla_hints_for_views_manager_size_of_hints_in_progress
* scylla_hints_for_views_manager_written
* scylla_hints_manager_corrupted_files
* scylla_hints_manager_discarded
* scylla_hints_manager_dropped
* scylla_hints_manager_errors
* scylla_hints_manager_sent
* scylla_hints_manager_size_of_hints_in_progress
* scylla_hints_manager_written
* scylla_node_operation_mode
* scylla_query_processor_queries
* scylla_reactor_aio_errors
* scylla_reactor_cpu_steal_time_ms
* scylla_scheduler_time_spent_on_task_quota_violations_ms
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
* scylla_storage_proxy_coordinator_background_replica_writes_failed_local_node
* scylla_storage_proxy_coordinator_background_writes_failed
* scylla_storage_proxy_coordinator_last_mv_flow_control_delay
* scylla_storage_proxy_replica_cross_shard_ops
* scylla_transport_requests_blocked_memory_current
* scylla_io_queue_shares

Updated Metrics
~~~~~~~~~~~~~~~

The following metric names have changed between Scylla Enterprise 2018.1 and 2019.1

.. list-table::
   :widths: 30 30
   :header-rows: 1
                 
   * - Scylla 2018.1 Name
     - Scylla 2019.1 Name
   * - scylla_io_queue_compaction_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_compaction_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_compaction_total_operations
     - scylla_io_queue_total_operations
   * - scylla_io_queue_default_delay
     - scylla_io_queue_delay
   * - scylla_io_queue_default_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_default_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_default_total_operations
     - scylla_io_queue_total_operations
   * - scylla_io_queue_memtable_flush_delay
     - scylla_io_queue_delay
   * - scylla_io_queue_memtable_flush_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_memtable_flush_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_memtable_flush_total_operations
     - scylla_io_queue_total_operations
   * - scylla_io_queue_commitlog_delay
     - scylla_io_queue_delay
   * - scylla_io_queue_commitlog_queue_length
     - scylla_io_queue_queue_length
   * - scylla_io_queue_commitlog_total_bytes
     - scylla_io_queue_total_bytes
   * - scylla_io_queue_commitlog_total_operations
     - scylla_io_queue_total_operations
   * - scylla_io_queue_compaction_delay
     - scylla_io_queue_delay
   * - scylla_reactor_cpu_busy_ns
     - scylla_reactor_cpu_busy_ms
   * - scylla_storage_proxy_coordinator_current_throttled_writes
     - scylla_storage_proxy_coordinator_current_throttled_base_writes

Deprecated Metrics
~~~~~~~~~~~~~~~~~~

* scylla_database_cpu_flush_quota
* scylla_scollectd_latency
* scylla_scollectd_records
* scylla_scollectd_total_bytes_sent
* scylla_scollectd_total_requests
* scylla_scollectd_total_time_in_ms
* scylla_scollectd_total_values
* scylla_transport_unpaged_queries

