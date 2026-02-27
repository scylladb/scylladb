====================================================================
Scylla Enterprise Metric Update - Scylla Enterprise 2017.1 to 2018.1
====================================================================



Updated Metrics
~~~~~~~~~~~~~~~

The following metric names have changed between Scylla Enterprise 2017.1 and 2018.1

=========================================================================== ===========================================================================
2017.1                                                                      2018.1
=========================================================================== ===========================================================================
scylla_batchlog_manager_total_operations_total_write_replay_attempts        scylla_batchlog_manager_total_write_replay_attempts
scylla_cache_objects_partitions                                             scylla_cache_partitions
scylla_cache_total_operations_concurrent_misses_same_key                    scylla_cache_concurrent_misses_same_key
scylla_cache_total_operations_evictions                                     scylla_cache_partition_evictions
scylla_cache_total_operations_hits                                          scylla_cache_partition_hits
scylla_cache_total_operations_insertions                                    scylla_cache_partition_insertions
scylla_cache_total_operations_merges                                        scylla_cache_partition_merges
scylla_cache_total_operations_misses                                        scylla_cache_partition_misses
scylla_cache_total_operations_removals                                      scylla_cache_partition_removals
scylla_commitlog_memory_buffer_list_bytes                                   scylla_commitlog_memory_buffer_bytes
scylla_commitlog_memory_total_size                                          scylla_commitlog_disk_total_bytes
scylla_commitlog_queue_length_allocating_segments                           scylla_commitlog_allocating_segments
scylla_commitlog_queue_length_pending_allocations                           scylla_commitlog_pending_allocations
scylla_commitlog_queue_length_pending_flushes                               scylla_commitlog_pending_flushes
scylla_commitlog_queue_length_segments                                      scylla_commitlog_segments
scylla_commitlog_queue_length_unused_segments                               scylla_commitlog_unused_segments
scylla_commitlog_total_bytes_slack                                          scylla_commitlog_slack
scylla_commitlog_total_bytes_written                                        scylla_commitlog_bytes_written
scylla_commitlog_total_operations_alloc                                     scylla_commitlog_alloc
scylla_commitlog_total_operations_cycle                                     scylla_commitlog_cycle
scylla_commitlog_total_operations_flush                                     scylla_commitlog_flush
scylla_commitlog_total_operations_flush_limit_exceeded                      scylla_commitlog_flush_limit_exceeded
scylla_commitlog_total_operations_requests_blocked_memory                   scylla_commitlog_requests_blocked_memory
scylla_compaction_manager_objects_compactions                               scylla_compaction_manager_compactions
scylla_cql_total_operations_batches                                         scylla_cql_batches
scylla_cql_total_operations_deletes                                         scylla_cql_deletes
scylla_cql_total_operations_inserts                                         scylla_cql_inserts
scylla_cql_total_operations_reads                                           scylla_cql_reads
scylla_cql_total_operations_updates                                         scylla_cql_updates
scylla_database_bytes_total_result_memory                                   scylla_database_total_result_bytes
scylla_database_queue_length_active_reads                                   scylla_database_active_reads
scylla_database_queue_length_active_reads_streaming                         scylla_database_active_reads
scylla_database_queue_length_active_reads_system_keyspace                   scylla_database_active_reads
scylla_database_queue_length_queued_reads                                   scylla_database_queued_reads
scylla_database_queue_length_queued_reads_streaming                         scylla_database_queued_reads
scylla_database_queue_length_queued_reads_system_keyspace                   scylla_database_queued_reads
scylla_database_queue_length_requests_blocked_memory                        scylla_database_requests_blocked_memory_current
scylla_database_total_operations_clustering_filter_count                    scylla_database_clustering_filter_count
scylla_database_total_operations_clustering_filter_fast_path_count          scylla_database_clustering_filter_fast_path_count
scylla_database_total_operations_clustering_filter_sstables_checked         scylla_database_clustering_filter_sstables_checked
scylla_database_total_operations_clustering_filter_surviving_sstables       scylla_database_clustering_filter_surviving_sstables
scylla_database_total_operations_requests_blocked_memory                    scylla_database_requests_blocked_memory
scylla_database_total_operations_short_data_queries                         scylla_database_short_data_queries
scylla_database_total_operations_short_mutation_queries                     scylla_database_short_mutation_queries
scylla_database_total_operations_sstable_read_queue_overloads               scylla_database_sstable_read_queue_overloads
scylla_database_total_operations_total_reads                                scylla_database_total_reads
scylla_database_total_operations_total_reads_failed                         scylla_database_total_reads_failed
scylla_database_total_operations_total_writes                               scylla_database_total_writes
scylla_database_total_operations_total_writes_failed                        scylla_database_total_writes_failed
scylla_database_total_operations_total_writes_timedout                      scylla_database_total_writes_timedout
scylla_gossip_derive_heart_beat_version                                     scylla_gossip_heart_beat
scylla_http_0_connections_http_connections                                  scylla_httpd_connections_total
scylla_http_0_current_connections_current                                   scylla_httpd_connections_current
scylla_http_0_http_requests_served                                          scylla_httpd_requests_served
scylla_io_queue_delay_commitlog                                             scylla_io_queue_commitlog_delay
scylla_io_queue_delay_compaction                                            scylla_io_queue_compaction_delay
scylla_io_queue_delay_default                                               scylla_io_queue_default_delay
scylla_io_queue_delay_memtable_flush                                        scylla_io_queue_memtable_flush_delay
scylla_io_queue_derive_commitlog                                            scylla_io_queue_commitlog_total_bytes
scylla_io_queue_derive_compaction                                           scylla_io_queue_compaction_total_bytes
scylla_io_queue_derive_default                                              scylla_io_queue_default_total_bytes
scylla_io_queue_derive_memtable_flush                                       scylla_io_queue_memtable_flush_total_bytes
scylla_io_queue_queue_length_commitlog                                      scylla_io_queue_commitlog_queue_length
scylla_io_queue_queue_length_compaction                                     scylla_io_queue_compaction_queue_length
scylla_io_queue_queue_length_default                                        scylla_io_queue_default_queue_length
scylla_io_queue_queue_length_memtable_flush                                 scylla_io_queue_memtable_flush_queue_length
scylla_io_queue_total_operations_commitlog                                  scylla_io_queue_commitlog_total_operations
scylla_io_queue_total_operations_compaction                                 scylla_io_queue_compaction_total_operations
scylla_io_queue_total_operations_default                                    scylla_io_queue_default_total_operations
scylla_io_queue_total_operations_memtable_flush                             scylla_io_queue_memtable_flush_total_operations
scylla_lsa_bytes_free_space_in_zones                                        scylla_lsa_free_space_in_zones
scylla_lsa_bytes_large_objects_total_space                                  scylla_lsa_large_objects_total_space_bytes
scylla_lsa_bytes_non_lsa_used_space                                         scylla_lsa_non_lsa_used_space_bytes
scylla_lsa_bytes_small_objects_total_space                                  scylla_lsa_small_objects_total_space_bytes
scylla_lsa_bytes_small_objects_used_space                                   scylla_lsa_small_objects_used_space_bytes
scylla_lsa_bytes_total_space                                                scylla_lsa_total_space_bytes
scylla_lsa_bytes_used_space                                                 scylla_lsa_used_space_bytes
scylla_lsa_objects_zones                                                    scylla_lsa_zones
scylla_lsa_operations_segments_compacted                                    scylla_lsa_segments_compacted
scylla_lsa_operations_segments_migrated                                     scylla_lsa_segments_migrated
scylla_lsa_percent_occupancy                                                scylla_lsa_occupancy
scylla_memory_bytes_dirty                                                   scylla_memory_dirty_bytes
scylla_memory_bytes_regular_dirty                                           scylla_memory_regular_dirty_bytes
scylla_memory_bytes_regular_virtual_dirty                                   scylla_memory_regular_virtual_dirty_bytes
scylla_memory_bytes_streaming_dirty                                         scylla_memory_streaming_dirty_bytes
scylla_memory_bytes_streaming_virtual_dirty                                 scylla_memory_streaming_virtual_dirty_bytes
scylla_memory_bytes_system_dirty                                            scylla_memory_system_dirty_bytes
scylla_memory_bytes_system_virtual_dirty                                    scylla_memory_system_virtual_dirty_bytes
scylla_memory_bytes_virtual_dirty                                           scylla_memory_virtual_dirty_bytes
scylla_memory_memory_allocated_memory                                       scylla_memory_allocated_memory
scylla_memory_memory_free_memory                                            scylla_memory_free_memory
scylla_memory_memory_total_memory                                           scylla_memory_total_memory
scylla_memory_objects_malloc                                                scylla_memory_malloc_live_objects
scylla_memory_total_operations_cross_cpu_free                               scylla_memory_cross_cpu_free_operations
scylla_memory_total_operations_free                                         scylla_memory_free_operations
scylla_memory_total_operations_malloc                                       scylla_memory_malloc_operations
scylla_memory_total_operations_reclaims                                     scylla_memory_reclaims_operations
scylla_memtables_bytes_pending_flushes                                      scylla_memtables_pending_flushes
scylla_memtables_queue_length_pending_flushes                               scylla_memtables_pending_flushes_bytes
scylla_query_processor_total_operations_statements_prepared                 scylla_query_processor_statements_prepared
scylla_reactor_derive_aio_read_bytes                                        scylla_reactor_aio_bytes_read
scylla_reactor_derive_aio_write_bytes                                       scylla_reactor_aio_bytes_write
scylla_reactor_derive_busy_ns                                               scylla_reactor_cpu_busy_ns
scylla_reactor_derive_polls                                                 scylla_reactor_polls
scylla_reactor_gauge_load                                                   scylla_reactor_utilization
scylla_reactor_gauge_queued_io_requests                                     scylla_reactor_io_queue_requests
scylla_reactor_queue_length_tasks_pending                                   scylla_reactor_tasks_pending
scylla_reactor_queue_length_timers_pending                                  scylla_reactor_timers_pending
scylla_reactor_total_operations_aio_reads                                   scylla_reactor_aio_reads
scylla_reactor_total_operations_aio_writes                                  scylla_reactor_aio_writes
scylla_reactor_total_operations_cexceptions                                 scylla_reactor_cpp_exceptions
scylla_reactor_total_operations_fsyncs                                      scylla_reactor_fsyncs
scylla_reactor_total_operations_io_threaded_fallbacks                       scylla_reactor_io_threaded_fallbacks
scylla_reactor_total_operations_logging_failures                            scylla_reactor_logging_failures
scylla_reactor_total_operations_tasks_processed                             scylla_reactor_tasks_processed
scylla_storage_proxy_coordinator_background_reads                           scylla_storage_proxy_coordinator_background_read_repairs
scylla_storage_proxy_coordinator_completed_data_reads_local_node            scylla_storage_proxy_coordinator_completed_reads_local_node
scylla_storage_proxy_coordinator_data_read_errors_local_node                scylla_storage_proxy_coordinator_read_errors_local_node
scylla_storage_proxy_coordinator_data_reads_local_node                      scylla_storage_proxy_coordinator_reads_local_node
scylla_streaming_derive_total_incoming_bytes                                scylla_streaming_total_incoming_bytes
scylla_streaming_derive_total_outgoing_bytes                                scylla_streaming_total_outgoing_bytes
scylla_thrift_connections_thrift_connections                                scylla_thrift_current_connections
scylla_thrift_current_connections_current                                   scylla_thrift_thrift_connections
scylla_thrift_total_requests_served                                         scylla_thrift_served
scylla_tracing_keyspace_helper_total_operations_bad_column_family_errors    scylla_tracing_keyspace_helper_bad_column_family_errors
scylla_tracing_keyspace_helper_total_operations_tracing_errors              scylla_tracing_keyspace_helper_tracing_errors
scylla_tracing_queue_length_active_sessions                                 scylla_tracing_active_sessions
scylla_tracing_queue_length_cached_records                                  scylla_tracing_cached_records
scylla_tracing_queue_length_flushing_records                                scylla_tracing_flushing_records
scylla_tracing_queue_length_pending_for_write_records                       scylla_tracing_pending_for_write_records
scylla_tracing_total_operations_dropped_records                             scylla_tracing_dropped_records
scylla_tracing_total_operations_dropped_sessions                            scylla_tracing_dropped_sessions
scylla_tracing_total_operations_trace_errors                                scylla_tracing_trace_errors
scylla_tracing_total_operations_trace_records_count                         scylla_tracing_trace_records_count
scylla_transport_connections_cql_connections                                scylla_transport_cql_connections
scylla_transport_current_connections_current                                scylla_transport_current_connections
scylla_transport_queue_length_requests_blocked_memory                       scylla_transport_requests_blocked_memory
scylla_transport_queue_length_requests_serving                              scylla_transport_requests_serving
scylla_transport_total_requests_requests_served                             scylla_transport_requests_served
=========================================================================== ===========================================================================


New Metrics
~~~~~~~~~~~

The following metrics are new in 2018.1

+--------------------------------------------------------------------------+
| New Metric Name                                                          |
+==========================================================================+
| scylla_cache_active_reads                                                |
+--------------------------------------------------------------------------+
| scylla_cache_garbage_partitions                                          |
+--------------------------------------------------------------------------+
| scylla_cache_mispopulations                                              |
+--------------------------------------------------------------------------+
| scylla_cache_evictions_from_garbage                                      |
+--------------------------------------------------------------------------+
| scylla_cache_pinned_dirty_memory_overload                                |
+--------------------------------------------------------------------------+
| scylla_cache_reads                                                       |
+--------------------------------------------------------------------------+
| scylla_cache_reads_with_misses                                           |
+--------------------------------------------------------------------------+
| scylla_cache_row_hits                                                    |
+--------------------------------------------------------------------------+
| scylla_cache_row_insertions                                              |
+--------------------------------------------------------------------------+
| scylla_cache_row_misses                                                  |
+--------------------------------------------------------------------------+
| scylla_cache_sstable_partition_skips                                     |
+--------------------------------------------------------------------------+
| scylla_cache_sstable_reader_recreations                                  |
+--------------------------------------------------------------------------+
| scylla_cache_sstable_row_skips                                           |
+--------------------------------------------------------------------------+
| scylla_cql_batches_pure_logged                                           |
+--------------------------------------------------------------------------+
| scylla_cql_batches_pure_unlogged                                         |
+--------------------------------------------------------------------------+
| scylla_cql_batches_unlogged_from_logged                                  |
+--------------------------------------------------------------------------+
| scylla_cql_prepared_cache_evictions                                      |
+--------------------------------------------------------------------------+
| scylla_cql_prepared_cache_memory_footprint                               |
+--------------------------------------------------------------------------+
| scylla_cql_prepared_cache_size                                           |
+--------------------------------------------------------------------------+
| scylla_cql_statements_in_batches                                         |
+--------------------------------------------------------------------------+
| scylla_database_active_reads_memory_consumption                          |
+--------------------------------------------------------------------------+
| scylla_database_counter_cell_lock_acquisition                            |
+--------------------------------------------------------------------------+
| scylla_database_counter_cell_lock_pending                                |
+--------------------------------------------------------------------------+
| scylla_database_cpu_flush_quota                                          |
+--------------------------------------------------------------------------+
| scylla_execution_stages_function_calls_enqueued                          |
+--------------------------------------------------------------------------+
| scylla_execution_stages_function_calls_executed                          |
+--------------------------------------------------------------------------+
| scylla_execution_stages_tasks_preempted                                  |
+--------------------------------------------------------------------------+
| scylla_execution_stages_tasks_scheduled                                  |
+--------------------------------------------------------------------------+
| scylla_httpd_read_errors                                                 |
+--------------------------------------------------------------------------+
| scylla_httpd_reply_errors                                                |
+--------------------------------------------------------------------------+
| scylla_scheduler_queue_length                                            |
+--------------------------------------------------------------------------+
| scylla_scheduler_runtime_ms                                              |
+--------------------------------------------------------------------------+
| scylla_scheduler_shares                                                  |
+--------------------------------------------------------------------------+
| scylla_scheduler_tasks_processed                                         |
+--------------------------------------------------------------------------+
| scylla_scylladb_current_version                                          |
+--------------------------------------------------------------------------+
| scylla_sstables_index_page_blocks                                        |
+--------------------------------------------------------------------------+
| scylla_sstables_index_page_hits                                          |
+--------------------------------------------------------------------------+
| scylla_sstables_index_page_misses                                        |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_background_reads                        |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_foreground_read_repair                  |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_foreground_reads                        |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_read_latency                            |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_write_latency                           |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_replica_reads                                       |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_replica_received_counter_updates                    |
+--------------------------------------------------------------------------+
| scylla_transport_unpaged_queries                                         |
+--------------------------------------------------------------------------+


Deprecated Metrics
~~~~~~~~~~~~~~~~~~

The following metrics are deprecated in 2018.1

+--------------------------------------------------------------------------+
| Deprecated Metric Name                                                   |
+==========================================================================+
| scylla_cache_total_operations_uncached_wide_partitions                   |
+--------------------------------------------------------------------------+
| scylla_cache_total_operations_wide_partition_evictions                   |
+--------------------------------------------------------------------------+
| scylla_io_queue_delay_query                                              |
+--------------------------------------------------------------------------+
| scylla_io_queue_derive_query                                             |
+--------------------------------------------------------------------------+
| scylla_io_queue_queue_length_query                                       |
+--------------------------------------------------------------------------+
| scylla_io_queue_total_operations_query                                   |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_digest_read_errors_local_node           |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_digest_reads_local_node                 |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_mutation_data_read_errors_local_node    |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_mutation_data_reads_local_node          |
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_completed_mutation_data_reads_local_node|
+--------------------------------------------------------------------------+
| scylla_storage_proxy_coordinator_reads                                   |
+--------------------------------------------------------------------------+

