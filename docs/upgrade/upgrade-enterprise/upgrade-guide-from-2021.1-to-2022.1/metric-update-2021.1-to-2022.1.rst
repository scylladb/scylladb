=========================================================================
ScyllaDB Enterprise Metric Update - ScyllaDB Enterprise 2021.1 to 2022.1
=========================================================================

The following metrics are new in 2022.1 compared to 2021.1:

* scylla_commitlog_disk_slack_end_bytes
* scylla_cql_authorized_prepared_statements_unprivileged_entries_evictions_on_size
* scylla_cql_unprivileged_entries_evictions_on_size
* scylla_database_reads_shed_due_to_overload
* scylla_database_sstable_read_queue_overloads
* scylla_io_queue_disk_queue_length
* scylla_io_queue_starvation_time_sec
* scylla_io_queue_total_delay_sec
* scylla_io_queue_total_exec_sec
* scylla_io_queue_total_read_bytes
* scylla_io_queue_total_read_ops
* scylla_io_queue_total_write_bytes
* scylla_io_queue_total_write_ops
* scylla_node_ops_finished_percentage
* scylla_scheduler_starvetime_ms
* scylla_scheduler_waittime_ms
* scylla_sstables_index_page_cache_bytes_in_std
* scylla_sstables_index_page_evictions
* scylla_sstables_index_page_populations
* scylla_sstables_index_page_used_bytes
* scylla_storage_proxy_coordinator_total_write_attempts_remote_node
* scylla_storage_proxy_coordinator_writes_failed_due_to_too_many_in_flight_hints
* scylla_transport_auth_responses
* scylla_transport_batch_requests
* scylla_transport_cql_errors_total
* scylla_transport_execute_requests
* scylla_transport_options_requests
* scylla_transport_prepare_requests
* scylla_transport_query_requests
* scylla_transport_register_requests
* scylla_transport_startups

The following metrics are not longer available in 2022.1 compared to 2021.1:

* scylla_memory_streaming_dirty_bytes
* scylla_memory_streaming_virtual_dirty_bytes
* querier_cache_memory_based_evictions
