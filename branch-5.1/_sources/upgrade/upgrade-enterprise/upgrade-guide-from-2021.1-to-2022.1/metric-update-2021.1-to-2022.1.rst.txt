=========================================================================
ScyllaDB Enterprise Metric Update - ScyllaDB Enterprise 2021.1 to 2022.1
=========================================================================

New Metrics
------------

The following metrics were added in ScyllaDB 2022.1:


.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_commitlog_disk_slack_end_bytes
     - Holds a size of disk space in bytes unused because of segment switching (end slack). A too high value indicates that we do not write enough data to each segment.
   * - scylla_cql_authorized_prepared_statements_unprivileged_entries_evictions_on_size
     - Counts a number of evictions of prepared statements from the authorized prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.
   * - scylla_cql_unprivileged_entries_evictions_on_size
     - Counts a number of evictions of prepared statements from the prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.
   * - scylla_database_reads_shed_due_to_overload
     - The number of reads shed because the admission queue reached its max capacity. When the queue is full, excessive reads are shed to avoid overload.
   * - scylla_database_sstable_read_queue_overloads
     - Counts the number of times the sstable read queue was overloaded. A non-zero value indicates that we have to drop read requests because they arrive faster than we can serve them.
   * - scylla_io_queue_disk_queue_length
     - Number of requests in the disk.
   * - scylla_io_queue_starvation_time_sec
     - Total time spent starving for disk.
   * - scylla_io_queue_total_delay_sec
     - Total time spent in the queue.
   * - scylla_io_queue_total_exec_sec
     - Total time spent in disk.
   * - scylla_io_queue_total_read_bytes
     - Total read bytes passed in the queue.
   * - scylla_io_queue_total_read_ops
     - Total read operations passed in the queue.
   * - scylla_io_queue_total_write_bytes
     - Total write bytes passed in the queue.
   * - scylla_io_queue_total_write_ops
     - Total write operations passed in the queue.
   * - scylla_node_ops_finished_percentage
     - Finished percentage of node operation on this shard.
   * - scylla_scheduler_starvetime_ms
     - Accumulated starvation time of this task queue; an increment rate of 1000ms per second indicates the scheduler feels really bad.
   * - scylla_scheduler_waittime_ms
     - Accumulated waittime of this task queue; an increment rate of 1000ms per second indicates queue is waiting for something (e.g. IO).
   * - scylla_sstables_index_page_cache_bytes_in_std
     - Total number of bytes in temporary buffers which live in the std allocator.
   * - scylla_sstables_index_page_evictions
     - Index pages which got evicted from memory.
   * - scylla_sstables_index_page_populations
     - Index pages which got populated into memory.
   * - scylla_sstables_index_page_used_bytes
     - Amount of bytes used by index pages in memory.
   * - scylla_storage_proxy_coordinator_total_write_attempts_remote_node
     - Total number of write requests when communicating with external Nodes in DC datacenter1.
   * - scylla_storage_proxy_coordinator_writes_failed_due_to_too_many_in_flight_hints
     - Number of CQL write requests which failed because the hinted handoff mechanism is overloaded and cannot store any more in-flight hints.
   * - scylla_transport_auth_responses
     - Counts the total number of received CQL AUTH messages.
   * - scylla_transport_batch_requests
     - Counts the total number of received CQL BATCH messages.
   * - scylla_transport_cql_errors_total
     - Counts the total number of returned CQL errors.
   * - scylla_transport_execute_requests
     - Counts the total number of received CQL EXECUTE messages.
   * - scylla_transport_options_requests
     - Counts the total number of received CQL OPTIONS messages.
   * - scylla_transport_prepare_requests
     - Counts the total number of received CQL PREPARE messages.
   * - scylla_transport_query_requests
     - Counts the total number of received CQL QUERY messages.
   * - scylla_transport_register_requests
     - Counts the total number of received CQL REGISTER messages.
   * - scylla_transport_startups
     - Counts the total number of received CQL STARTUP messages.

Removed Metrics
-----------------

The following metrics are no longer available in ScyllaDB 2022.1:

* scylla_memory_streaming_dirty_bytes
* scylla_memory_streaming_virtual_dirty_bytes
* querier_cache_memory_based_evictions
