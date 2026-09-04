.. |SRC_VERSION| replace:: 2026.x
.. |NEW_VERSION| replace:: 2026.3
.. |PRECEDING_VERSION| replace:: 2026.2

================================================================
Metrics Update Between |SRC_VERSION| and |NEW_VERSION|
================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.


New Metrics in |NEW_VERSION|
--------------------------------------

The following metrics are new in ScyllaDB |NEW_VERSION| compared to |PRECEDING_VERSION|.

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_alternator_conditional_check_failed
     - The number of conditional requests whose condition was false (ConditionalCheckFailedException).
   * - scylla_alternator_returned_items
     - The number of items returned by Query and Scan operations (not other operations).
   * - scylla_alternator_returned_items_histogram
     - A histogram of the number of items returned per Query or Scan operation.
   * - scylla_alternator_returned_records
     - The number of stream records returned by GetRecords operations.
   * - scylla_alternator_system_errors
     - The number of HTTP 500 internal server error responses.
   * - scylla_alternator_user_errors
     - The number of HTTP 400 client error responses, excluding ConditionalCheckFailedException.
   * - scylla_alternator_vector_search_query
     - The number of Query operations with VectorSearch.
   * - scylla_alternator_vector_search_query_items_from_base_table
     - The total number of items read from the base table by Vector Search queries.
   * - scylla_alternator_vector_search_query_items_from_vs
     - The total number of nearest neighbors found by the vector store (some may be post-filtered and not returned).
   * - scylla_alternator_vector_search_query_returned_items
     - The total number of items returned by Query operations with Vector Search.
   * - scylla_database_large_cell_exceeding_threshold
     - The number of large cells exceeding ``compaction_large_cell_warning_threshold_mb``.
       Large cells have performance impact and should be avoided.
   * - scylla_database_large_collection_exceeding_threshold
     - The number of large collections exceeding ``compaction_collection_elements_count_warning_threshold``.
       Large collections have performance impact and should be avoided.
   * - scylla_database_large_rows_exceeding_threshold
     - The number of large rows exceeding ``compaction_large_row_warning_threshold_mb``.
       Large rows have performance impact and should be avoided.
   * - scylla_database_reads_memory_borrowed_from_shared_pool
     - The current amount of memory borrowed from the shared pool.
   * - scylla_database_reads_shared_pool_available_memory
     - The current amount of available memory in the shared reader concurrency semaphore pool.
   * - scylla_database_reads_shared_pool_total_memory
     - The total memory of the shared reader concurrency semaphore pool.
   * - scylla_group_name_rebuilds_produced
     - The number of rebuilds produced by the load balancer.
   * - scylla_group_name_repairs_produced
     - The number of repairs produced by the load balancer.
   * - scylla_logstor_sm_separator_buffers_in_use
     - The current number of separator buffers in use.
   * - scylla_memory_logstor_bytes
     - The current size of memory used by logstor in bytes.
   * - scylla_s3_integrated_request_queue_length
     - The number of queued HTTP requests integrated over time (measured in request-seconds).
   * - scylla_s3_total_connect_latency_sec
     - The total time in seconds spent in HTTP CONNECT requests.
   * - scylla_s3_total_connect_requests
     - The total number of HTTP CONNECT requests.
   * - scylla_s3_total_connect_retries
     - The total number of HTTP CONNECT retries.
   * - scylla_s3_total_delete_latency_sec
     - The total time in seconds spent in HTTP DELETE requests.
   * - scylla_s3_total_delete_requests
     - The total number of HTTP DELETE requests.
   * - scylla_s3_total_delete_retries
     - The total number of HTTP DELETE retries.
   * - scylla_s3_total_get_latency_sec
     - The total time in seconds spent in HTTP GET requests
   * - scylla_s3_total_get_requests
     - The total number of HTTP GET requests.
   * - scylla_s3_total_get_retries
     - The total number of HTTP GET retries.
   * - scylla_s3_total_head_latency_sec
     - The total time in seconds spent in HTTP HEAD requests.
   * - scylla_s3_total_head_requests
     - The total number of HTTP HEAD requests.
   * - scylla_s3_total_head_retries
     - The total number of HTTP HEAD retries.
   * - scylla_s3_total_options_latency_sec
     - The total time in seconds spent in HTTP OPTIONS requests.
   * - scylla_s3_total_options_requests
     - The total number of HTTP OPTIONS requests.
   * - scylla_s3_total_options_retries
     - The total number of HTTP OPTIONS retries.
   * - scylla_s3_total_patch_latency_sec
     - The total time in seconds spent in HTTP PATCH requests.
   * - scylla_s3_total_patch_requests
     - The total number of HTTP PATCH requests.
   * - scylla_s3_total_patch_retries
     - The total number of HTTP PATCH retries.
   * - scylla_s3_total_post_latency_sec
     - The total time in seconds spent in HTTP POST requests.
   * - scylla_s3_total_post_requests
     - The total number of HTTP POST requests.
   * - scylla_s3_total_post_retries
     - The total number of HTTP POST retries.
   * - scylla_s3_total_put_latency_sec
     - The total time in seconds spent in HTTP PUT requests.
   * - scylla_s3_total_put_requests
     - The total number of HTTP PUT requests.
   * - scylla_s3_total_put_retries
     - The total number of HTTP PUT retries.
   * - scylla_s3_total_trace_latency_sec
     - The total time in seconds spent in HTTP TRACE requests.
   * - scylla_s3_total_trace_requests
     - The total number of HTTP TRACE requests.
   * - scylla_s3_total_trace_retries
     - The total number of HTTP TRACE retries.
   * - scylla_service_level_workload_type
     - The  workload type configured for each service level. 0 - unspecified, 1 - batch, 2 - interactive.
   * - scylla_strong_consistency_coordinator_read_errors
     - The number of strong consistency read requests that failed.
   * - scylla_strong_consistency_coordinator_read_latency
     - A strong consistency read latency histogram.
   * - scylla_strong_consistency_coordinator_read_latency_summary
     - A strong consistency read latency summary.
   * - scylla_strong_consistency_coordinator_read_node_bounces
     - The number of strong consistency read requests bounced to another node.
   * - scylla_strong_consistency_coordinator_read_shard_bounces
     - The number of strong consistency read requests bounced to another shard.
   * - scylla_strong_consistency_coordinator_write_errors
     - The number of strong consistency write requests that failed.
   * - scylla_strong_consistency_coordinator_write_latency
     - A strong consistency write latency histogram.
   * - scylla_strong_consistency_coordinator_write_latency_summary
     - A strong consistency write latency summary.
   * - scylla_strong_consistency_coordinator_write_node_bounces
     - The vnumber of strong consistency write requests bounced to another node.
   * - scylla_strong_consistency_coordinator_write_shard_bounces
     - The number of strong consistency write requests bounced to another shard.
   * - scylla_transport_cql_client_timestamp_drift_histogram
     - A histogram of the drift in microseconds between the client-provided CQL
       timestamp and the server time at request arrival. Useful for detecting
       transport delays before requests reach the CQL server.
   * - scylla_transport_cql_request_latency_histogram
     - A histogram of transport-level CQL request latencies (in microseconds),
       measuring from the start of request processing until the response is
       written to the socket.


Renamed Metrics in |NEW_VERSION|
---------------------------------

The following metrics are renamed in ScyllaDB |NEW_VERSION| compared to |PRECEDING_VERSION|.

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric Name in |PRECEDING_VERSION|
     - Metric Name in |NEW_VERSION|
   * - scylla_s3_downloads_blocked_on_memory
     - scylla_s3_downloads_starving_on_max_concurrency

New and Updated Metrics in Previous 2026.x Releases
-----------------------------------------------------

* `Metrics Update Between 2026.1 and 2026.2 <https://docs.scylladb.com/manual/branch-2026.2/upgrade/upgrade-guides/upgrade-guide-from-2026.1-to-2026.2/metric-update-2026.1-to-2026.2.html>`_
