.. |SRC_VERSION| replace:: 2025.2
.. |NEW_VERSION| replace:: 2025.3

================================================================
Metrics Update Between |SRC_VERSION| and |NEW_VERSION|
================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.

New Metrics
------------

The following metrics are new in ScyllaDB |NEW_VERSION| compared to |SRC_VERSION|.

Alternator Per-table Metrics
===================================

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_alternator_table_batch_item_count
     - The total number of items processed across all batches.
   * - scylla_alternator_table_batch_item_count_histogram	
     - A histogram of the number of items in a batch request.
   * - scylla_alternator_table_filtered_rows_dropped_total	
     - The number of rows read and dropped during filtering operations.
   * - scylla_alternator_table_filtered_rows_matched_total	
     - The number of rows read and matched during filtering operations.
   * - scylla_alternator_table_filtered_rows_read_total	
     - The number of rows read during filtering operations.
   * - scylla_alternator_table_op_latency
     - A latency histogram of an operation via Alternator API.
   * - scylla_alternator_table_op_latency_summary	
     - A latency summary of an operation via Alternator API.
   * - scylla_alternator_table_operation
     - The number of operations via Alternator API.
   * - scylla_alternator_table_rcu_total
     - The total number of consumed read units.
   * - scylla_alternator_table_reads_before_write
     - The number of performed read-before-write operations.
   * - scylla_alternator_table_requests_blocked_memory
     - Counts the number of requests blocked due to memory pressure.
   * - scylla_alternator_table_requests_shed
     - Counts the number of requests shed due to overload.
   * - scylla_alternator_table_shard_bounce_for_lwt
     - The number of writes that had to be bounced from this shard because of LWT requirements.
   * - scylla_alternator_table_total_operations
     - The number of total operations via Alternator API.
   * - scylla_alternator_table_unsupported_operations
     - The number of unsupported operations via Alternator API.
   * - scylla_alternator_table_wcu_total	
     - The total number of consumed write units.
   * - scylla_alternator_table_write_using_lwt
     - The number of writes that used LWT.

Other Metrics
===============

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_batchlog_manager_total_write_replay_attempts
     - Counts write operations issued in a batchlog replay flow.
       A high value of this metric indicates that there is a long batch replay list.
   * - scylla_corrupt_data_entries_reported	
     - Counts the number of corrupt data instances reported to the corrupt data handler.
       A non-zero value indicates that the database suffered data corruption.
   * - scylla_memory_oversized_allocs
     - The total count of oversized memory allocations.
   * - scylla_reactor_internal_errors
     - The total number of internal errors (subset of cpp_exceptions) that usually
       indicate a malfunction in the code
   * - scylla_stall_detector_io_threaded_fallbacks	
     - The total number of io-threaded-fallbacks operations.

Removed Metrics
---------------------

The following metrics have been removed in 2025.3:

* scylla_cql_authorized_prepared_statements_cache_evictions
* scylla_lsa_large_objects_total_space_bytes
* scylla_lsa_small_objects_total_space_bytes
* scylla_lsa_small_objects_used_space_bytes

