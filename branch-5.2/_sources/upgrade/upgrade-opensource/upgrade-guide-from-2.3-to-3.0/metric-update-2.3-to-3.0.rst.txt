

Scylla Metric Update - Scylla 2.3 to 3.0
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 3.0 Dashboards are available as part of the latest  |mon_root|

The following metrics are new in Scylla 3.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `scylla_cql_filtered_read_requests`
* `scylla_cql_filtered_rows_dropped_total`
* `scylla_cql_filtered_rows_matched_total`
* `scylla_cql_filtered_rows_read_total`
* `scylla_cql_reverse_queries`
* `scylla_database_large_partition_exceeding_threshold`
* `scylla_database_multishard_query_failed_reader_saves`
* `scylla_database_multishard_query_failed_reader_stops`
* `scylla_database_multishard_query_unpopped_bytes`
* `scylla_database_multishard_query_unpopped_fragments`
* `scylla_node_operation_mode`
* `scylla_storage_proxy_replica_cross_shard_ops`


The following metric was updated from Scylla 2.3 to Scylla 3.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `scylla_reactor_cpu_busy_ns` changed to `scylla_reactor_cpu_busy_ms`
* `scylla_reactor_cpu_steal_time_ns` changed to `scylla_reactor_cpu_steal_time_ms`
* `scylla_scheduler_time_spent_on_task_quota_violations_ns` changed to `scylla_scheduler_time_spent_on_task_quota_violations_ms`
* `scylla_transport_unpaged_queries` changed to `scylla_cql_unpaged_select_queries`
