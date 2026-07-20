

Scylla Metric Update - Scylla 2.2 to 2.3
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 2.3 Dashboards are available as part of the latest |mon_root|

The following metrics are new in Scylla 2.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_cql_authorized_prepared_statements_cache_evictions
* scylla_cql_authorized_prepared_statements_cache_size
* scylla_cql_rows_read
* scylla_cql_secondary_index_creates
* scylla_cql_secondary_index_drops
* scylla_cql_secondary_index_reads
* scylla_cql_secondary_index_rows_read
* scylla_cql_user_prepared_auth_cache_footprint
* scylla_hints_for_views_manager_dropped
* scylla_hints_for_views_manager_errors
* scylla_hints_for_views_manager_sent
* scylla_hints_for_views_manager_size_of_hints_in_progress
* scylla_hints_for_views_manager_written
* scylla_lsa_memory_allocated
* scylla_lsa_memory_compacted
* scylla_lsa_free_space
* scylla_reactor_cpu_steal_time_ms

The following metrics were updated from Scylla 2.2 to Scylla 2.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_reactor_cpu_busy_ns changed to scylla_reactor_cpu_busy_ms
* scylla_storage_proxy_coordinator_read_latency is split into:
  
  * scylla_storage_proxy_coordinator_read_latency_bucket
  * scylla_storage_proxy_coordinator_read_latency_count
  * scylla_storage_proxy_coordinator_read_latency_sum
    
* scylla_storage_proxy_coordinator_write_latency changed is split into:
  
  * scylla_storage_proxy_coordinator_write_latency_bucket
  * scylla_storage_proxy_coordinator_write_latency_count
  * scylla_storage_proxy_coordinator_write_latency_sum



The following metrics were removed in Scylla 2.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_lsa_zones
* scylla_lsa_free_space_in_zones

