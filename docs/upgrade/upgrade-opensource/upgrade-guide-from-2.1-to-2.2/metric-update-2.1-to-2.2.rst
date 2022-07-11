

Scylla Metric Update - Scylla 2.1 to 2.2
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 2.2 Dashboards are available as part of the latest |mon_root|

The following metrics are new in Scylla 2.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* scylla_cache_row_evictions
* scylla_cache_row_removals
* scylla_cache_rows
* scylla_cache_rows_dropped_from_memtable
* scylla_cache_rows_merged_from_memtable
* scylla_cache_rows_processed_from_memtable
* scylla_cache_static_row_insertions
* scylla_database_querier_cache_drops
* scylla_database_querier_cache_lookups
* scylla_database_querier_cache_memory_based_evictions
* scylla_database_querier_cache_misses
* scylla_database_querier_cache_population
* scylla_database_querier_cache_resource_based_evictions
* scylla_database_querier_cache_time_based_evictions
* scylla_database_requests_blocked_memory_current
* scylla_io_queue_commitlog_shares
* scylla_io_queue_compaction_shares
* scylla_io_queue_default_shares
* scylla_io_queue_memtable_flush_shares
* scylla_storage_proxy_coordinator_speculative_data_reads
* scylla_storage_proxy_coordinator_speculative_digest_reads
* scylla_transport_requests_blocked_memory_current


The following metric was removed in Scylla 2.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* scylla_database_cpu_flush_quota

