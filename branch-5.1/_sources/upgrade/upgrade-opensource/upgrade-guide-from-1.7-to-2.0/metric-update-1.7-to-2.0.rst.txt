

========================================
Scylla Metric Update - Scylla 1.7 to 2.0
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

The following metric names have changed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_cache_evictions To scylla_cache_partition_evictions
* scylla_cache_hits To scylla_cache_partition_hits
* scylla_cache_insertions To scylla_cache_partition_insertions
* scylla_cache_merges To scylla_cache_partition_merges
* scylla_cache_misses To scylla_cache_partition_misses
* scylla_cache_removals To scylla_cache_partition_removals
* scylla_cache_total To scylla_cache_bytes_total
* scylla_cache_used To scylla_cache_bytes_used

The following metrics are no longer available
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_cache_uncached_wide_partitions
* scylla_cache_wide_partition_evictions
* scylla_cache_wide_partition_mispopulations

The following metrics are new in Scylla 2.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_cache_mispopulations
* scylla_cache_reads
* scylla_cache_active_reads
* scylla_cache_reads_with_misses
* scylla_cache_row_hits
* scylla_cache_row_insertions
* scylla_cache_row_misses
* scylla_cache_sstable_partition_skips
* scylla_cache_sstable_reader_recreations
* scylla_cache_sstable_row_skips
* scylla_column_family_live_disk_space
* scylla_column_family_live_sstable
* scylla_column_family_memtable_switch
* scylla_column_family_pending_compaction
* scylla_column_family_pending_tasks
* scylla_column_family_total_disk_space
* scylla_database_active_reads_streaming
* scylla_database_counter_cell_lock_acquisition
* scylla_database_counter_cell_lock_pending
* scylla_database_cpu_flush_quota
* scylla_database_queued_reads_streaming
* scylla_execution_stages_function_calls_enqueued
* scylla_execution_stages_function_calls_executed
* scylla_execution_stages_tasks_preempted
* scylla_execution_stages_tasks_scheduled
* scylla_scylladb_current_version
* scylla_sstables_index_page_blocks
* scylla_sstables_index_page_hits
* scylla_sstables_index_page_misses
* scylla_storage_proxy_coordinator_background_read_repairs
* scylla_storage_proxy_coordinator_foreground_read_repair
* scylla_storage_proxy_coordinator_read_latency
* scylla_storage_proxy_coordinator_write_latency
* scylla_storage_proxy_replica_received_counter_updates
