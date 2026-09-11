

Scylla Metric Update - Scylla 2.0 to 2.1
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

The following metric names have changed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Scylla_database_active_reads_streaming changed To scylla_database_active_reads {reads=streaming}
* Scylla_database_active_reads_system_keyspace changed To scylla_database_active_reads {reads=system_keyspace}
* scylla_database_queued_reads_streaming changed To scylla_database_queued_reads {reads=streaming}
* scylla_database_queued_reads_system_keyspace changed To scylla_database_queued_reads_system {reads=keyspace}

The following metrics are new in Scylla 2.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_cache_pinned_dirty_memory_overload
* scylla_column_family_cache_hit_rate
* scylla_column_family_live_disk_space
* scylla_column_family_live_sstable
* scylla_column_family_memtable_switch
* scylla_column_family_pending_compaction
* scylla_column_family_pending_tasks
* scylla_column_family_read_latency
* scylla_column_family_total_disk_space
* scylla_column_family_write_latency
* scylla_cql_prepared_cache_evictions
* scylla_cql_prepared_cache_memory_footprint
* scylla_cql_prepared_cache_size
* scylla_database_active_reads_memory_consumption
* scylla_io_queue_commitlog_shares
* scylla_io_queue_compaction_shares
* scylla_io_queue_default_shares
* scylla_io_queue_memtable_flush_shares
* scylla_scheduler_queue_length
* scylla_scheduler_runtime_ms
* scylla_scheduler_shares
* scylla_scheduler_tasks_processed
* scylla_storage_proxy_coordinator_speculative_data_reads
* scylla_storage_proxy_coordinator_speculative_digest_reads
* scylla_storage_proxy_coordinator_total_write_attempts_remote_node
* scylla_transport_requests_blocked_memory_current
* scylla_transport_unpaged_queries
