====================================================================
Scylla Enterprise Metric Update - Scylla Enterprise 2020.1 to 2021.1
====================================================================


The following metrics are new in 2021.1 compared to 2020.1:

* scylla_commitlog_disk_active_bytes
* scylla_compaction_manager_backlog
* scylla_hints_for_views_manager_pending_drains
* scylla_hints_for_views_manager_pending_sends
* scylla_hints_manager_pending_drains
* scylla_hints_manager_pending_sends
* scylla_sstables_index_page_cache_bytes
* scylla_sstables_index_page_cache_evictions
* scylla_sstables_index_page_cache_hits
* scylla_sstables_index_page_cache_misses
* scylla_sstables_index_page_cache_populations
* scylla_sstables_pi_cache_block_count
* scylla_sstables_pi_cache_bytes
* scylla_sstables_pi_cache_evictions
* scylla_sstables_pi_cache_hits_l0
* scylla_sstables_pi_cache_hits_l1
* scylla_sstables_pi_cache_hits_l2
* scylla_sstables_pi_cache_misses_l0
* scylla_sstables_pi_cache_misses_l1
* scylla_sstables_pi_cache_misses_l2
* scylla_sstables_pi_cache_populations
* scylla_storage_proxy_coordinator_background_replica_writes_failed_remote_node
* scylla_storage_proxy_coordinator_cas_background
* scylla_storage_proxy_coordinator_cas_foreground
* scylla_storage_proxy_coordinator_cas_total_operations
* scylla_storage_proxy_coordinator_total_write_attempts_remote_node
* scylla_transport_requests_shed
* scylla_view_builder_builds_in_progress
* scylla_view_builder_pending_bookkeeping_ops
* scylla_view_builder_steps_failed
* scylla_view_builder_steps_performed
* scylla_view_update_generator_pending_registrations
* scylla_view_update_generator_queued_batches_count
* scylla_view_update_generator_sstables_to_move_count

The following metrics are not longer available in 2021.1 compared to 2020.1:

* scylla_lsa_segments_migrated
* scylla_reactor_io_queue_requests
