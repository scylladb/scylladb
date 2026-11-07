.. |SRC_VERSION| replace:: 2026.x
.. |NEW_VERSION| replace:: 2026.4
.. |PRECEDING_VERSION| replace:: 2026.3

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
   * - scylla_column_family_logstor_segments
     - Number of logstor segments owned by this table.
   * - scylla_column_family_sstables_per_read
     - Number of sstables accessed to serve a single-partition read.
   * - scylla_cql_select_paging_plan_rederivations
     - Counts the pages of a prepared SELECT re-derived, each paying a re-parse, because the paging state pinned another query plan.
   * - scylla_encryption_key_attr_cache_blocked
     - Number of key cache blocking load resolves.
   * - scylla_encryption_key_attr_cache_evictions
     - Number of key cache evictions.
   * - scylla_encryption_key_attr_cache_hits
     - Number of key cache hits.
   * - scylla_encryption_key_attr_cache_misses
     - Number of key cache misses.
   * - scylla_encryption_key_attr_cache_privileged_evictions
     - Number of privileged key cache evictions.
   * - scylla_encryption_key_attr_cache_unprivileged_evictions
     - Number of unprivileged key cache evictions.
   * - scylla_encryption_key_id_cache_blocked
     - Number of key cache blocking load resolves.
   * - scylla_encryption_key_id_cache_evictions
     - Number of key cache evictions.
   * - scylla_encryption_key_id_cache_hits
     - Number of key cache hits.
   * - scylla_encryption_key_id_cache_misses
     - Number of key cache misses.
   * - scylla_encryption_key_id_cache_privileged_evictions
     - Number of privileged key cache evictions.
   * - scylla_encryption_key_id_cache_unprivileged_evictions
     - Number of unprivileged key cache evictions.
   * - scylla_hints_for_views_manager_discarded_segments
     - Number of hints files that were discarded without being replayed.
   * - scylla_hints_manager_discarded_segments
     - Number of hints files that were discarded without being replayed.
   * - scylla_load_balancer_auto_repair_enabled_nr
     - Number of tablets with auto repair enabled.
   * - scylla_load_balancer_auto_repair_needs_repair_nr
     - Number of tablets with auto repair enabled that currently needs repair.
   * - scylla_load_balancer_calls
     - Number of calls to the load balancer.
   * - scylla_load_balancer_cross_rack_collocations
     - Number of co-locating migrations which move replica across racks.
   * - scylla_load_balancer_load
     - Node load during last load balancing.
   * - scylla_load_balancer_migrations_produced
     - Number of migrations produced by the load balancer.
   * - scylla_load_balancer_migrations_skipped
     - Number of migrations skipped by the load balancer due to load limits.
   * - scylla_load_balancer_rebuilds_produced
     - Number of rebuilds produced by the load balancer.
   * - scylla_load_balancer_repairs_produced
     - Number of repairs produced by the load balancer.
   * - scylla_load_balancer_resizes_emitted
     - Number of resizes produced by the load balancer.
   * - scylla_load_balancer_resizes_finalized
     - Number of resizes finalized by the load balancer.
   * - scylla_load_balancer_resizes_revoked
     - Number of resizes revoked by the load balancer.
   * - scylla_logstor_queued_write_count
     - Number of writes currently queued in the write buffer.
   * - scylla_logstor_sm_compaction_buffer_allocation_waits
     - Counts number of times a compaction had to wait for a write buffer to become available.
   * - scylla_logstor_sm_compaction_buffers_dropped
     - Counts number of write buffers permanently removed from the pool after they could not be reclaimed.
   * - scylla_logstor_sm_compaction_buffers_in_use
     - Counts number of write buffers currently held by compaction. The pool size is the cap on concurrent compactions.
   * - scylla_logstor_sm_compaction_bytes_read
     - Counts number of bytes read by compaction.
   * - scylla_logstor_sm_compaction_failures
     - Counts number of logstor compaction failures.
   * - scylla_logstor_sm_live_record_bytes
     - Counts the durable live record bytes currently referenced by the primary index.
   * - scylla_logstor_sm_live_record_count
     - Counts the durable live records currently referenced by the primary index.
   * - scylla_logstor_sm_owned_segments_live_record_bytes
     - Record bytes of the live records in the segments that the compaction groups of this shard own. Dead records use the remaining space of these segments. Compaction frees that space.
   * - scylla_logstor_sm_segment_free_failures
     - Counts number of logstor segment references that failed to free the segment.
   * - scylla_logstor_sm_segment_utilization
     - Distribution by utilization of the segments that the compaction groups of this shard own. The utilization of a segment is the record bytes of its live records divided by the segment size.
   * - scylla_logstor_sm_separator_buffer_allocation_waits
     - Counts number of times the separator had to wait for a write buffer to become available.
   * - scylla_logstor_sm_separator_buffer_pool_capacity
     - Number of write buffers the separator can hold at once, which follows the number of compaction groups.
   * - scylla_logstor_sm_separator_buffers_allocated
     - Counts number of write buffers the separator buffer pool holds the memory of, including the ones it keeps for reuse.
   * - scylla_logstor_sm_separator_buffers_dropped
     - Counts number of separator write buffers permanently removed from the pool after they could not be reclaimed.
   * - scylla_logstor_sm_separator_flush_failures
     - Counts number of logstor separator flush failures.
   * - scylla_logstor_sm_separator_task_failures
     - Counts number of logstor separator task failures.
   * - scylla_logstor_sm_separator_task_queue_size
     - Current number of pending tasks in the separator task queue.
   * - scylla_logstor_sm_write_failures
     - Counts number of write failures when appending to a logstor segment.
   * - scylla_logstor_write_failures
     - Number of writes that failed to be persisted.
   * - scylla_memory_logstor_index_entries_bytes
     - Bytes of memory that the logstor primary index entries take, without the nodes and buckets of the B+trees.
   * - scylla_object_storage_integrated_request_queue_length
     - The number of queued HTTP requests integrated over time (measured in request-seconds).
   * - scylla_object_storage_nr_active_connections
     - Total number of connections with running requests.
   * - scylla_object_storage_nr_connections
     - Total number of connections.
   * - scylla_object_storage_total_connect_latency_sec
     - Total time in seconds spent in HTTP CONNECT requests.
   * - scylla_object_storage_total_connect_requests
     - Total number of HTTP CONNECT requests.
   * - scylla_object_storage_total_connect_retries
     - Total number of HTTP CONNECT retries.
   * - scylla_object_storage_total_delete_latency_sec
     - Total time in seconds spent in HTTP DELETE requests.
   * - scylla_object_storage_total_delete_requests
     - Total number of HTTP DELETE requests.
   * - scylla_object_storage_total_delete_retries
     - Total number of HTTP DELETE retries.
   * - scylla_object_storage_total_get_latency_sec
     - Total time in seconds spent in HTTP GET requests.
   * - scylla_object_storage_total_get_requests
     - Total number of HTTP GET requests.
   * - scylla_object_storage_total_get_retries
     - Total number of HTTP GET retries.
   * - scylla_object_storage_total_head_latency_sec
     - Total time in seconds spent in HTTP HEAD requests.
   * - scylla_object_storage_total_head_requests
     - Total number of HTTP HEAD requests.
   * - scylla_object_storage_total_head_retries
     - Total number of HTTP HEAD retries.
   * - scylla_object_storage_total_new_connections
     - Total number of new connections created so far.
   * - scylla_object_storage_total_options_latency_sec
     - Total time in seconds spent in HTTP OPTIONS requests.
   * - scylla_object_storage_total_options_requests
     - Total number of HTTP OPTIONS requests.
   * - scylla_object_storage_total_options_retries
     - Total number of HTTP OPTIONS retries.
   * - scylla_object_storage_total_patch_latency_sec
     - Total time in seconds spent in HTTP PATCH requests.
   * - scylla_object_storage_total_patch_requests
     - Total number of HTTP PATCH requests.
   * - scylla_object_storage_total_patch_retries
     - Total number of HTTP PATCH retries.
   * - scylla_object_storage_total_post_latency_sec
     - Total time in seconds spent in HTTP POST requests.
   * - scylla_object_storage_total_post_requests
     - Total number of HTTP POST requests.
   * - scylla_object_storage_total_post_retries
     - Total number of HTTP POST retries.
   * - scylla_object_storage_total_put_latency_sec
     - Total time in seconds spent in HTTP PUT requests.
   * - scylla_object_storage_total_put_requests
     - Total number of HTTP PUT requests.
   * - scylla_object_storage_total_put_retries
     - Total number of HTTP PUT retries.
   * - scylla_object_storage_total_read_bytes
     - Total number of bytes read from objects.
   * - scylla_object_storage_total_read_latency_sec
     - Total time spent reading data from objects.
   * - scylla_object_storage_total_read_requests
     - Total number of object read requests.
   * - scylla_object_storage_total_trace_latency_sec
     - Total time in seconds spent in HTTP TRACE requests.
   * - scylla_object_storage_total_trace_requests
     - Total number of HTTP TRACE requests.
   * - scylla_object_storage_total_trace_retries
     - Total number of HTTP TRACE retries.
   * - scylla_object_storage_total_write_bytes
     - Total number of bytes written to objects.
   * - scylla_object_storage_total_write_latency_sec
     - Total time spent writing data to objects.
   * - scylla_object_storage_total_write_requests
     - Total number of object write requests.
   * - scylla_raft_blocked_followers
     - Number of followers the leader cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight), snapshot (waiting for a snapshot transfer) or log_full (the follower reported that its log is full); followers the failure detector reports down are not counted.
   * - scylla_raft_group0_append_entries_in_flight_bytes
     - Bytes of append requests currently being sent, as charged against the in-flight memory limit.
   * - scylla_raft_group0_append_entries_memory_waiters
     - Number of append requests currently waiting for the memory of other in-flight append requests to be released.
   * - scylla_raft_group0_append_entries_memory_waits
     - Number of append requests that had to wait for the memory of other in-flight append requests to be released.
   * - scylla_raft_log_full
     - 1 if the in-memory log reached ``max_follower_log_size``, so entries from a leader are being refused.
   * - scylla_raft_log_limiter_waiters
     - Number of entries currently waiting for the in-memory log to shrink below ``max_log_size``.
   * - scylla_raft_log_limiter_waits
     - Number of times appending an entry had to wait for the in-memory log to shrink below ``max_log_size``; an entry waiting for its turn to be appended is counted only if it still has to wait once its turn comes.
   * - scylla_s3_refused_request_ratio
     - Smoothed share of S3 requests the endpoint is refusing.
   * - scylla_s3_send_freezes
     - Times sending was held back after a throttling response.
   * - scylla_s3_throttles
     - Total number of throttling responses (503 SlowDown etc.) from S3.
   * - scylla_storage_proxy_coordinator_read_repairs_diff_found_total
     - Number of read repairs in which the full-data reconciliation found actual differences between replicas and repair mutations were sent.
   * - scylla_strong_consistency_raft_add_entries
     - Number of entries added on this node, the log_entry_type label can be command, dummy or config.
   * - scylla_strong_consistency_raft_append_entries_in_flight_bytes
     - Bytes of append requests currently being sent, as charged against the in-flight memory limit.
   * - scylla_strong_consistency_raft_append_entries_memory_waiters
     - Number of append requests currently waiting for the memory of other in-flight append requests to be released.
   * - scylla_strong_consistency_raft_append_entries_memory_waits
     - Number of append requests that had to wait for the memory of other in-flight append requests to be released.
   * - scylla_strong_consistency_raft_applied_entries
     - Number of log entries applied.
   * - scylla_strong_consistency_raft_blocked_followers
     - Number of followers the tablet raft group leaders cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight), snapshot (waiting for a snapshot transfer) or log_full (the follower reported that its log is full); followers the failure detector reports down are not counted.
   * - scylla_strong_consistency_raft_full_logs
     - Number of raft logs of the table which reached ``max_follower_log_size``, so entries from a leader are being refused.
   * - scylla_strong_consistency_raft_in_memory_log_size
     - Number of entries in the in-memory part of the raft logs of the table.
   * - scylla_strong_consistency_raft_leaders
     - Number of tablet raft groups of the table led by this node.
   * - scylla_strong_consistency_raft_log_limiter_waiters
     - Number of entries currently waiting for an in-memory raft log of the table to shrink below ``max_log_size``.
   * - scylla_strong_consistency_raft_log_limiter_waits
     - Number of times appending an entry had to wait for the in-memory log to shrink below ``max_log_size``; an entry waiting for its turn to be appended is counted only if it still has to wait once its turn comes.
   * - scylla_strong_consistency_raft_log_memory_usage
     - Bytes used by the in-memory part of the raft logs of the table.
   * - scylla_strong_consistency_raft_messages_received
     - Number of messages received, the message_type determines the type of message.
   * - scylla_strong_consistency_raft_messages_sent
     - Number of messages sent, the message_type determines the type of message.
   * - scylla_strong_consistency_raft_persisted_log_entries
     - Number of log entries persisted.
   * - scylla_strong_consistency_raft_polls
     - Number of times raft state machine polled.
   * - scylla_strong_consistency_raft_queue_entries_for_apply
     - Number of log entries scheduled to be applied.
   * - scylla_strong_consistency_raft_sm_load_snapshot
     - Number of times user state machine reloaded with a snapshot.
   * - scylla_strong_consistency_raft_snapshots_taken
     - Number of times user's state machine snapshotted.
   * - scylla_strong_consistency_raft_store_snapshot
     - Number of snapshots persisted.
   * - scylla_strong_consistency_raft_store_term_and_vote
     - Number of times term and vote persisted.
   * - scylla_strong_consistency_raft_truncate_persisted_log
     - Number of times log truncated on storage.
   * - scylla_strong_consistency_raft_unapplied_entries
     - Number of committed raft log entries of the table not applied yet.
   * - scylla_strong_consistency_raft_uncommitted_entries
     - Number of raft log entries of the table not committed yet.
   * - scylla_strong_consistency_raft_waiter_awoken
     - Number of waiters that got result back.
   * - scylla_strong_consistency_raft_waiter_dropped
     - Number of waiters that did not get result back.
   * - scylla_tablet_ops_cancelled_transitions
     - Number of tablet transitions which were cancelled and rolled back. Counted by the topology coordinator, so it restarts when the coordinator changes.


Renamed Metrics in |NEW_VERSION|
---------------------------------

The following metrics are renamed in ScyllaDB |NEW_VERSION| compared to |PRECEDING_VERSION|.

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric Name in |PRECEDING_VERSION|
     - Metric Name in |NEW_VERSION|
   * - scylla_alternator_vector_search_query_items_from_base_table
     - scylla_alternator_vector_search_items_from_base_table
   * - scylla_alternator_vector_search_query_items_from_vs
     - scylla_alternator_vector_search_items_from_vs
   * - scylla_alternator_vector_search_query_returned_items
     - scylla_alternator_vector_search_returned_items
   * - scylla_logstor_sm_segments_compacted
     - scylla_logstor_sm_compaction_segments_in
   * - scylla_logstor_sm_compaction_segments_freed
     - scylla_logstor_sm_compaction_segments_out


Removed Metrics in |NEW_VERSION|
---------------------------------

The following metrics are removed in ScyllaDB |NEW_VERSION|.

* scylla_alternator_vector_search_query
* scylla_logstor_sm_separator_flow_control_delay

New and Updated Metrics in Previous 2026.x Releases
-----------------------------------------------------

* `Metrics Update Between 2026.x and 2026.3 <https://docs.scylladb.com/manual/branch-2026.3/upgrade/upgrade-guides/upgrade-guide-from-2026.x-to-2026.3/metric-update-2026.x-to-2026.3.html>`_
* `Metrics Update Between 2026.1 and 2026.2 <https://docs.scylladb.com/manual/branch-2026.2/upgrade/upgrade-guides/upgrade-guide-from-2026.1-to-2026.2/metric-update-2026.1-to-2026.2.html>`_
