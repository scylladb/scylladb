.. |SRC_VERSION| replace:: 2024.x
.. |NEW_VERSION| replace:: 2025.1

=======================================================================================
Metrics Update Between |SRC_VERSION| and |NEW_VERSION|
=======================================================================================

ScyllaDB Enterprise |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.


New Metrics
------------

The following metrics are new in ScyllaDB |NEW_VERSION| compared to |SRC_VERSION|:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_alternator_batch_item_count
     - The total number of items processed across all batches.
   * - scylla_hints_for_views_manager_sent_bytes_total
     - The total size of the sent hints (in bytes).
   * - scylla_hints_manager_sent_bytes_total
     - The total size of the sent hints (in bytes).
   * - scylla_io_queue_activations
     - The number of times the class was woken up from idle.
   * - scylla_raft_apply_index
     - The applied index.
   * - scylla_raft_commit_index
     - The commit index.
   * - scylla_raft_log_last_index
     - The index of the last log entry.
   * - scylla_raft_log_last_term
     - The term of the last log entry.
   * - scylla_raft_snapshot_last_index
     - The index of the snapshot.
   * - scylla_raft_snapshot_last_term
     - The term of the snapshot.
   * - scylla_raft_state
     - The current state: 0 - follower, 1 - candidate, 2 - leader
   * - scylla_rpc_client_delay_samples
     - The total number of delay samples.
   * - scylla_rpc_client_delay_total
     - The total delay in seconds.
   * - scylla_storage_proxy_replica_received_hints_bytes_total
     - The total size of hints and MV hints received by this node.
   * - scylla_storage_proxy_replica_received_hints_total
     - The number of hints and MV hints received by this node.

Renamed Metrics
------------------

The following metrics are renamed in ScyllaDB |NEW_VERSION| compared to |SRC_VERSION|:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - 2024.2
     - 2025.1
   * - scylla_hints_for_views_manager_sent
     - scylla_hints_for_views_manager_sent_total
   * - scylla_hints_manager_sent
     - scylla_hints_manager_sent_total
   * - scylla_forward_service_requests_dispatched_to_other_nodes
     - scylla_mapreduce_service_requests_dispatched_to_other_nodes
   * - scylla_forward_service_requests_dispatched_to_own_shards
     - scylla_mapreduce_service_requests_dispatched_to_own_shards
   * - scylla_forward_service_requests_executed
     - scylla_mapreduce_service_requests_executed
  
