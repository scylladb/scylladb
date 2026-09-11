.. |SRC_VERSION| replace:: 6.0
.. |NEW_VERSION| replace:: 6.1

ScyllaDB Metric Update - ScyllaDB |SRC_VERSION| to |NEW_VERSION|
================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.

New Metrics
------------

The following metrics are new in ScyllaDB |NEW_VERSION|:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_database_total_view_updates_on_wrong_node
     - The total number of view updates which are computed on the wrong node.
   * - scylla_raft_apply_index
     - The applied index.
   * - scylla_raft_commit_index	
     - The commit index.
   * - scylla_raft_log_last_term
     - The term of the last log entry.
   * - scylla_raft_log_last_index
     - The index of the last log entry.
   * - scylla_raft_snapshot_last_index
     - The index of the snapshot.
   * - scylla_raft_snapshot_last_term
     - The term of the snapshot.
   * - scylla_raft_state
     - The current state: 0 - follower, 1 - candidate, 2 - leader
   * - scylla_storage_proxy_replica_received_hints_bytes_total
     - The total size of hints and MV hints received by this node.
   * - scylla_storage_proxy_replica_received_hints_total
     - The number of hints and MV hints received by this node.
   * - scylla_storage_proxy_stats::REPLICA_STATS_CATEGORY_view_update_backlog
     - Tracks the size of ``scylla_database_view_update_backlog`` and is used
       instead of that one to calculate the max backlog across all shards, which
       is then used by other nodes to calculate appropriate throttling delays if it grows
       too large. If it's notably different from ``scylla_database_view_update_backlog``,
       it means that we're currently processing a write that generated a large number
       of view updates.

  





