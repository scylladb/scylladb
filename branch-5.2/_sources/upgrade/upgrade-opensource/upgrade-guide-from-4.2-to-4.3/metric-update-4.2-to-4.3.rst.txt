

Scylla Metric Update - Scylla 4.2 to 4.3
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 4.3 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in Scylla 4.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Commit Logs Metrics
^^^^^^^^^^^^^^^^^^^

* scylla_commitlog_disk_active_bytes

Hinted Handoff Metrics
^^^^^^^^^^^^^^^^^^^^^^
  
* scylla_hints_for_views_manager_pending_drains
* scylla_hints_for_views_manager_pending_sends
* scylla_hints_manager_pending_drains
* scylla_hints_manager_pending_sends


Cluster Operation  Metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^
  
* scylla_node_maintenance_operations_bootstrap_finished_percentage
* scylla_node_maintenance_operations_decommission_finished_percentage
* scylla_node_maintenance_operations_rebuild_finished_percentage
* scylla_node_maintenance_operations_removenode_finished_percentage
* scylla_node_maintenance_operations_repair_finished_percentage
* scylla_node_maintenance_operations_replace_finished_percentage

Repair Metrics
^^^^^^^^^^^^^^

* scylla_repair_row_from_disk_bytes
* scylla_repair_row_from_disk_nr
* scylla_repair_rx_hashes_nr
* scylla_repair_rx_row_bytes
* scylla_repair_rx_row_nr
* scylla_repair_tx_hashes_nr
* scylla_repair_tx_row_bytes
* scylla_repair_tx_row_nr

More metrics
^^^^^^^^^^^^

* scylla_storage_proxy_coordinator_total_write_attempts_remote_node
* scylla_transport_requests_shed
* scylla_view_builder_builds_in_progress
* scylla_view_builder_pending_bookkeeping_ops
* scylla_view_builder_steps_failed
* scylla_view_update_generator_pending_registrations
* scylla_view_update_generator_queued_batches_count
* scylla_view_update_generator_sstables_to_move_count

