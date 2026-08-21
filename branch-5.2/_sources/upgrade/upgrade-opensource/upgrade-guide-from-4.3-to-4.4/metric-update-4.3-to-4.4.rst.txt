

Scylla Metric Update - Scylla 4.3 to 4.4
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 4.4 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in Scylla 4.4
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_database_reads_shed_due_to_overload
* scylla_scheduler_starvetime_ms
* scylla_scheduler_waittime_ms
* scylla_storage_proxy_coordinator_completed_reads_remote_node
* scylla_storage_proxy_coordinator_read_repair_write_attempts_remote_node
* scylla_storage_proxy_coordinator_reads_remote_node
* scylla_storage_proxy_coordinator_writes_failed_due_to_too_many_in_flight_hints
* scylla_transport_auth_responses
* scylla_transport_batch_requests
* scylla_transport_cql_errors_total
* scylla_transport_execute_requests
* scylla_transport_options_requests
* scylla_transport_prepare_requests
* scylla_transport_query_requests
* scylla_transport_register_requests
* scylla_transport_startups
* scylla_view_builder_steps_performed

The following metrics are removed in Scylla 4.4
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_memory_streaming_dirty_bytes
* scylla_memory_streaming_virtual_dirty_bytes
* scylla_node_maintenance_operations_bootstrap_finished_percentage
* scylla_node_maintenance_operations_decommission_finished_percentage
* scylla_node_maintenance_operations_rebuild_finished_percentage
* scylla_node_maintenance_operations_removenode_finished_percentage
* scylla_node_maintenance_operations_repair_finished_percentage
* scylla_node_maintenance_operations_replace_finished_percentage
* scylla_repair_row_from_disk_bytes
* scylla_repair_row_from_disk_nr
* scylla_repair_rx_hashes_nr
* scylla_repair_rx_row_bytes
* scylla_repair_rx_row_nr
* scylla_repair_tx_hashes_nr
* scylla_repair_tx_row_bytes
* scylla_repair_tx_row_nr
