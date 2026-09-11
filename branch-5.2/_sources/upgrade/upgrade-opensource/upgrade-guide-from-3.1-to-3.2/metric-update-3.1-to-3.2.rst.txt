

Scylla Metric Update - Scylla 3.1 to 3.2
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 3.2 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in Scylla 3.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `scylla_database_schema_changed`
* `scylla_memtables_failed_flushes`
* `scylla_storage_proxy_coordinator_cas_read_contention`
* `scylla_storage_proxy_coordinator_cas_read_latency`
* `scylla_storage_proxy_coordinator_cas_read_timeouts`
* `scylla_storage_proxy_coordinator_cas_read_unavailable`
* `scylla_storage_proxy_coordinator_cas_read_unfinished_commit`
* `scylla_storage_proxy_coordinator_cas_write_condition_not_met`
* `scylla_storage_proxy_coordinator_cas_write_contention`
* `scylla_storage_proxy_coordinator_cas_write_latency`
* `scylla_storage_proxy_coordinator_cas_write_timeouts`
* `scylla_storage_proxy_coordinator_cas_write_unavailable`
* `scylla_storage_proxy_coordinator_cas_write_unfinished_commit`
* `scylla_storage_proxy_coordinator_reads_coordinator_outside_replica_set`
* `scylla_storage_proxy_coordinator_writes_coordinator_outside_replica_set`
* `scylla_transport_requests_memory_available`


The following metric was updated from Scylla 3.1 to Scylla 3.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 30
   :header-rows: 1

   * - Scylla 3.1 Name
     - Scylla 3.2 Name
   * - scylla_storage_proxy_coordinator_foreground_read_repair
     - scylla_storage_proxy_coordinator_foreground_read_repairs
