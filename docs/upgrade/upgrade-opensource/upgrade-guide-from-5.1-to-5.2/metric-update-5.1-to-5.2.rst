ScyllaDB Metric Update - Scylla 5.1 to 5.2
============================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 5.2 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in ScyllaDB 5.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_database_disk_reads
     - Holds the number of currently active disk read operations.
   * - scylla_database_sstables_read
     - Holds the number of currently read sstables.
   * - scylla_memory_malloc_failed
     - Total count of failed memory allocations
   * - scylla_raft_group0_status
     - status of the raft group, 0 - disabled, 1 - normal, 2 - aborted
   * - scylla_storage_proxy_coordinator_cas_read_latency_summary
     - CAS read latency summary
   * - scylla_storage_proxy_coordinator_cas_write_latency_summary
     - CAS write latency summary
   * - scylla_storage_proxy_coordinator_read_latency_summary
     - Read latency summary
   * - scylla_storage_proxy_coordinator_write_latency_summary
     - Write latency summary
   * - scylla_streaming_finished_percentage
     - Finished percentage of node operation on this shard
   * - scylla_view_update_generator_sstables_pending_work
     - Number of bytes remaining to be processed from SSTables for view updates


The following metrics are renamed in ScyllaDB 5.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - 5.1
     - 5.2
   * - scylla_database_active_reads_memory_consumption
     - scylla_database_reads_memory_consumption
   * - scylla_memory_regular_virtual_dirty_bytes
     - scylla_memory_regular_unspooled_dirty_bytes
   * - scylla_memory_system_virtual_dirty_bytes
     - scylla_memory_system_unspooled_dirty_bytes
   * - scylla_memory_virtual_dirty_bytes
     - scylla_memory_unspooled_dirty_bytes

Reporting Latencies
~~~~~~~~~~~~~~~~~~~~~~~~

ScyllaDB 5.2 comes with a new approach to reporting latencies, which are reported using histograms and summaries:

* Histograms are reported per node.
* Summaries are reported per shard and contain P50, P95, and P99 latency.

For more information on Prometheus histograms and summaries, see the `Prometheus documentation <https://prometheus.io/docs/practices/histograms/>`_.