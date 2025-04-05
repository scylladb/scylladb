Scylla Metric Update - Scylla 4.6 to 5.0
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 5.0 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in Scylla 5.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_cql_unprivileged_entries_evictions_on_size
     - Counts a number of evictions of prepared statements from the prepared statements cache after they have been used only once. An increasing counter suggests the user may be preparing a different statement for each request instead of reusing the same prepared statement with parameters.
   * - scylla_io_queue_starvation_time_sec
     - Total time spent starving for disk
   * - scylla_io_queue_total_read_bytes
     - Total read bytes passed in the queue
   * - scylla_io_queue_total_read_ops
     - Total read operations passed in the queue
   * - scylla_io_queue_total_write_bytes
     - Total write bytes passed in the queue
   * - scylla_io_queue_total_write_ops
     - Total write operations passed in the queue
   * - scylla_storage_proxy_coordinator_current_throttled_base_writes
     - number of currently throttled base replica write requests
   * - scylla_storage_proxy_coordinator_current_throttled_writes
     - number of currently throttled write requests
   * - scylla_storage_proxy_coordinator_foreground_read_repairs
     - number of foreground read repairs
   * - scylla_storage_proxy_coordinator_foreground_reads
     - number of currently pending foreground read requests
   * - scylla_storage_proxy_coordinator_foreground_writes
     - number of currently pending foreground write requests
   * - scylla_storage_proxy_coordinator_last_mv_flow_control_delay
     - delay (in seconds) added for MV flow control in the last request
   * - scylla_storage_proxy_coordinator_range_timeouts
     - number of range read operations failed due to a timeout
   * - scylla_storage_proxy_coordinator_range_unavailable
     - number of range read operations failed due to an "unavailable" error
   * - scylla_storage_proxy_coordinator_read_errors_local_node
     - number of data read requests that failedon a local Node
   * - scylla_storage_proxy_coordinator_read_latency
     - The general read latency histogram
   * - scylla_storage_proxy_coordinator_read_repair_write_attempts_local_node
     - number of write operations in a read repair contexton a local Node
