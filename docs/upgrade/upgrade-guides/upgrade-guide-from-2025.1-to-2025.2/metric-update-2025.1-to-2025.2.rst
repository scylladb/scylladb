.. |SRC_VERSION| replace:: 2025.1
.. |NEW_VERSION| replace:: 2025.2

Metrics Update Between |SRC_VERSION| and |NEW_VERSION|
================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.

New Metrics
------------

The following metrics are new in ScyllaDB |NEW_VERSION| compared to |SRC_VERSION|:


.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_alternator_batch_item_count_histogram
     - A histogram of the number of items in a batch request.
   * - scylla_database_total_view_updates_failed_pairing
     - Total number of view updates for which we failed base/view pairing.
   * - scylla_group_name_cross_rack_collocations
     - The number of co-locating migrations that move replica across racks.
   * - scylla_network_bytes_received
     - The number of bytes received from network sockets.
   * - scylla_network_bytes_sent
     - The number of bytes written to network sockets.
   * - scylla_reactor_awake_time_ms_total
     - Total reactor awake time (wall_clock).
   * - scylla_reactor_cpu_used_time_ms
     - Total reactor thread CPU time (from CLOCK_THREAD_CPUTIME).
   * - scylla_reactor_sleep_time_ms_total
     - Total reactor sleep time (wall clock).
   * - scylla_sstable_compression_dicts_total_live_memory_bytes
     - Total amount of memory consumed by SSTable compression dictionaries in RAM.
   * - scylla_transport_connections_blocked
     - Holds an incrementing counter with the CQL connections that were blocked
       before being processed due to threshold configured via
       uninitialized_connections_semaphore_cpu_concurrency.Blocks are normal
       when we have multiple connections initialized at once. If connectionsare
       timing out and this value is high it indicates either connections storm
       or unusually slow processing.
   * - scylla_transport_connections_shed
     - Holds an incrementing counter with the CQL connections that were shed
       due to concurrency semaphore timeout (threshold configured via
       uninitialized_connections_semaphore_cpu_concurrency). This typically can
       happen during connection.
   
  





