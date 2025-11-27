.. |SRC_VERSION| replace:: 2025.x
.. |NEW_VERSION| replace:: 2025.4
.. |PRECEDING_VERSION| replace:: 2025.3

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
   * - scylla_database_total_view_updates_due_to_replica_count_mismatch	
     - The total number of view updates for which there were more view replicas
       than base replicas and we had to generate an extra view update because
       the additional view replica wouldn't get paired with any base replica.
       It should only increase during the Replication Factor (RF) change. It
       should stop increasing shortly after finishing the RF change.
   * - scylla_database_total_writes_rejected_due_to_out_of_space_prevention
     - Counts write operations that were rejected due to disabled user tables
       writes.
   * - scylla_index_query_latencies
     - Index query latencies.
   * - scylla_reactor_aio_retries
     - The total number of IOCB-s re-submitted via thread-pool.
   * - scylla_reactor_io_threaded_fallbacks
     - The total number of io-threaded-fallbacks operations.
   * - scylla_repair_inc_sst_read_bytes
     - The total number of bytes read from SStables for incremental repair
       on this shard.
   * - scylla_repair_inc_sst_skipped_bytes
     - The total number of bytes skipped from SStables for incremental repair
       on this shard.
   * - scylla_repair_tablet_time_ms
     - The time spent on tablet repair on this shard (in milliseconds).
   * - scylla_s3_downloads_blocked_on_memory
     - Counts the number of times the S3 client downloads were delayed due to
       insufficient memory availability.
   * - scylla_s3_memory_usage
     - The total number of bytes consumed by the S3 client.
   * - scylla_s3_total_read_prefetch_bytes
     - The total number of bytes requested from object.
   * - scylla_storage_proxy_replica_fenced_out_requests
     - The number of requests that resulted in a stale_topology_exception.
   * - scylla_vector_store_dns_refreshes	
     - The number of DNS refreshes.

New and Updated Metrics in Previous 2025.x Releases
-------------------------------------------------------

* `Metrics Update Between 2025.2 and 2025.3 <https://docs.scylladb.com/manual/branch-2025.3/upgrade/upgrade-guides/upgrade-guide-from-2025.2-to-2025.3/metric-update-2025.2-to-2025.3.html>`_
* `Metrics Update Between 2025.1 and 2025.2 <https://docs.scylladb.com/manual/branch-2025.2/upgrade/upgrade-guides/upgrade-guide-from-2025.1-to-2025.2/metric-update-2025.1-to-2025.2.html>`_


