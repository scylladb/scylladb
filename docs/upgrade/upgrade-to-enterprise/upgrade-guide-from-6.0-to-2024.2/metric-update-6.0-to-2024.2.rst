.. |SRC_VERSION| replace:: 6.0
.. |NEW_VERSION| replace:: 2024.2

=======================================================================================
ScyllaDB Metric Update - ScyllaDB |SRC_VERSION| to ScyllaDB Enterprise |NEW_VERSION|
=======================================================================================

ScyllaDB Enterprise |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.


New Metrics
------------

The following metrics are new in ScyllaDB |NEW_VERSION|:

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_rpc_compression_bytes_received
     - Bytes read from RPC connections (before decompression).
   * - scylla_rpc_compression_bytes_sent
     - Bytes written to RPC connections (after compression).
   * - scylla_rpc_compression_compressed_bytes_received
     - RPC messages received.
   * - scylla_rpc_compression_compressed_bytes_sent
     - RPC messages sent.
   * - scylla_rpc_compression_compression_cpu_nanos
     - Nanoseconds spent on compression.
   * - scylla_rpc_compression_decompression_cpu_nanos
     - Nanoseconds spent on decompression.
   * - scylla_rpc_compression_messages_received
     - Size of backlog on this queue, in tasks; indicates whether the queue is
       busy and/or contended.
   * - scylla_rpc_compression_messages_sent
     - Accumulated runtime of this task queue; an increment rate of 1000ms per
       second indicates full utilization.


