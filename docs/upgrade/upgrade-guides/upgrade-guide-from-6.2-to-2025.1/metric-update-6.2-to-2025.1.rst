.. |SRC_VERSION| replace:: 6.2
.. |NEW_VERSION| replace:: 2025.1

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
   * - scylla_alternator_rcu_total 
     - The total number of consumed read units, counted as half units.
   * - scylla_alternator_wcu_total
     - The total number of consumed write units, counted as half units.
   * - scylla_rpc_compression_bytes_received
     - The bytes read from RPC connections after decompression.
   * - scylla_rpc_compression_bytes_sent
     - The bytes written to RPC connections before compression.
   * - scylla_rpc_compression_compressed_bytes_received
     - The bytes read from RPC connections before decompression.
   * - scylla_rpc_compression_compressed_bytes_sent
     - The bytes written to RPC connections after compression.
   * - scylla_rpc_compression_compression_cpu_nanos
     - The nanoseconds spent on compression.
   * - scylla_rpc_compression_decompression_cpu_nanos
     - The nanoseconds spent on decompression.
   * - scylla_rpc_compression_messages_received
     - The RPC messages received.
   * - scylla_rpc_compression_messages_sent	
     - The RPC messages sent.




  





