# ScyllaDB Metric Update - ScyllaDB 6.0 to ScyllaDB Enterprise 2024.2

ScyllaDB Enterprise 2024.2 Dashboards are available as part of the latest [Scylla Monitoring Stack](https://monitoring.docs.scylladb.com).

## New Metrics

The following metrics are new in ScyllaDB 2024.2:

| Metric                                           | Description                                                                                                    |
|--------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| scylla_rpc_compression_bytes_received            | Bytes read from RPC connections (before decompression).                                                        |
| scylla_rpc_compression_bytes_sent                | Bytes written to RPC connections (after compression).                                                          |
| scylla_rpc_compression_compressed_bytes_received | RPC messages received.                                                                                         |
| scylla_rpc_compression_compressed_bytes_sent     | RPC messages sent.                                                                                             |
| scylla_rpc_compression_compression_cpu_nanos     | Nanoseconds spent on compression.                                                                              |
| scylla_rpc_compression_decompression_cpu_nanos   | Nanoseconds spent on decompression.                                                                            |
| scylla_rpc_compression_messages_received         | Size of backlog on this queue, in tasks; indicates whether the queue is<br/>busy and/or contended.             |
| scylla_rpc_compression_messages_sent             | Accumulated runtime of this task queue; an increment rate of 1000ms per<br/>second indicates full utilization. |
