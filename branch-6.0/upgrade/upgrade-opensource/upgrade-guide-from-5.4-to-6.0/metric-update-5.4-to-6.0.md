# ScyllaDB Metric Update - Scylla 5.4 to 6.0

ScyllaDB 6.0 Dashboards are available as part of the latest [Scylla Monitoring Stack](https://monitoring.docs.scylladb.com).

## The following metrics are new in ScyllaDB 6.0:

| Metric                                               | Description                                                                                                                                                                                      |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scylla_column_family_tablet_count                    | Tablet count                                                                                                                                                                                     |
| scylla_cql_replication_strategy_fail_list_violations | Counts the number of replication_strategy_fail_list guardrail violations,<br/>i.e., attempts to set a forbidden replication strategy in a keyspace via<br/>CREATE/ALTER KEYSPACE.                |
| scylla_cql_replication_strategy_warn_list_violations | Counts the number of replication_strategy_warn_list guardrail violations,<br/>i.e., attempts to set a discouraged replication strategy in a keyspace<br/>via CREATE/ALTER KEYSPACE.              |
| scylla_load_balancer_resizes_emitted                 | Number of resizes produced by the load balancer                                                                                                                                                  |
| scylla_load_balancer_resizes_finalized               | Number of resizes finalized by the load balancer.                                                                                                                                                |
| scylla_reactor_fstream_read_bytes_blocked            | Counts the number of bytes read from disk that could not be satisfied<br/>from read-ahead buffers, and had to block. Indicates short streams or<br/>incorrect read ahead configuration.          |
| scylla_reactor_fstream_read_bytes                    | Counts bytes read from disk file streams. A high rate indicates high disk<br/>activity. Divide by fstream_reads to determine the average read size.                                              |
| scylla_reactor_fstream_reads_ahead_bytes_discarded   | Counts the number of buffered bytes that were read ahead of time and were<br/>discarded because they were not needed, wasting disk bandwidth. Indicates<br/>over-eager read ahead configuration. |
| scylla_reactor_fstream_reads_aheads_discarded        | Counts the number of times a buffer that was read ahead of time and was<br/>discarded because it was not needed, wasting disk bandwidth. Indicates<br/>over-eager read ahead configuration.      |
| scylla_reactor_fstream_reads_blocked                 | Counts the number of times a disk read could not be satisfied from<br/>read-ahead buffers, and had to block. Indicates short streams or<br/>incorrect read ahead configuration.                  |
| scylla_reactor_fstream_reads                         | Counts reads from disk file streams. A high rate indicates high disk<br/>activity. Contrast with other fstream_read\* counters to locate bottlenecks.                                            |
| scylla_tablets_count                                 | Tablet count                                                                                                                                                                                     |
