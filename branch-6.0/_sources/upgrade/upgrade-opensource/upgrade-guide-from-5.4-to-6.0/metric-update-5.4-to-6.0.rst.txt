.. |SRC_VERSION| replace:: 5.4
.. |NEW_VERSION| replace:: 6.0

ScyllaDB Metric Update - Scylla |SRC_VERSION| to |NEW_VERSION|
====================================================================

.. toctree::
   :maxdepth: 2
   :hidden:

ScyllaDB |NEW_VERSION| Dashboards are available as part of the latest |mon_root|.

The following metrics are new in ScyllaDB |NEW_VERSION|:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 150
   :header-rows: 1

   * - Metric
     - Description
   * - scylla_column_family_tablet_count
     - Tablet count
   * - scylla_cql_replication_strategy_fail_list_violations	
     - Counts the number of replication_strategy_fail_list guardrail violations,
       i.e., attempts to set a forbidden replication strategy in a keyspace via
       CREATE/ALTER KEYSPACE.
   * - scylla_cql_replication_strategy_warn_list_violations	
     - Counts the number of replication_strategy_warn_list guardrail violations,
       i.e., attempts to set a discouraged replication strategy in a keyspace
       via CREATE/ALTER KEYSPACE.
   * - scylla_load_balancer_resizes_emitted
     - Number of resizes produced by the load balancer
   * - scylla_load_balancer_resizes_finalized	
     - Number of resizes finalized by the load balancer.
   * - scylla_reactor_fstream_read_bytes_blocked	
     - Counts the number of bytes read from disk that could not be satisfied
       from read-ahead buffers, and had to block. Indicates short streams or
       incorrect read ahead configuration.
   * - scylla_reactor_fstream_read_bytes
     - Counts bytes read from disk file streams. A high rate indicates high disk
       activity. Divide by fstream_reads to determine the average read size.
   * - scylla_reactor_fstream_reads_ahead_bytes_discarded	
     - Counts the number of buffered bytes that were read ahead of time and were
       discarded because they were not needed, wasting disk bandwidth. Indicates
       over-eager read ahead configuration.
   * - scylla_reactor_fstream_reads_aheads_discarded
     - Counts the number of times a buffer that was read ahead of time and was
       discarded because it was not needed, wasting disk bandwidth. Indicates
       over-eager read ahead configuration.
   * - scylla_reactor_fstream_reads_blocked	
     - Counts the number of times a disk read could not be satisfied from
       read-ahead buffers, and had to block. Indicates short streams or
       incorrect read ahead configuration.
   * - scylla_reactor_fstream_reads	
     - Counts reads from disk file streams. A high rate indicates high disk
       activity. Contrast with other fstream_read* counters to locate bottlenecks.
   * - scylla_tablets_count	
     - Tablet count





