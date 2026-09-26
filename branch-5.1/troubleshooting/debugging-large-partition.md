# Large Partitions Hunting

This document describes how to catch large partitions.

## What Should Make You Want To Start Looking For A Large Partition?

Any of the following:

* Latencies on a single shard become very long (look at the “Scylla Overview Metrics” dashboard of [ScyllaDB Monitoring Stack](https://monitoring.docs.scylladb.com/stable/)).
* Oversized allocation warning messages in the log:
  ```none
  seastar_memory - oversized allocation: 2842624 bytes, please report
  ```
* A warning of “too many rows” is issued when writing to a table (usually happens during a compaction):
  ```none
  Nov 26 07:36:29 hostname scylla[24314]:  [shard 9] large_partition - Writing a partition with too many rows [Some_KS/Some_table:PK_VAL1] (211663 rows)
  Nov 26 08:36:19 hostname scylla[24314]:  [shard 34] large_partition - Writing a partition with too many rows [Some_KS/Some_table:PK_VAL2] (171994 rows)
  ```

  In this case, refer to [Troubleshooting Large Partition Tables](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/large-partition-table.md#large-partition-table-configure) for more information.

## What To Do When You Suspect You May Have A Large Partition?

For each table you suspect run:

```console
nodetool flush <keyspace name> <table name>
nodetool cfstats <keyspace name>.<table name> | grep "Compacted partition maximum bytes"
```

For example:

```console
nodetool cfstats demodb.tmcr | grep "Compacted partition maximum bytes"

                Compacted partition maximum bytes: 1188716932
```

## Using system tables to detect large partitions, rows, or cells

Starting from scylla 2.3, large partitions are listed in the `system.large_partitions` table.  See [Scylla Large Partitions Table](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/large-partition-table.md) for more information.

Starting from scylla 3.1, large rows and large cells are listed similarly in the `system.large_rows` and `system.large_cells` tables, respectively.  See [Scylla Large Rows and Cells Tables](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/large-rows-large-cells-tables.md) for more information.

## When Compaction Creates an Error

When compaction or writing to a table results in a “Writing a partition with too many rows” warning:

This warning indicates that there is a huge multi-row partition (based on the number of rows) and it is orthogonal
to the size-based warnings. The warning is controlled by `compaction_rows_count_warning_threshold`, which is set in the scylla.yaml.
See [Troubleshooting Large Partition Tables](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/large-partition-table.md#large-partition-table-configure) for more information.
