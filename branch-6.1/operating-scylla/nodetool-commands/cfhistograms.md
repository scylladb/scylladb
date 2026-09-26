# Nodetool cfhistograms (tablehistograms)

**cfhistograms** `<keyspace> <cfname>`- This command provides statistics about a table, including number of SSTables, read/write latency, partition size and column count. cfhistograms covers all operations since the last time you ran the nodetool cfhistograms command. Note that this command can also be used for `tablehistograms`. This command uses the following syntax:

```shell
nodetool cfhistograms keyspaces cfname
```

For example: to get statistics about the `standard1` table in the `keyspaces1` keyspace, use this command:

```shell
nodetool cfhistograms keyspaces1 standard1
```

| Percentile   |   SSTables value |   Write Latency |   Read Latency |   Partition Size |   Cell Count |
|--------------|------------------|-----------------|----------------|------------------|--------------|
| 50%          |               10 |              42 |             20 |              210 |            5 |
| 75%          |               20 |              42 |             24 |              280 |            5 |
| 95%          |               25 |              42 |             42 |              300 |            5 |
| 98%          |               25 |              42 |            770 |              310 |            5 |
| 99%          |               26 |              42 |           2759 |              310 |            5 |
| Min          |                0 |              36 |              7 |              210 |            5 |
| Max          |               26 |              42 |          35425 |              310 |            5 |

In this example, 95% of the write latency measurements were 42 microseconds (μs) or less, 95% of the read latencies were 42 μs or less, and 95% of the partitions are 300 bytes or less. The SSTables column means that the 95% of all single-key read requests touched at most 25 SSTables.

| Parameter      | Description                             |
|----------------|-----------------------------------------|
| SSTables       | Distribution number of sstable per read |
| Write Latency  | Distribution of the Write Latency       |
| Read Latency   | Distribution of the Read Latency        |
| Partition Size | Estimated row size                      |
| Cell Count     | Estimated column count                  |

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md)
