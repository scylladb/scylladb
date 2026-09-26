# Nodetool compact

Forces a (major) compaction on one or more tables.
Compaction is an optimization that reduces the cost of IO and CPU over time by merging rows in the background.

By default, major compaction runs on all the `keyspaces` and tables.
Major compactions will take all the SSTables for a column family and merge them into a **single SSTable per shard**.
If a keyspace is provided, the compaction will run on all of the tables within that keyspace. If one or more tables are provided as command-line arguments, the compaction will run only on those tables.

## Syntax

```console
nodetool [options] compact [<keyspace> [<cfnames>]...]
```

## Options

The following options are available in Cassandra’s nodetool, but are NOT implemented in ScyllaDB’s nodetool:

* `-st` or `--start-token`
* `-et` or `--end-token`
* `--user-defined`
* `--split-output`

## Examples

```shell
nodetool compact
nodetool compact keyspace1
nodetool compact keyspace1 standard1
```

## See Also

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool.md)

[Compaction Overview](https://opensource.docs.scylladb.com/branch-6.0/kb/compaction.md)

[CQL compaction Reference](https://opensource.docs.scylladb.com/branch-6.0/cql/compaction.md)

[How to choose a Compaction Strategy](https://opensource.docs.scylladb.com/branch-6.0/architecture/compaction/compaction-strategies.md)
