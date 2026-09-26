# Nodetool compact

Forces a (major) compaction on one or more tables.
Compaction is an optimization that reduces the cost of IO and CPU over time by merging rows in the background.

By default, major compaction runs on all the `keyspaces` and tables.
Major compactions will take all the SSTables for a column family and merge them into a **single SSTable per shard**.
If a keyspace is provided, the compaction will run on all of the tables within that keyspace. If one or more tables are provided as command-line arguments, the compaction will run on all tables.

## Syntax

```console
nodetool [options] compact [--partition <partition_key>] [<keyspace> [<cfnames>]...]
```

## Options

* `-h <host>` or  `--host <host>` - Node hostname or IP address.
* `-p <port>` or `--port <port>` - Remote JMX agent port number.
* `--partition <partition_key>` - String representation of the partition key.
* `-pp` or `--print-port` - Operate in 4.0 mode with hosts disambiguated by port number.
* `-pw <password>` or `--password <password>` - Remote JMX agent password.
* `-pwf <passwordFilePath>` or `--password-file <passwordFilePath>` - Path to the JMX password file.
* `-u <username>` or `--username <username>` - Remote JMX agent username.
* `--` - Separates command-line options from the list of argument(useful when an argument might be mistaken for a command-line option).

The following options are NOT supported:

* `-st` or `--start-token`
* `-et` or `--end-token`
* `--user-defined`
* `--split-output`

## Examples

```shell
nodetool compact
nodetool compact keyspace1
nodetool compact standard1
```

## See Also

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool.md)

[Compaction Overview](https://opensource.docs.scylladb.com/branch-5.1/kb/compaction.md)

[CQL compaction Reference](https://opensource.docs.scylladb.com/branch-5.1/cql/compaction.md)

[How to choose a Compaction Strategy](https://opensource.docs.scylladb.com/branch-5.1/architecture/compaction/compaction-strategies.md)
