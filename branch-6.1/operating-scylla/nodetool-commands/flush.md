# Nodetool flush

**flush** - Flush memtables to on-disk SSTables in the specified keyspace and table(s).

For example:

```shell
nodetool flush
nodetool flush keyspace1
nodetool flush keyspace1 standard1
```

## Syntax

```none
nodetool flush [<keyspace> [<table> ...]]
```

nodetool flush takes the following parameters:

| Parameter Name   | Description                                                                                                                                            |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<keyspace>`     | The keyspace to operate on.  If omitted, all keyspaces are flushed.                                                                                    |
| `<table> ...`    | One or more tables to operate on.  Tables may be specified only if a keyspace is given.  If omitted, all tables in the specified keyspace are flushed. |

See also

[Nodetool drain](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool-commands/drain.md)

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md)
