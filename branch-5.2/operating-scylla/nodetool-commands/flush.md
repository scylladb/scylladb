# Nodetool flush

**flush** `[<keyspace> <cfnames>...]`- Specify a keyspace and one or more tables that you want to flush from the memtable to on disk SSTables.

For example:

```shell
nodetool flush keyspaces1 standard1
```

See also

[Nodetool drain](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/drain.md)

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool.md)
