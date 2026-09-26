# Nodetool enableautocompaction

**enableautocompaction** enables automatic compaction for the given keyspace and table(s).

For example:

```default
nodetool enableautocompaction keyspace1 standard1
```

## Syntax

```none
nodetool enableautocompaction [<keyspace> [<tables>...]]
```

nodetool enableautocompaction takes the following parameters:

| Parameter Name   | Description                                                                                                                                                                                             |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<keyspace>`     | The keyspace to operate on.  If omitted, auto-compaction will be enabled in all keyspaces.                                                                                                              |
| `<tables>...`    | A comma-separated list of one or more tables to operate on.  Tables may be specified only if a keyspace is given.  If omitted, auto-compaction will be enabled in all tables in the specified keyspace. |

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md)

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
