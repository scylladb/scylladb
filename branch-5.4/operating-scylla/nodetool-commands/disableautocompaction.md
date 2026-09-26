# Nodetool disableautocompaction

**disableautocompaction** disables automatic compaction for the given keyspace and table according to its compaction strategy.

For example:

```default
nodetool disableautocompaction keyspace.table
```

## Syntax

```none
nodetool disableautocompaction [<keyspace> <tables>...]
```

nodetool disableautocompaction takes the following parameters:

| Parameter Name             | Description                                 |
|----------------------------|---------------------------------------------|
| `[<keyspace> <tables>...]` | The keyspace followed by one or many tables |

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/nodetool.md)

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
