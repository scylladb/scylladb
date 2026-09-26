# SSTable 3.0 Format in ScyllaDB

Scylla supports the same SSTable format as Apache Cassandra 3.0.
You can simply place SSTables from a Cassandra data directory into a Scylla uploads directory
and use the `nodetool refresh` command to ingest their data into the table.

Looking more carefully, you will see that Scylla maintains more,
smaller, SSTables than Cassandra does. On Scylla, each core manages its
own subset of SSTables. This internal sharding allows each core (shard)
to work more efficiently, avoiding the complexity and delays of multiple
cores competing for the same data

[Scylla Architecture](https://opensource.docs.scylladb.com/branch-5.2/architecture/index.md)

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
