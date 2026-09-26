# Scylla Anti-Entropy

Scylla replicates data according to [eventual consistency](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Eventual-Consistency).  This means that, in Scylla, when considering the [CAP Theorem](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-CAP-Theorem), availability and partition tolerance are considered a higher priority over consistency. Although Scylla’s tunable consistency allows users to make a tradeoff between availability and consistency,  Scylla’s [consistency level](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Consistency-Level-CL) is tunable per query.

However, over time, there can be a number of reasons for data inconsistencies, including:

1. a down node;
2. a network partition;
3. dropped mutations;
4. process crashes (before a flush);
5. a replica that cannot write due to being out of resources;
6. file corruption.

To mitigate [entropy](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Entropy), or data inconsistency, Scylla uses a few different processes.  The goal of Scylla [anti-entropy](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Anti-entropy) - based on that of Apache Cassandra  -  is to compare data on all replicas, synchronize data between all replicas,  and, finally,  ensure each replica has the most recent data.

Anti-entropy measures include *write-time* changes such as [hinted handoff](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Hinted-Handoff), *read-time* changes such as [read repair](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Read-Repair), and finally, periodic maintenance via [repair](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Repair).

* [Scylla Hinted Handoff](https://opensource.docs.scylladb.com/branch-6.0/architecture/anti-entropy/hinted-handoff.md) - High-Level view of Scylla Hinted Handoff
* [Scylla Read Repair](https://opensource.docs.scylladb.com/branch-6.0/architecture/anti-entropy/read-repair.md) - High-Level view of Scylla Read Repair
* [Scylla Repair](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/maintenance/repair.md) - Description of Scylla Repair

Also learn more in the [Cluster Management, Repair and Scylla Manager lesson](https://university.scylladb.com/courses/scylla-operations/lessons/cluster-management-repair-and-scylla-manager/topic/cluster-management-repair-and-scylla-manager/) on Scylla University.

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
