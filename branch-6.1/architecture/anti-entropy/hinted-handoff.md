# ScyllaDB Hinted Handoff

A typical write in ScyllaDB works according to the scenarios described in our [Fault Tolerance documentation](https://opensource.docs.scylladb.com/branch-6.1/architecture/architecture-fault-tolerance.md).

But what happens when a write request is sent to a ScyllaDB node that is unresponsive due to reasons including heavy write load on a node, network issues, or even hardware failure? To ensure availability and consistency, ScyllaDB implements [hinted handoff](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Hinted-Handoff).

> [Hint](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Hint) = target replica ID + [mutation](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Mutation) data

In other words, ScyllaDB saves a copy of the writes intended for down nodes, and replays them to the nodes when they are up later.  Thus, the write operation flow, when a node is down, looks like this:

1. The co-ordinator determines all the replica nodes;
2. Based on the replication factor (RF) , the co-ordinator [attempts to write to RF nodes](https://opensource.docs.scylladb.com/branch-6.1/architecture/architecture-fault-tolerance.md);

![image](architecture/1-write_op_RF_3.jpg)
1. If one node is down, acknowledgments are only returned from two nodes:

![image](architecture/anti-entropy/hinted-handoff-3.png)
1. If the consistency level does not require responses from all replicas, the co-ordinator, *V* in this case,  will respond to the client that the write was successful.   The co-ordinator will write and store a [hint](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Hint) for the missing node:

![image](architecture/anti-entropy/hinted-handoff-4.png)
1. Once the down node comes up, the co-ordinator will replay the hint for that node.  After the co-ordinator receives an acknowledgement of the write, the hint is deleted.

![image](architecture/anti-entropy/hinted-handoff-5.png)

A co-ordinator stores hints for a handoff under the following conditions:

1. For down nodes;
2. If the replica doesn’t respond within `write_request_timeout_in_ms`.

The co-ordinator will stop creating any hints for a dead node if the node’s downtime is greater than `max_hint_window_in_ms`.

Hinted handoff is enabled and managed by these settings in `scylla.yaml`:

* `hinted_handoff_enabled`: enables or disables the hinted handoff feature completely or enumerates data centers where hints are allowed. By default, “true” enables hints to all nodes.
* `max_hint_window_in_ms`: do not generate hints if the destination node has been down for more than this value.  If  a node is down longer than this period, new hints are not created.  Hint generation resumes once the destination node is back up. By default, this is set to 3 hours.
* `hints_directory`: the directory where ScyllaDB will store hints. By default this is `$SCYLLA_HOME/hints`.

Storing of the hint can also fail. Enabling hinted handoff therefore does not eliminate the need for repair; a user must recurrently [run a full repair](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/procedures/maintenance/repair.md) to ensure data consistency across the cluster nodes.

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
