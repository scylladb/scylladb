==================================================
How does Scylla LWT Differ from Apache Cassandra ?
==================================================

Scylla is making an effort to be compatible with Cassandra, down to the level of limitations of the implementation. 
How is it different?

* Scylla most commonly uses fewer rounds than Cassandra to complete a lightweight transaction. While Cassandra issues a separate read query to fetch the old record, scylla piggybacks the read result on the response to the prepare round.
* Scylla will automatically use synchronous commit log write mode for all lightweight transaction writes. Before a lightweight transaction completes, scylla will ensure that the data in it has hit the device. This is done in all commitlog_sync modes.
* Conditional statements return a result set, and unlike Cassandra, Scylla result set metadata doesn’t change from execution to execution: Scylla always returns  the old version of the  row, regardless of whether the condition is true or not. This ensures conditional statements work well with prepared statements.
* For batch statement, the returned result set contains an old row for every conditional statement in the batch, in statement order. Cassandra returns results in clustering key order.
* Unlike Cassandra, Scylla  uses per-core data partitioning, so the RPC  that is done to perform a transaction talks directly to the right core on a peer replica, avoiding the concurrency overhead. This is,  of course, true, if Scylla’s own shard-aware driver is used - otherwise we  add an extra hop to the right core at the coordinator node.
* Scylla does not store  hints for lightweight transaction writes, since this is redundant as all such writes are already present in system.paxos table.


More on :doc:`Lightweight Transactions (LWT) </using-scylla/lwt>`
