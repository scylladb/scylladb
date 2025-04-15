Gossip in ScyllaDB
==================
**Topic: Internals**

**Audience: Devops professionals, architects**

ScyllaDB, like Apache Cassandra, uses a type of protocol called “gossip” to exchange metadata about the identities of nodes in a cluster and whether nodes are up or down. Of course, since there is no single point of failure there can be no single registry of node state, so nodes must share information among themselves.

Gossip protocols are only required in distributed systems so are probably new to most administrators. `According to Wikipedia <https://en.wikipedia.org/wiki/Gossip_protocol>`_, the ideal gossip protocol has several qualities:

* Gossip involves periodic, pairwise interactions between nodes
* Information exchanged between nodes is of bounded size.
* The state of at least one agent changes to reflect the state of the other.
* Reliable communication is not assumed.
* The frequency of the interactions is low compared to typical message latencies so that the protocol costs are negligible.
* There is some form of randomness in the peer selection.
* Due to the replication there is an implicit redundancy of the delivered information.

Individual gossip interactions in ScyllaDB, like Apache Cassandra, are relatively infrequent and simple. 
Each node, once per second, randomly selects 1 to 3 nodes to interact with.

Each node runs the gossip protocol once per second, but the gossip runs are not synchronized across the cluster.

One round = three messages
--------------------------
One round of gossip consists of three messages. (We’ll call the node initiating the round Node A, and the randomly selected node Node B).

* Node A sends: gossip_digest_syn
* Node B sends: gossip_digest_ack
* Node A replies: gossip_digest_ack2

While the names are borrowed from TCP, gossip does not require making a new TCP connection between nodes.

What are nodes gossiping about?
-------------------------------
Nodes exchange a small amount of information about each other. The main two data structures are heart_beat_state and application_state.

A heart_beat_state contains integers for generation and “version number”. The generation is a number that grows each time the node is started, and version number is an ever-increasing integer that covers the version of the application state. ApplicationState contains data on status of components within the node (such as load) and a version number. Each node maintains a map of node IP address and node gossip metadata for all nodes in the cluster including itself.

A round of gossip is designed to minimize the amount of data sent, while resolving any conflicts between the node state data on the two gossiping nodes. In the gossip_digest_syn message, Node A sends a gossip digest: a list of all its known nodes, generations, and versions. Node B compares generation and version to its known nodes, and, in the gossip_digest_ack message, sends any of its own data that differ, along with its own digest. Finally, Node A replies with any state differences between its known state and Node B’s digest.

ScyllaDB gossip implementation
------------------------------
ScyllaDB gossip messages run over the ScyllaDB messaging_service, along with all other inter-node traffic including sending mutations, and streaming of data. ScyllaDB’s messaging_service runs on the Seastar RPC service. Seastar is the scalable software framework for multicore systems that ScyllaDB uses. If no TCP connection is up between a pair of nodes, messaging_service will create a new one. If it is up already, messaging service will use the existing one.

Gossip on multicore
-------------------
Each ScyllaDB node consists of several independent shards, one per core, which operate on a shared-nothing basis and communicate without locking. Internally, the gossip component, which runs on CPU 0 only, needs to have connections forwarded from other shards. The node state data, shared by gossip, is replicated to the other shards.

The gossip protocol provides important advantages especially for large clusters. Compared to “flooding” information across nodes, it can synchronize data faster, and allow for fast recovery when a new node is down or a node is returned to service. Nodes only mark other nodes as down if an actual failure is detected, but gossip quickly shares the good news of a node coming back up.

References
----------
`Cassandra Wiki: ArchitectureGossip <https://cwiki.apache.org/confluence/display/CASSANDRA2/ArchitectureGossip>`_

`Apple Inc.: Cassandra Internals — Understanding Gossip <https://www.youtube.com/watch?v=FuP1Fvrv6ZQ&list=PLqcm6qE9lgKJkxYZUOIykswDndrOItnn2&index=49>`_

`Using Gossip Protocols For Failure Detection, Monitoring, Messaging And Other Good Things, by Todd Hoff <http://highscalability.com/blog/2011/11/14/using-gossip-protocols-for-failure-detection-monitoring-mess.html>`_

`Gossip protocol on Wikipedia <https://en.wikipedia.org/wiki/Gossip_protocol>`_

:doc:`Knowledge Base </kb/index>`


.. include:: /rst_include/apache-copyrights.rst
