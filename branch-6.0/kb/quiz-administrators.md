# Scylla consistency quiz for administrators

**Topic: Architecture and development**

**Learn: Understanding consistency in Scylla: a quiz**

**Audience: Scylla administrators**

Q: When you run `nodetool decommission` to remove a node…

1. Will the token range the node had be relocated to other nodes?
   What about vnodes?
2. When and how will we know the “decommission” operation is finished?

A1. Yes. The node enters the state “STATE_LEAVING” while streaming its
data to other nodes. Then it is in “STATE_LEFT” when the data has been
streamed.

A2. Use `nodetool netstats` to check the state of a node.

Q: Let’s say I have a 16 node cluster using Network Topology Strategy
across 2 data centers. The replication factor is TWO in each datacenter
(DC1: 2, DC2: 2). If I write using a LOCAL_QUORUM, I will write the
data to 4 nodes (2 in each data center) but when will the
acknowledgement happen?

A: The acknowledgment will be sent to the user when two **local** replicas respond.
In this case, floor(local_RF/2 +1) = 2 , so LOCAL_QUORUM is equal to RF per DC, which is why RF of 3 is usually better.

[Knowledge Base](https://opensource.docs.scylladb.com/branch-6.0/kb/index.md)
