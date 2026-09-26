# Cluster membership changes and LWT consistency

In Scylla, membership changes, or topology changes, is an operation that requires a redistribution of the token ring ranges. Examples are removing a node or a Data Center (DC) or adding a node or a DC.

## Do not run more than one operation at a time

Scylla guarantees that [LWT Operations](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/lwt.md) will be strictly serializable during a single membership change. Parallel membership changes will violate this guarantee.
Human or software-based operators should verify that no parallel membership changes are executed if the following guarantee is required.

## Each node may have a different membership view

In Scylla, Membership is managed in a distributed fashion using [Gossip](https://opensource.docs.scylladb.com/branch-5.1/kb/gossip.md); thus, each node’s cluster view may be different. A membership operation may assume done by one node and may still be in progress by another. Operators, human or software agents, must verify that all live nodes share the same view of the cluster before proceeding to another membership change view.
On the occasion of a node removal with no consensus about its state, the operator must verify that the node would not ever come back to life under any circumstances. We recommend executing destructive measures such as destroying the VM and wait on a successful api approval of such an operation before assuming the membership operation was completed. More on [safe node removal](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/remove-node.md).
