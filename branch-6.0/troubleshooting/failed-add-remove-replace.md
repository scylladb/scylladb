# Failure to Add, Remove, or Replace a Node

ScyllaDB relies on the Raft consensus algorithm, which requires at least a quorum
of nodes in a cluster to be available. If some nodes are down and the quorum is
lost, adding, removing, and replacing a node fails.

See [Handling Node Failures](https://opensource.docs.scylladb.com/branch-6.0/troubleshooting/handling-node-failures.md) for information about
recovery actions depending on the number of nodes and DCs in your cluster.
