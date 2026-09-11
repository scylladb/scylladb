Failure to Update the Schema
------------------------------------------------

ScyllaDB relies on the Raft consensus algorithm, which requires at least a quorum 
of nodes in a cluster to be available. If some nodes are down and the quorum is 
lost, schema updates fail.

See :doc:`Handling Node Failures <handling-node-failures>` for information about 
recovery actions depending on the number of nodes and DCs in your cluster.