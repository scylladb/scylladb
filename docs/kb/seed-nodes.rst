===================
ScyllaDB Seed Nodes
===================

A ScyllaDB seed node is a node specified with the ``seeds`` configuration parameter in ``scylla.yaml``. It is used by new node joining as the first contact point.
It allows nodes to discover the cluster ring topology on startup (when joining the cluster). This means that any time a node is joining the cluster, it needs to learn the cluster ring topology, meaning:

-  What are the IPs of the nodes in the cluster are
-  Which token ranges are available
-  Which nodes will own which tokens when a new node joins the cluster

**Once the nodes have joined the cluster, the seed node has no function.**
     
The first node in a new cluster must be a seed node. In typical scenarios,
there's no need to configure more than one seed node.

