===============================
How to Avoid Node Mismanagement
===============================

This document contains a few procedures to avoid, so that you do not accidentally bring the wrong nodes into your cluster.
It contains links to the recommended practices and procedures to use instead.




Bootstrap Error
---------------

In this example you have a three node cluster:

* node1
* node2
* node3

All three nodes are in the cluster. You now want to add node4.

If you do the following:

* Add and then start node4 and list only node4 as the seed node (contact point)
* Stop node4
* Start node4 and list only node1 as the seed node (contact point)

Then, node4 will be added to the cluster **without streaming any data**.

**Lesson Learned** - To prevent new nodes from not bootstrapping, always configure the seed node (contact point) as an existing node in the cluster.

**Procedure to Use** - :doc:`Add a New Node Into an Existing ScyllaDB Cluster </operating-scylla/procedures/cluster-management/add-node-to-cluster/>`

Node Removal Error
------------------

In this example you have a three node cluster:

* node1
* node2
* node3

Node3 is a dead node. If you do the following:

* Login to node1 and run a ``nodetool removenode`` operation and remove node3 from the cluster.
* Three days later, the ``REMOVED_TOKEN`` status is expired and the gossip is removed from the cluster.
* If at some point someone restarts node3 either because someone didn't know it is dead or because power returns after it was removed, node3 is added back to the cluster

**Lesson Learned** - Never reinstate a node that was removed.

**Procedure to Use** - :doc:`Remove a Node from a ScyllaDB Cluster (Down Scale) </operating-scylla/procedures/cluster-management/remove-node/>`

Decommission Error
------------------

In this example you have a three node cluster:

* node1
* node2
* node3

Node2 is down. You login node3. You run ``nodetool decommission`` to remove n3 from the cluster. If you choose to ignore node2:

* After three days have passed the ``LEFT`` status is expired and removed from gossip in the cluster.
* Node2 restarts and goes back into the cluster.
* Node2 will revive Node3 abd bring it back into the cluster because from Node2's point of view, Node3 should be part of the cluster.

**Lesson Learned** -  It is best to fix dead nodes before a ``nodetool decommission`` operation so that every node knows which nodes are decommissioned.
If there is no way to fix the dead node and decommission is performed without it, do not bring that dead node.

**Procedure to Use** - :doc:`Replace a Dead Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-dead-node/>`

Node Replacement Error
----------------------

In this example you have a three node cluster:

* node1
* node2
* node3

Node3 is dead. If you add node4 to replace node3 with the same IP address as node3 and node4 goes down, node3 will be restarted with the original IP address.

**Lesson Learned** - Never reinstate a node that was removed.

**Procedure to Use** - :doc:`Replace a Dead Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-dead-node/>`
