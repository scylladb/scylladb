
Replace a Running Node in a ScyllaDB Cluster 
*********************************************

There are two methods to replace a running node in a ScyllaDB cluster.

#. `Add a new node to the cluster and then decommission the old node`_
#. `Replace a running node - by taking its place in the cluster`_

.. note::

    .. include:: /operating-scylla/procedures/cluster-management/_common/quorum-requirement.rst


Add a new node to the cluster and then decommission the old node
=================================================================

Adding a new node and only then decommissioning the old node allows the cluster to keep the same level of data replication throughout the process, but at the cost of more data being transferred during the procedure.
When adding a new node to a ScyllaDB cluster, existing nodes will give the new node responsibility for a subset of their vNodes, making sure that data is once again equally distributed. In the process, these nodes will stream relevant data to the new node.
When decommissioning a node from a ScyllaDB cluster, it will give its vNodes to other nodes, making sure data is once again equally distributed. In the process, this node will stream its data to the other nodes.
Hence, replacing a node by adding and decommissioning redistribute the vNodes twice, streaming a node worth of data each time.


Procedure
^^^^^^^^^

1. Follow the procedure: :doc:`Adding a New Node Into an Existing ScyllaDB Cluster </operating-scylla/procedures/cluster-management/add-node-to-cluster/>`.

2. Decommission the old node using the :doc:`Remove a Node from a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/remove-node>` procedure

3. Run the :doc:`nodetool cleanup </operating-scylla/nodetool-commands/cleanup/>` command on all the remaining nodes in the cluster

4. Verify that the node removed using :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command


Replace a running node - by taking its place in the cluster 
===========================================================

Stopping a node and taking its place in the cluster is not as safe as the data replication factor is temporarily reduced during the process. However, it is more efficient, as vNode distribution does not change, and only one node worth of data is streamed

Procedure
^^^^^^^^^
1. Run :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>` command (ScyllaDB stops listening to its connections from the client and other nodes).

2. Stop the ScyllaDB node you want to replace 
   
.. include:: /rst_include/scylla-commands-stop-index.rst

3. Follow the :doc:`Replace a Dead Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-dead-node/>` procedure

4. Verify that the node is successfully replaced using :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command

