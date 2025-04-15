Node Joins With No Data
========================
A new node under normal operation joins a ring in the ``UJ`` (Up Joining) state.
Gossip between the nodes report the new node's addition and the nodes wait for the new node to change to the ``UN`` (Up Normal) state.
Data is re-distributed to include the new node and when it has its share of the data, the new node's status will change to ``UN`` (Up Normal) state.
Should this fail to happen, the node may join the cluster directly into the ``UN`` state without receiving any data.
Use the procedure that follows to correct situations where a node joins a cluster directly in the UN state.

.. note:: This procedure only covers the situation in which a new node joins the cluster. Nodes replacing existing nodes following the :doc:`Replace A Dead Node </operating-scylla/procedures/cluster-management/replace-dead-node/>` procedure are expected to be in ``UN`` state from the start.

The Problem
^^^^^^^^^^^

A new node may not acquire data from its peers if the ``seeds`` parameter in the new node's ``scylla.yaml`` file 
does not specify the IP of an existing node in the cluster (for example, it specifies its own IP).

The scenario constitutes a user error and is the incorrect way of adding new nodes to the cluster. See :doc:`Adding New Node </operating-scylla/procedures/cluster-management/add-node-to-cluster/>` for more details. This article explains how to restore a cluster safely in such a case.

.. warning:: You mustn't run nodetool cleanup in any other nodes in the cluster until this is fixed, as it will delete data that is not yet available from the new node.

Solution
^^^^^^^^

Steps to fix:

#. For every node that joined the cluster directly in UN state, run nodetool decommission serially. Do not decommission more than one node at the same time.
#. Make sure to clean the data from the decommissioned node

   .. include:: /rst_include/clean-data-code.rst

   Actual directories might be different than above, check your ``scylla.yaml`` to validate.   
   
#. Once all the new nodes are decommissioned, add them again with the :doc:`correct procedure </operating-scylla/procedures/cluster-management/add-node-to-cluster/>`.


Other important considerations:

* Avoid running ``nodetool removenode`` for the new nodes. Running ``nodetool removenode`` instead of ``nodetool decommission`` will cause the new writes sent to the new nodes to be lost. In all cases, you should always prefer to decommission over removenode. Use removenode **only** in cases where there is no other way to restart the node.
* While running a repair in the cluster will bring some data back to the new node and restore its data, it will take much longer than the procedure suggested here. It is also important to keep in mind that one copy is lost per node inserted directly into the UN state. In the extreme case in which N nodes were added where N is your replication factor, some data may have been permanently lost and repairs will not be able to restore it. Therefore, repair is not recommended to fix this issue, except when the user is aware and their consistency model can tolerate missing data points.
  

