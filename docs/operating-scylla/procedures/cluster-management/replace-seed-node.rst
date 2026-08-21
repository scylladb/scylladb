
===========================
Replacing a Dead Seed Node
===========================

In ScyllaDB, it is not possible to bootstrap a seed node. The following steps describe how to replace a dead seed node.

.. note::
   A seed node is only used by a new node during startup to learn about
   the cluster topology.
   Once the nodes have joined the cluster, the seed node has no function.
   In typical scenarios, there's no need to replace the node
   configured with the ``seeds`` parameter in the ``scylla.yaml`` file.

* The first node in a new cluster must be a seed node.
* It is sufficient to configure one seed node in a node's ``scylla.yaml`` file.
  You may choose to configure two or three seed nodes if your cluster is large.
* It’s not recommended that all the nodes in the cluster be defined as seed nodes.
* If you update the IP address of a seed node or remove it from the cluster,
  you should update configuration files on all the remaining nodes to keep the
  configuration consistent.
  Once a node has joined the cluster and has all the peer information saved
  locally in the ``system.peers`` system table, seed nodes are no longer used,
  but they are still contacted on each restart. To avoid configuration errors
  and to be able to reach out to the cluster if the seed IP address changes,
  the seed configuration should be valid.

Prerequisites
-------------

Verify that the node is listed as a seed node in the ``scylla.yaml`` file by running the following command:

``cat /etc/scylla/scylla.yaml | grep seeds:``

If the dead node's IP address is in the seeds list, it needs to be replaced.

Procedure
---------

#. Perform steps 1-3 for all the nodes in the cluster:

     #. Promote an existing node from the cluster to be a seed node by adding the node IP to the seed list in the ``scylla.yaml`` file in ``/etc/scylla/``.
     #. Remove the dead node IP from the seeds providers list. See :doc:`Remove a Seed Node from Seed List </operating-scylla/procedures/cluster-management/remove-seed/>` for instructions.
     #. Restart the node in the cluster by running the following command: 

     .. include:: /rst_include/scylla-commands-restart-index.rst

     .. Note:: 
        This operation needs to be performed on all the nodes in the cluster. For this reason, you need to orchestrate the procedure so that only a small number of nodes are restarted simultaneously.
        Use ``nodetool status`` to verify that restarted nodes are online before restarting more nodes. If too many nodes are offline, the cluster may suffer temporary service degradation or outage. 
#. Replace the dead node using the :doc:`dead node replacement procedure </operating-scylla/procedures/cluster-management/replace-dead-node/>`.


