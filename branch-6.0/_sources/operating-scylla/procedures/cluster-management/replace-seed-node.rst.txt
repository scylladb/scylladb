
===========================
Replacing a Dead Seed Node
===========================

.. note::
    The seed concept in gossip has been removed. Starting with Scylla Open Source 4.3 and Scylla Enterprise 2021.1, 
    a seed node is only used by a new node during startup to learn about the cluster topology. As a result, there's no need 
    to replace the node configured with the ``seeds`` parameter in the ``scylla.yaml`` file.

In Scylla, it is not possible to bootstrap a seed node. The following steps describe how to replace a dead seed node.

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

Your cluster should have more than one seed node, but it's not allowed to define all the nodes in the cluster to be seed nodes.
