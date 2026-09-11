
Replace More Than One Dead Node In A ScyllaDB Cluster
******************************************************

.. note::

    Replacing a node requires at least a quorum of nodes in a cluster to be available. 
    If the quorum is lost, it must be restored before a node is replaced. 
    See :doc:`Handling Node Failures </troubleshooting/handling-node-failures>` for details. 

Prerequisites
-------------

* .. include:: /operating-scylla/procedures/cluster-management/_common/quorum-requirement.rst

* Verify the status of the cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command. A node with status ``DN`` is down and needs to be replaced.

.. code-block:: shell

   Datacenter: DC1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   DN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   DN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1

Login to one of the nodes in the cluster with (UN) status, collect the following info from the node:

* cluster_name - ``cat /etc/scylla/scylla.yaml | grep cluster_name``
* seeds - ``cat /etc/scylla/scylla.yaml | grep seeds:``
* endpoint_snitch - ``cat /etc/scylla/scylla.yaml | grep endpoint_snitch``
* Scylla version - ``scylla --version``

Procedure
---------

Depend on the Replication Factor (RF)

* If the number of failed nodes is smaller than your keyspaces RF, you still have at least one available replica with your data, and you can use :doc:`Replace a Dead Node </operating-scylla/procedures/cluster-management/replace-dead-node/>` procedure.
* If the number of failed nodes is equal or larger than your keyspaces RF, then some of the data is lost, and you need to retrieve it from backup. Use the :doc:`Replace a Dead Node </operating-scylla/procedures/cluster-management/replace-dead-node/>` procedure and :doc:`restore the data from backup </operating-scylla/procedures/backup-restore/restore/>`.
