Nodetool removenode
===================

.. warning::
    You must never use the ``nodetool removenode`` command to remove a running node that can be reached by other nodes in the cluster.
    Before using the command, make sure the node is permanently down and cannot be recovered.

    If the node is up and reachable by other nodes, use ``nodetool decommission``.
    See :doc:`Remove a Node from a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/remove-node>` for more information.


This command allows you to remove a node from the cluster when the status of the node is Down Normal (DN) and all attempts to restore the node have failed.

The node you are removing, as well as the :ref:`ignored dead nodes <removenode-ignore-dead-nodes>`, 
are permanently banned from the cluster at the beginning of the procedure. As a result, you 
won't be able to bring them back, even if the ``removenode`` procedure fails.
Once a node is banned, the only way forward is to remove or replace it - you won't be able to 
perform other topology operations, such as decommission or bootstrap, until the banned node 
is removed from the cluster or replaced.

Prerequisites
------------------------

* Using ``removenode`` requires at least a quorum of nodes in a cluster to be available. 
  If the quorum is lost, it must be restored before you change the cluster topology. 
  See :doc:`Handling Node Failures </troubleshooting/handling-node-failures>` for details.

* Make sure that the number of nodes remaining in the DC after you remove a node
  will be the same or higher than the Replication Factor configured for the keyspace
  in this DC. If the number of remaining nodes is lower than the RF, the removenode
  request may fail. In such a case, you should follow the procedure to
  :doc:`replace a dead node </operating-scylla/procedures/cluster-management/replace-dead-node>`
  instead of running ``nodetool removenode``.

Usage
--------

Provide the Host ID of the node to specify which node you want to remove.

.. code-block:: console

    nodetool removenode  <Host ID of the node to remove>

Example:

.. code-block:: console

    nodetool removenode 675ed9f4-6564-6dbd-can8-43fddce952gy


.. _removenode-ignore-dead-nodes:

Ignoring Dead Nodes
---------------------

All the nodes in the cluster participate in the ``removenode`` operation to sync data if needed. For this reason, the operation will fail if one or more nodes in the cluster are not available.
In such a case, to ensure that the operation succeeds, you must explicitly specify a list of unavailable nodes with the ``--ignore-dead-nodes`` option.

Use a comma-separated list to specify the Host IDs of all unavailable nodes in the cluster before specifying the node to remove.

Example:

.. code-block:: console

    nodetool removenode --ignore-dead-nodes 8d5ed9f4-7764-4dbd-bad8-43fddce94b7c,125ed9f4-7777-1dbn-mac8-43fddce9123e 675ed9f4-6564-6dbd-can8-43fddce952gy   

.. include:: nodetool-index.rst
