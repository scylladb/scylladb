Nodetool removenode
===================

This command allows you to remove a node from the cluster when the status of the node is Down Normal (DN) and all attempts to restore the node have failed.

Provide the Host ID of the node to specify which node you want to remove.

See :doc:`Remove a Node from a Scylla Cluster </operating-scylla/procedures/cluster-management/remove-node>` for more information.

.. warning::
    You must never use the ``nodetool removenode`` command to remove a running node that can be reached by other nodes in the cluster.
    Before using the command, make sure the node is permanently down and cannot be recovered.

    If the node is up and reachable by other nodes, use ``nodetool decommission``.
    See :doc:`Remove a Node from a Scylla Cluster </operating-scylla/procedures/cluster-management/remove-node>` for more information.


Usage:

.. code-block:: console

    nodetool removenode  <Host ID of the node to remove>

Example:

.. code-block:: console

    nodetool removenode 675ed9f4-6564-6dbd-can8-43fddce952gy

Note that all the nodes in the cluster participate in the ``removenode`` operation to sync data if needed. For this reason, the operation will fail if one or more nodes in the cluster are not available.
In such a case, to ensure that the operation succeeds, you must explicitly specify a list of unavailable nodes with the ``--ignore-dead-nodes`` option.

Use a comma-separated list to specify the Host IDs of all unavailable nodes in the cluster before specifying the node to remove.

.. warning::
    Before you use the ``nodetool removenode --ignore-dead-nodes`` command, you must make sure that both the node you want
    to remove AND the nodes you want to ignore are permanently down and cannot be recovered. Ignoring a node that is running
    or a node that is temporarily down and could be later restored in the cluster may result in data loss.

Example:

.. code-block:: console

    nodetool removenode --ignore-dead-nodes 192.168.1.4,192.168.1.5 675ed9f4-6564-6dbd-can8-43fddce952gy
    nodetool removenode --ignore-dead-nodes 8d5ed9f4-7764-4dbd-bad8-43fddce94b7c,125ed9f4-7777-1dbn-mac8-43fddce9123e 675ed9f4-6564-6dbd-can8-43fddce952gy


.. versionadded:: version 4.6 ``--ignore-dead-nodes`` option    

.. include:: nodetool-index.rst
