Nodetool rebuild
================

**rebuild** ``[[--force] <source-dc-name>]`` - This command rebuilds a node's data by streaming data from other nodes in the cluster (similarly to bootstrap).

When executing the command, ScyllaDB first figures out which ranges the local node (the one we want to rebuild) is responsible for.
Then which node in the cluster contains the same ranges.
If ``source-dc-name`` is provided, ScyllaDB will stream data only from nodes in that datacenter, when safe to do so.
Otherwise, an alternative datacenter that lost no nodes will be considered, and if none exist, all datacenters will be considered.
Use the ``--force`` option to enforce rebuild using the source datacenter, even if it is unsafe to do so.

When ``rebuild`` is enabled in :doc:`Repair Based Node Operations (RBNO) </operating-scylla/procedures/cluster-management/repair-based-node-operation>`,
data is rebuilt using repair-based-rebuild by reading all source replicas in each token range and repairing any discrepancies between them.
Otherwise, data is streamed from a single source replica when rebuilding each token range.
 
When :doc:`adding a new data-center into an existing ScyllaDB cluster </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/>` use the rebuild command.


.. note:: The ScyllaDB rebuild process continues to run in the background, even if the nodetool command is killed or interrupted.


For Example:

.. code-block:: shell

   nodetool rebuild <source-dc-name>

.. include:: nodetool-index.rst
