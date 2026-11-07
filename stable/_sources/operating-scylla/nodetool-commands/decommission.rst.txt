nodetool decommission
=====================

**decommission** - Deactivate a selected node by streaming its data to the next node in the ring.

For example:

``nodetool decommission``

Use the ``nodetool netstats`` command to monitor the progress of the token reallocation.             

.. note::

    You cannot decomission a node if any existing node is down.

See :doc:`Remove a Node from a ScyllaDB Cluster (Down Scale) </operating-scylla/procedures/cluster-management/remove-node>`
for procedure details.

Before you run ``nodetool decommission``:

* Review current disk space utilization on existing nodes and make sure the amount 
  of data streamed from the node being removed can fit into the disk space available
  on the remaining nodes. If there is not enough disk space on the remaining nodes,
  the removal of a node will fail. Add more storage to remaining nodes **before**
  starting the removal procedure.
* Make sure that the number of nodes remaining in the DC after you decommission a node
  will be the same or higher than the Replication Factor configured for the keyspace
  in this DC. If the number of remaining nodes is lower than the RF, the decommission
  request may fail.
  In such a case, ALTER the keyspace to reduce the RF before running ``nodetool decommission``.


.. include:: nodetool-index.rst
