=================
Upscale a Cluster
=================

Upcaling your cluster involves moving the cluster to a larger instance. With this procedure, it can be done without downtime.

ScyllaDB was designed with big servers and multi-cores in mind. In most cases, it is better to run a smaller cluster on a bigger machine instance than a larger cluster on a small machine instance.
However, there may be cases where you started with a small cluster, and you now you want to upscale.

There are a few alternatives to do this:

* `Add Bigger Nodes to a Cluster`_ and removing the old smaller nodes. This is useful when you can not upscale (add more CPU) for each node, for example using I3 instances on EC2.
* `Upscale Nodes by Adding CPUs`_

.. _add-bigger-nodes-to-a-cluster:

Add Bigger Nodes to a Cluster
-----------------------------

This procedure can be used to either upscale an entire cluster or to upscale a single node.

#. Add :doc:`new bigger nodes </operating-scylla/procedures/cluster-management/add-node-to-cluster/>`  to the cluster. Confirm Streaming has completed before continuing.
#. :doc:`Remove the old smaller nodes </operating-scylla/procedures/cluster-management/remove-node/>`. Confirm Streaming has completed before continuing.
#. Repeat steps 1 and 2 until the entire cluster is using bigger nodes.

.. note:: The cluster is only as strong as its weakest node. Do not overload the cluster before all nodes are as upscaled.

Upscale Nodes by Adding CPUs
----------------------------

.. note::

   Upscaling a cluster by adding CPUs requires at least a quorum of nodes in a cluster to be available. 
   If the quorum is lost, it must be restored before a node is upscaled. 
   See :doc:`Handling Node Failures </troubleshooting/handling-node-failures>` for details. 

This procedure is only useful for entire clusters, not individual nodes.
Do the following on each node in the cluster, making sure the nodes are restarted **sequentially** 
to avoid interrupting the availability of your application:

#. Run :doc:`nodetool drain </operating-scylla/nodetool-commands/drain/>` to stop traffic to the node.
#. Stop the service.

   .. include:: /rst_include/scylla-commands-stop-index.rst

#. Add cores
#. Run ``scylla_setup`` to set ScyllaDB to the new HW configuration.
#. Start the service

   .. include:: /rst_include/scylla-commands-start-index.rst


.. note:: Updating the number of cores will cause ScyllaDB to reshard the SSTables to match the new core number. This is done by compacting all of the data on disk at startup.
