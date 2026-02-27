=========================
Shutdown Clusters Cleanly
=========================

In cases where you need to physically move hardware, or you have no other choice you will need to shut down your cluster in a safe manner.

**Before you begin**

Confirm no applications are running that are using the cluster as backend storage.

**Procedure**


On each node, in parallel:

#. Run the command :doc:`nodetool drain </operating-scylla/nodetool-commands/drain/>`.
#. Validate that the drain procedure has completed by running :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`. If the node's status is listed as ``DN``, then the drain command has been executed successfully.
#. Stop the node after drain completed successfully.
   
   .. include:: /rst_include/scylla-commands-stop-index.rst

#. To start the nodes again safely, proceed to the :doc:`Start Clusters Cleanly <safe-start>` procedure.
