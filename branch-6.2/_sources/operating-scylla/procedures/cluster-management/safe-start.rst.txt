=========================
Start Clusters Cleanly
=========================

In cases where you needed to shut down your cluster, use this procedure to bring it back up.

**Before you begin**

Confirm that the cluster was shut down using the :doc:`shutdown procedure <safe-shutdown>`.

**Procedure**

#. Start the nodes in parallel.

   .. include:: /rst_include/scylla-commands-start-index.rst

#. Validate that the nodes have all returned to normal. Run :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
   If each node's status is listed as ``UN``, then the start command has been executed successfully.
