=========================
Start Clusters Cleanly
=========================

In cases where you needed to shut down your cluster, use this procedure to bring it back up.

**Before you begin**

* Confirm that the cluster was shut down using the :doc:`shutdown procedure <safe-shutdown>`.
* (Only for versions prior to Scylla Open Source 4.3 and Scylla Enterprise 2021.1) Confirm that you know which nodes are the seed nodes. Seed nodes are specified in the ``scylla.yaml`` file.

**Procedure**

.. note::
   If your Scylla version is earlier than Scylla Open Source 4.3 or Scylla Enterprise 2021.1, start the seed
   nodes first. Validate that the seed nodes have all returned to normal by running :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
   If each seed node's status is listed as ``UN``, you can start the remaining nodes.

#. Start the nodes in parallel.

   .. include:: /rst_include/scylla-commands-start-index.rst

#. Validate that the nodes have all returned to normal. Run :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
   If each node's status is listed as ``UN``, then the start command has been executed successfully.
