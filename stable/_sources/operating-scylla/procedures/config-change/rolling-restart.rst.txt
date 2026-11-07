Rolling Restart Procedure
=========================

This is a general procedure that describes how to perform a rolling restart. You can use this procedure where a restart of each node is required (changing the ``scylla.yaml`` file, for example).

.. note::

   Perform this procedure on one node at the time. Move to the next node **only after** validating the current node is up and running.

---------
Procedure
---------

1. Run :doc:`nodetool drain </operating-scylla/nodetool-commands/drain/>` command (ScyllaDB stops listening to its connections from the client and other nodes).

2. Stop the ScyllaDB node.

.. include:: /rst_include/scylla-commands-stop-index.rst

3. Update the relevant configuration file, for example, scylla.yaml the file can be found under ``/etc/scylla/``.

4. Start the ScyllaDB node. 

.. include:: /rst_include/scylla-commands-start-index.rst

5. Verify the node is up and has returned to the ScyllaDB cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.

6. Repeat this procedure for all the relevant nodes in the cluster.
