
Safely Remove a Joining Node
============================

Sometimes when adding a node to the cluster, it gets stuck in a JOINING state (UJ) and never completes the process to an Up-Normal (UN) state. The only solution is to remove the node. As long as the node did not join the cluster, meaning it never went into UN state, you can stop this node, clean its data, and try again.

1. Run the :doc:`nodetool drain </operating-scylla/nodetool-commands/drain>` command (ScyllaDB stops listening to its connections from the client and other nodes).

2. Stop the node

.. include:: /rst_include/scylla-commands-stop-index.rst

3. Clean the data

.. include:: /rst_include/clean-data-code.rst

4. Start the node

.. include:: /rst_include/scylla-commands-start-index.rst
