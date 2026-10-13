================================
Nodetool getcompactionthroughput
================================
**getcompactionthroughput** - Print the throughput cap for compaction in the system

If zero is printed, it means throughput is uncapped

The value printed is the limit in effect on the node: the ``compaction_throughput_mb_per_sec`` cluster configuration option
when it is set for this node at any scope, and the node's ``compaction_throughput_mb_per_sec`` configuration option otherwise.
See :ref:`Configuring a Cluster with CQL <cluster-config>`.

Syntax
-------
.. code-block:: console

   nodetool [options] getcompactionthroughput

See also

* :doc:`setcompactionthroughput <setcompactionthroughput>`

.. include:: nodetool-index.rst

