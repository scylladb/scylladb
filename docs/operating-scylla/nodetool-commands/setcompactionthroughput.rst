================================
Nodetool setcompactionthroughput
================================
**setcompactionthroughput** - Print the throughput cap for compaction in the system

Setting zero throughput disables capping

.. note::

   This command sets the deprecated, node-local ``compaction_throughput_mb_per_sec`` configuration option. It takes
   effect only when the ``compaction_throughput_mb_per_sec`` cluster configuration option is not set for this node at any
   scope. To set the limit for the cluster, a datacenter, a rack, or a node, use
   ``ALTER CLUSTER|DATACENTER|RACK|NODE ... WITH compaction_throughput_mb_per_sec = <value>`` instead. See
   :ref:`Configuring a Cluster with CQL <cluster-config>`.

Syntax
-------
.. code-block:: console

   nodetool [options] setcompactionthroughput <value_in_mib>

See also

* :doc:`getcompactionthroughput <getcompactionthroughput>`

.. include:: nodetool-index.rst

