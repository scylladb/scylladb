============================
Nodetool setstreamthroughput
============================
**setstreamthroughput** - Print the throughput cap for SSTables streaming in the system

Setting zero throughput disables capping

.. note::

   This command sets the deprecated, node-local ``stream_io_throughput_mb_per_sec`` configuration option. It takes
   effect only when the ``stream_io_throughput_mb_per_sec`` cluster configuration option is not set for this node at any
   scope. To set the limit for the cluster, a datacenter, a rack, or a node, use
   ``ALTER CLUSTER|DATACENTER|RACK|NODE ... WITH stream_io_throughput_mb_per_sec = <value>`` instead. See
   :ref:`Configuring a Cluster with CQL <cluster-config>`.

Syntax
-------
.. code-block:: console

   nodetool [options] setstreamthroughput <value_in_mb>

See also

* :doc:`getstreamthroughput <getstreamthroughput>`

.. include:: nodetool-index.rst

