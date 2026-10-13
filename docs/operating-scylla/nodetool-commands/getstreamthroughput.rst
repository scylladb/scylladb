============================
Nodetool getstreamthroughput
============================
**getstreamthroughput** - Print the throughput cap for SSTables streaming in the system

If zero is printed, it means throughput is uncapped

The value printed is the limit in effect on the node: the ``stream_io_throughput_mb_per_sec`` cluster configuration option
when it is set for this node at any scope, and the node's ``stream_io_throughput_mb_per_sec`` configuration option otherwise.
See :ref:`Configuring a Cluster with CQL <cluster-config>`.

Syntax
-------
.. code-block:: console

   nodetool [options] getstreamthroughput [--mib]

Options
--------

* ``--mib`` - Print the value in MiB rather than megabits per second

See also

* :doc:`setstreamthroughput <setstreamthroughput>`

.. include:: nodetool-index.rst

