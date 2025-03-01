============================
Nodetool getstreamthroughput
============================
**getstreamthroughput** - Print the throughput cap for SSTables streaming in the system

If zero is printed, it means throughput is uncapped

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

