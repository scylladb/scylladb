================
Nodetool restore
================

**restore** - Load SSTables from a designated bucket in object store into a specified keyspace or table

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
               --endpoint <endpoint> --bucket <bucket>
               --snapshot <snapshot>
               --keyspace <keyspace> [--table <table>]
               [--nowait]

Options
-------

* ``-h <host>`` or ``--host <host>`` - Node hostname or IP address.
* ``--endpoint`` - ID of the configured object storage endpoint to load SSTables from
* ``--bucket`` - Name of the bucket to load SSTables from
* ``--snapshot`` - Name of a snapshot to load SSTables from
* ``--keyspace`` - Name of a keyspace to load SSTables into
* ``--table`` - Name of a table to load SSTables into
* ``--nowait`` - Don't wait on the restore process

.. include:: nodetool-index.rst
