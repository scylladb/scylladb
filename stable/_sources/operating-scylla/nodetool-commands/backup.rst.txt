===============
Nodetool backup
===============

**backup** - Copy SSTables from a specified keyspace's snapshot to a designated bucket in object storage

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
               --keyspace <keyspace> [--snapshot <snapshot>]
               --endpoint <endpoint> --bucket <bucket>
               [--nowait]

Options
-------

* ``-h <host>`` or ``--host <host>`` - Node hostname or IP address.
* ``--keyspace`` - Name of a keyspace to copy SSTables from
* ``--snapshot`` - Name of a snapshot to copy sstables from
* ``--endpoint`` - ID of the configured object storage endpoint to copy SSTables to
* ``--bucket`` - Name of the bucket to backup SSTables to
* ``--nowait`` - Don't wait on the backup process

.. include:: nodetool-index.rst
