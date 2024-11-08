===============
Nodetool backup
===============

**backup** - Copy SSTables from a specified keyspace's snapshot to a designated bucket in object storage

Note that status of backup can be checked for ``user_task_ttl`` seconds after the operation is done.
You can set the ttl using :doc:`nodetool tasks user-ttl </operating-scylla/nodetool-commands/tasks/user-ttl>`.
If ``--nowait`` flag is not set, the command relies on ``user_task_ttl`` internally.

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)] backup
               --keyspace <keyspace> --table <table>
               [--snapshot <snapshot>]
               --endpoint <endpoint> --bucket <bucket> --prefix <prefix>
               [--nowait]

Example
-------

.. code-block:: console

    nodetool backup --endpoint s3.us-east-2.amazonaws.com  --bucket bucket-foo --prefix foo/bar/baz --keyspace ks --table table --snapshot ss

Options
-------

* ``-h <host>`` or ``--host <host>`` - Node hostname or IP address.
* ``--keyspace`` - Name of a keyspace to copy SSTables from
* ``--table`` - Name of a table to copy SSTables from
* ``--snapshot`` - Name of a snapshot to copy sstables from
* ``--endpoint`` - Name of the configured object storage endpoint to copy SSTables to.
  This should be configured as per :ref:`the object storage configuration instructions <object-storage-configuration>`.
* ``--bucket`` - Name of the bucket to backup SSTables to
* ``--prefix`` - Prefix to backup SSTables to
* ``--nowait`` - Don't wait on the backup process

See also

:doc:`Nodetool restore </operating-scylla/nodetool-commands/restore/>`

.. include:: nodetool-index.rst
