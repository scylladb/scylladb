Nodetool cluster restore
========================

**cluster restore** - Restore a table from a backup made with :doc:`nodetool cluster backup </operating-scylla/nodetool-commands/backup>`.

The destination table must exist and must use tablets. The replicas of each tablet download the SSTables that belong to
the tablet from the backup location of their datacenter, so no data is streamed between nodes.
See :ref:`Restore a cluster backup <restore-cluster-backup>` for the whole procedure.

The restore runs as a background task. Without ``--nowait``, the command waits for the task to finish and prints its final status.
With ``--nowait``, the command prints the task ID and returns; use the :doc:`nodetool tasks </operating-scylla/nodetool-commands/tasks/index>`
commands to monitor or abort the task. The status of the task can be checked for ``user_task_ttl`` seconds after it is done.
You can set the ttl using :doc:`nodetool tasks user-ttl </operating-scylla/nodetool-commands/tasks/user-ttl>`.

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)] cluster restore
               <keyspace> --table <table>
               (-s <snapshot> | --snapshot <snapshot>)
               (-l | --location) <dc>,<endpoint>,<bucket>[,<prefix>]
               [(-l | --location) <dc2>,<endpoint2>,<bucket2>[,<prefix2>]]...
               [--source-keyspace <keyspace>]
               [--source-table <table>]
               [--repair (full | disabled)]
               [--nowait]

Example
-------

Restore the table ``ks.table`` from the snapshot ``ss`` of a backup taken with the same ``--location``:

.. code-block:: console

    nodetool cluster restore ks --table table --snapshot ss --location mydc,s3.us-east-2.amazonaws.com,bucket-foo,foo/bar/baz

Restore the backup of ``ks.table`` into ``ks2.table2``:

.. code-block:: console

    nodetool cluster restore ks2 --table table2 --source-keyspace ks --source-table table --snapshot ss \
        --location mydc,s3.us-east-2.amazonaws.com,bucket-foo,foo/bar/baz

Restore without repairing the restored tablets:

.. code-block:: console

    nodetool cluster restore ks --table table --snapshot ss --location mydc,s3.us-east-2.amazonaws.com,bucket-foo,foo/bar/baz --repair disabled

Options
-------

* ``-h <host>`` or ``--host <host>`` - Node hostname or IP address.
* ``<keyspace>`` - Keyspace to restore into. Required.
* ``--table`` - Table to restore into. Required.
* ``-s`` or ``--snapshot`` - Name of the snapshot to restore. Required.
* ``-l`` or ``--location`` - Tuple of <dc>,<endpoint>,<bucket> and optional <prefix> to read the backup from, as given to ``nodetool cluster backup``.
  Required; repeat it for each DC the keyspace replicates to.
  Endpoints should be configured as per :ref:`the object storage configuration instructions <object-storage-configuration>`.
* ``--source-keyspace`` - Keyspace the snapshot was taken from, if its name differs from the keyspace to restore into.
* ``--source-table`` - Table the snapshot was taken from, if its name differs from the table to restore into.
* ``--repair`` - Whether to repair each tablet once it is restored: ``full``, the default, runs a full repair of the tablet,
  and ``disabled`` leaves it unrepaired. If a repair fails, the tablet stays restored and the restore task fails with an error naming it.
* ``--nowait`` - Don't wait on the restore process

See also

:doc:`Nodetool cluster backup </operating-scylla/nodetool-commands/backup/>`

:doc:`Nodetool restore </operating-scylla/nodetool-commands/restore/>`
