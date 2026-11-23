Nodetool cluster backup
=======================

**cluster backup** - Back up a cluster-wide snapshot of specified keyspaces or specified table(s) to object storage.

Running ``cluster backup`` on a **single node** backs up the snapshot of all nodes in the cluster: each node uploads its own SSTables
to the location given for its datacenter. Cluster backup supports only tablet-based keyspaces, and stores a single copy of the repaired data.
See :ref:`Cluster-wide backup <backup-cluster-wide-backup>` for details.
To back up the snapshot of a single node, see :doc:`nodetool backup </operating-scylla/nodetool-commands/backup>`.

The object storage endpoints must be configured on the nodes, as described in :ref:`the object storage configuration instructions <object-storage-configuration>`.
In the current ScyllaDB version, tables stored on object storage cannot be backed up.

Cluster backup is incremental: SSTables that are already present in the backup location are not uploaded again,
for example SSTables uploaded by a previous backup, of the same snapshot or of another one.

A backup that failed part-way can therefore be run again, and continues where it stopped.
To complete the original backup, run the command again with ``--snapshot`` set to the snapshot name that nodetool printed when the backup started.
If ``--snapshot`` is omitted, the command is a new backup rather than a retry: it always takes a new snapshot first, named after the current time,
and uploads only the SSTables that are not present in the backup location yet.

Note that the status of the backup task can be checked for ``user_task_ttl`` seconds after the operation is done.
You can set the ttl using :doc:`nodetool tasks user-ttl </operating-scylla/nodetool-commands/tasks/user-ttl>`.
If ``--nowait`` flag is not set, the command relies on ``user_task_ttl`` internally.

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)] cluster backup
               [--keyspaces <keyspace>]...
               [--table <table>[,<table>...]]
               [(-s <snapshot> | --snapshot <snapshot>)]
               (-l | --location) <dc>,<endpoint>,<bucket>[,<prefix>]
               [(-l | --location) <dc2>,<endpoint2>,<bucket2>[,<prefix2>]]...
               [--nowait]
               [--move-files]

ScyllaDB nodetool cluster backup command supports the following options:

- ``-l`` ``--location`` sets the location to back up the data of a datacenter to, as a tuple of ``<dc>,<endpoint>,<bucket>`` and an optional ``<prefix>``.
  This option is required. Repeat it for each datacenter.

  For example:

  ::

     nodetool cluster backup --location dc1,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --location dc2,s3.eu-west-1.amazonaws.com,bucket-bar,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace

- ``-s`` ``--snapshot`` sets the name of the snapshot to back up. If the snapshot does not exist, it is taken first, on all nodes in the cluster.
  By default, a new snapshot is taken, named after the current time, in milliseconds since the Unix epoch.

  .. caution:: If the snapshot already exists, it is not extended to the selected tables. Select only the keyspaces and tables
     that the snapshot includes: a selected table that is not in the snapshot is backed up with a manifest that lists no SSTables,
     and the backup still reports success.

  .. warning:: Do not back up a snapshot again to a location it was already backed up to successfully.
     The snapshot is recorded as backed up to that location as a whole, not per table, so no further tables are uploaded,
     and the command may fail. To back up other tables, or to back up the same tables again, take a new snapshot.

  For example:

  ::

     nodetool cluster backup --location dc1,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace

- ``--keyspaces`` backs up a specific keyspace. Repeat it for each keyspace. The keyspaces must use tablets. If no keyspace is specified,
  the command includes all keyspaces, including the system keyspaces, which use local replication, so the command fails if it has to take the snapshot.
  With an existing snapshot, it processes all tables, and writes an empty manifest for each table that the snapshot doesn't include. Always specify the keyspaces.

  For example:

  ::

     nodetool cluster backup --location dc1,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace

- ``--table`` backs up a specific table, or a comma-separated list of tables. You must specify one and only one keyspace when using this option.
  The default is all tables in the keyspace, including materialized views and the tables that back secondary indexes.

  For example:

  ::

     nodetool cluster backup --location dc1,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace --table team_roster

- ``--move-files`` moves the SSTable files instead of copying them: each node deletes the SSTable files it uploaded from its local disk.

- ``--nowait`` returns the ID of the backup task immediately, without waiting for the backup to finish. Use the :doc:`nodetool tasks </operating-scylla/nodetool-commands/tasks/index>`
  commands to track the progress of the backup.

To restore the backup, see :ref:`Restore a cluster backup <restore-cluster-backup>`.

See also :doc:`nodetool cluster snapshot </operating-scylla/nodetool-commands/cluster/snapshot>`.
