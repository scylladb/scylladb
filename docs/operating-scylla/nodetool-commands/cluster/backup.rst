Nodetool cluster backup
=======================

**cluster backup** - Back up a cluster-wide snapshot of specified keyspaces or specified table(s) to object storage.

Running ``cluster backup`` on a **single node** backs up the snapshot of all nodes in the cluster: each node uploads its own SSTables
to the location given for its datacenter. Cluster backup supports only tablet-based keyspaces, and stores a single copy of the repaired data.
See :ref:`Cluster-wide backup <backup-cluster-wide-backup>` for details.
To back up the snapshot of a single node, see :doc:`nodetool backup </operating-scylla/nodetool-commands/backup>`.

The object storage endpoints must be configured on the nodes, as described in :ref:`the object storage configuration instructions <object-storage-configuration>`.
Tables stored on object storage cannot be backed up.

A backup that failed part-way can be run again with the same parameters, including the same ``--snapshot`` name; SSTables that were already uploaded are skipped.
If ``--snapshot`` was omitted, a rerun takes a new snapshot, named after the current time, and uploads all SSTables again.

Note that status of backup can be checked for ``user_task_ttl`` seconds after the operation is done.
You can set the ttl using :doc:`nodetool tasks user-ttl </operating-scylla/nodetool-commands/tasks/user-ttl>`.
If ``--nowait`` flag is not set, the command relies on ``user_task_ttl`` internally.

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

     Back up all the tables of a snapshot with a single command. Once a backup of a snapshot to a location completes, the snapshot
     is recorded as backed up to that location as a whole, not per table. To back up other tables, take a new snapshot.

  For example:

  ::

     nodetool cluster backup --location dc1,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace

- ``--keyspaces`` backs up a specific keyspace. Repeat it for each keyspace. The keyspaces must use tablets. If no keyspace is specified,
  the command includes all keyspaces, including the system keyspaces, which use local replication, so taking the snapshot fails. Always specify the keyspaces.

  For example:

  ::

     nodetool cluster backup --location dc1,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace

- ``--table`` backs up a specific table, or a comma-separated list of tables. You must specify one and only one keyspace when using this option. The default is all tables.

  For example:

  ::

     nodetool cluster backup --location dc1,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace --table team_roster

- ``--move-files`` moves the SSTable files instead of copying them: each node deletes the SSTable files it uploaded from its local disk.

- ``--nowait`` returns the ID of the backup task immediately, without waiting for the backup to finish. Use the :doc:`nodetool tasks </operating-scylla/nodetool-commands/tasks/index>`
  commands to track the progress of the backup.

To restore the backup, see :ref:`Restore a cluster backup <restore-cluster-backup>`.

See also :doc:`nodetool cluster snapshot </operating-scylla/nodetool-commands/cluster/snapshot>`.
