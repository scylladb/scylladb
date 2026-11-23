Nodetool cluster snapshot
=========================

**cluster snapshot** - Take a cluster-wide snapshot of specified keyspaces or a snapshot of the specified table(s).

Running ``cluster snapshot`` on a **single node** takes the snapshot on all nodes in the cluster. The snapshot is recorded in the
``system_distributed`` keyspace, so that :doc:`nodetool cluster backup </operating-scylla/nodetool-commands/cluster/backup>` can
upload it to object storage.
To take a snapshot on a single node only, see :doc:`nodetool snapshot </operating-scylla/nodetool-commands/snapshot>`.

The keyspaces must use tablets. The command fails if any of the keyspaces uses vnodes or local replication,
or if a snapshot with the same name already exists on the node that runs the command, in any of the selected tables.

Syntax
------

.. code-block:: console

   nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)] cluster snapshot
               [(-t <tag> | --tag <tag>)]
               [--table <table>[,<table>...]]
               [<keyspace>...]

ScyllaDB nodetool cluster snapshot command supports the following options:

- ``-t`` ``--tag`` sets the name of the snapshot. By default, the snapshot is named after the current time, in milliseconds since the Unix epoch.

  For example:

  ::

     nodetool cluster snapshot --tag mysnapshot mykeyspace

- ``--table`` takes the snapshot of a specific table, or of a comma-separated list of tables. You must specify one and only one keyspace when using this option.
  The default is all tables in the keyspace, including materialized views and the tables that back secondary indexes.

  For example:

  ::

     nodetool cluster snapshot --table team_roster,team_players mykeyspace

- ``keyspaces`` takes the snapshot of a specific keyspace, or of a list of space-delimited keyspace names. If no keyspace is specified,
  the command includes all keyspaces, including the system keyspaces, which use local replication, so it fails. Always specify the keyspaces.

  For example:

  ::

     nodetool cluster snapshot --tag mysnapshot mykeyspace yourkeyspace

See also :ref:`Cluster-wide backup <backup-cluster-wide-backup>`.
