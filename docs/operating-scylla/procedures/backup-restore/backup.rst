================
Backup your Data
================

Even though ScyllaDB is a fault-tolerant system, it is recommended to regularly back up the data to external storage.

* Backup is a per-node procedure. Make sure to back up each node in your 
  cluster. For cluster-wide backup and restore, see `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/restore/>`_.
* Backup works the same for non-encrypted and encrypted SStables. You can use 
  `Encryption at Rest <https://enterprise.docs.scylladb.com/stable/operating-scylla/security/encryption-at-rest.html>`_ 
  available in ScyllaDB Enterprise without affecting the backup procedure.

You can choose one of the following:

* :ref:`Full Backup <backup-full-backup-snapshots>` - Copy the entire data of a node.

* :ref:`Incremental Backup <backup-incremental-backup>` - Updates (delta) - flushed SSTables.


.. _backup-full-backup-snapshots:

Full Backup - Snapshots
=======================

Snapshots are taken using :doc:`nodetool snapshot </operating-scylla/nodetool-commands/snapshot>`. First, the command flushes the MemTables from memory to SSTables on disk, and afterward, it creates a hard link for each SSTable in each keyspace.
With time, SSTables are compacted, but the hard link keeps a copy of each file. This takes up an increasing amount of disk space. It is important to clear space by :doc:`clean unnecessary snapshots </operating-scylla/procedures/backup-restore/delete-snapshot>`.

**Procedure**

| 1. Data can only be restored from a snapshot of the table schema, where data exists in a backup. Backup your schema with the following command:

| ``$: cqlsh -e "DESC SCHEMA" > <schema_name.cql>``

For example:

| ``$: cqlsh -e "DESC SCHEMA" > db_schema.cql``

|
| 2. Take a snapshot, including every keyspace you want to backup.

| ``$ nodetool snapshot <KEYSPACE_NAME>``

| For example:

| ``$ nodetool snapshot mykeyspace``

| The snapshot is created under Scylla data directory ``/var/lib/scylla/data``
| It will have the following structure:
| ``keyspace_name/table_name-UUID/snapshots/snapshot_name``

| For example:
| ``/mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/snapshots/1487847672222``

From one of the nodes, recreate the schema. Repeat these steps for each node in the cluster.

.. _backup-incremental-backup:

Incremental Backup
==================

| Enabling the incremental backup (disabled by default) will create a hard-link from each SSTable, right after it is **flushed**, to a backups directory.
| For a complete point in time backup, the following is required: 

  * A snapshot 
  * All incremental backups and commit logs from the time of the snapshot. 
  * Make sure to delete unnecessary incremental backups. Scylla does not do this automatically.

**Procedure**

| 1. In the ``/etc/scylla/scylla.yaml`` file set the ``incremental backups`` parameters to ``true`` and restart the Scylla service. Snapshot are created under Scylla data directory ``/var/lib/scylla/data``
| with the following structure:
| ``keyspace_name/table_name-UUID/backups/backups_name``

| For example:
| ``/mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/backups/1437827672721``


Additional Resources
====================

* :doc:`Scylla Snapshots </kb/snapshots>`


