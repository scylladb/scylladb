================
Backup your Data
================

Even though ScyllaDB is a fault-tolerant system, it is recommended to regularly back up the data to external storage.

* Backup is a per-node procedure. Make sure to back up each node in your 
  cluster. For cluster-wide backup and restore, see `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/restore/>`_.
* Backup works the same for non-encrypted and encrypted SStables. You can use 
  :doc:`Encryption at Rest </operating-scylla/security/encryption-at-rest>` 
  without affecting the backup procedure.

You can choose one of the following:

* :ref:`Full Backup <backup-full-backup-snapshots>` - Copy the entire data of a node.

* :ref:`Incremental Backup <backup-incremental-backup>` - Updates (delta) - flushed SSTables.


.. _backup-full-backup-snapshots:

Full Backup - Snapshots
=======================

Snapshots are taken using :doc:`nodetool snapshot </operating-scylla/nodetool-commands/snapshot>`. First, the command flushes the MemTables from memory to SSTables on disk, and afterward, it creates a hard link for each SSTable in each keyspace and table.
Over time, as SSTables are compacted, the original files remain hard-linked from the snapshot directories, so snapshots hold an increasing amount of disk space. It is important to clear space by :doc:`cleaning unnecessary snapshots </operating-scylla/procedures/backup-restore/delete-snapshot>`.

**Procedure**

| 1. Data can only be restored from a snapshot of the table schema, where data exists in a backup. Backup your schema, roles, permissions
|    and service levels with the following command:

| ``$: cqlsh -e "DESC SCHEMA WITH INTERNALS AND PASSWORDS" > <schema_name.cql>``

| For example:

| ``$: cqlsh -e "DESC SCHEMA WITH INTERNALS AND PASSWORDS" > db_schema.cql``

| The command can only be executed by a superuser.

.. warning::

  To get a proper schema description, you need to use cqlsh at least in version ``6.0.23``. Restoring a schema backup created by
  an older version of cqlsh may lead to data resurrection or data loss. To check the version of your cqlsh, you can use ``cqlsh --version``.

See :doc:`the relevant article to learn more </cql/describe-schema>`.

|
| 2. Take a snapshot, including every keyspace you want to backup.

| ``$ nodetool snapshot <KEYSPACE_NAME>``

| For example:

| ``$ nodetool snapshot mykeyspace``

| The snapshot is created under ScyllaDB data directory ``/var/lib/scylla/data``
| It will have the following structure:
| ``keyspace_name/table_name-UUID/snapshots/snapshot_name``

| For example:
| ``/mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/snapshots/1487847672222``

.. note::

   By default, nodetool names the snapshot after the current time, in milliseconds since the Unix epoch.
   To choose the name (a.k.a. tag), list the keyspaces to snapshot, or set a TTL after which the snapshot
   is deleted automatically, see :doc:`nodetool snapshot </operating-scylla/nodetool-commands/snapshot>`.
   ``nodetool cluster snapshot`` takes the snapshot on all nodes of the cluster with one command.

.. note::

   Each snapshot directory also contains a ``schema.cql`` file with the ``CREATE`` statement of its table,
   including the table's dropped columns. It describes that one table only, and does not include the keyspace,
   roles, permissions, or service levels, so it does not replace step 1.

Repeat these steps on each node in the cluster.

.. _backup-incremental-backup:

Incremental Backup
==================

| Enabling the incremental backup (disabled by default) will create a hard-link from each SSTable, right after it is **flushed**, to a backups directory.
| For a complete point in time backup, the following is required: 

  * A snapshot 
  * All incremental backups and commit logs from the time of the snapshot. 
  * Make sure to delete unnecessary incremental backups. ScyllaDB does not do this automatically.

**Procedure**

| 1. In the ``/etc/scylla/scylla.yaml`` file set the ``incremental_backups`` parameter to ``true`` and restart the ScyllaDB service. From now on, flushed SSTables are hard-linked under ScyllaDB data directory ``/var/lib/scylla/data``
| into a directory with the following structure:
| ``keyspace_name/table_name-UUID/backups``

| For example:
| ``/mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/backups``


Additional Resources
====================

* :doc:`ScyllaDB Snapshots </kb/snapshots>`


