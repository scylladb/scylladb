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

Snapshots are taken using :doc:`nodetool snapshot </operating-scylla/nodetool-commands/snapshot>`.
First, the command flushes the MemTables from memory to SSTables on disk, and afterward, it creates a hard link for each SSTable in each keyspace and table.

Note that over time, as SSTables are compacted, the original SSTable files may still be hard linked to one or more snapshots subdirectories.
Therefore, an increasing amount of disk space may be held up by snapshots,
hence it is important to clear space using the :doc:`clean unnecessary snapshots </operating-scylla/procedures/backup-restore/delete-snapshot>` procedure.

**Procedure**

On each node, run:

| ``$ nodetool snapshot``
|
| The snapshot is created under the Scylla data directory ``/var/lib/scylla/data``
| It will have the following structure for each table:
|
| ``keyspace_name/table_name-UUID/snapshots/snapshot_name``
|
| For example:
|
| ``mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/snapshots/1487847672222``

.. note::

   By default, nodetool automatically names the snapshot using the current time since the system EPOCH, in milliseconds.
   For advanced options, like setting the snapshot name (a.k.a. tag) or taking a snapshot of a list of keyspaces,
   see :doc:`nodetool snapshot </operating-scylla/nodetool-commands/snapshot>`.

.. note::

   To restore data from a given snapshot, the corresponding schema must be used.

   In the common case, when no columns were deleted or altered,
   Scylla automatically describes the schema for each table in the snapshot,
   storing the result in a file named ``schema.cql`` in each snapshot subdirectory.

   The full schema can also be backed up using the ``DESCRIBE SCHEMA`` cql query.
   For example:

     ``$ cqlsh -e "DESCRIBE SCHEMA" > db_schema.cql``

   If the backed up data in a snapshot contain deleted or altered columns,
   the schema should be restored from the ``system_schema`` keyspace,
   in the same full-backup snapshot.

   See the :doc:`Restore from Backup </operating-scylla/procedures/backup-restore/restore>` page for more information.

.. _backup-incremental-backup:

Incremental Backup
==================

| Enabling the incremental backup (disabled by default) will create a hard-link from each SSTable, right after it is **flushed**, to a ``backups/`` directory.
| For a complete point in time backup, the following is required:

|  * A snapshot
|  * All incremental backups and commit logs from the time of the snapshot.

.. note::

   | Make sure to delete unnecessary incremental backups files after copying them to external storage.
   | Scylla does not do this automatically!

**Procedure**

| 1. In the ``/etc/scylla/scylla.yaml`` file set the ``incremental backups`` parameters to ``true`` and restart the Scylla service. Snapshot are created under Scylla data directory ``/var/lib/scylla/data``
| with the following structure:
| ``keyspace_name/table_name-UUID/backups/backups_name``

| For example:
| ``/mykeyspace/team_roster-91cd2060f99d11e6a47a000000000000/backups/1437827672721``


Additional Resources
====================

* :doc:`Scylla Snapshots </kb/snapshots>`


