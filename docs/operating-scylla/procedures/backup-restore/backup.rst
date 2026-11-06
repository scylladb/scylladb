================
Backup your Data
================

Even though ScyllaDB is a fault-tolerant system, it is recommended to regularly back up the data to external storage.

* Snapshot-based backup is a per-node procedure. Make sure to back up each node in your
  cluster. To back up the whole cluster from one node, use the cluster-wide
  :ref:`Native Backup <backup-native-backup>`, or `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/restore/>`_,
  which orchestrates backup and restore across the cluster.
* Backup works the same for non-encrypted and encrypted SStables. You can use 
  :doc:`Encryption at Rest </operating-scylla/security/encryption-at-rest>` 
  without affecting the backup procedure.

You can choose one of the following:

* :ref:`Full Backup <backup-full-backup-snapshots>` - Copy the entire data of a node.

* :ref:`Incremental Backup <backup-incremental-backup>` - Updates (delta) - flushed SSTables.

* :ref:`Native Backup <backup-native-backup>` - ScyllaDB uploads the SSTables of a snapshot to S3-compatible object storage, cluster-wide or per node.


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


.. _backup-native-backup:

Native Backup
=============

With native backup, ScyllaDB itself uploads the SSTables of a snapshot to S3-compatible object storage (AWS S3 or Google Cloud Storage).
The upload runs inside the node, so its disk, CPU and network usage is scheduled and throttled like the rest of the node's work,
and no external tool needs access to the data directory. See :doc:`Backup and Restore Overview </features/backup-and-restore>`
for how native backup compares with uploading through the ScyllaDB Manager agent, and for the options that limit its I/O throughput.

The object storage endpoint must be configured on the nodes first, as described in :ref:`Configuring Object Storage <object-storage-configuration>`.

Native backup uploads the SSTables of the snapshot, not the schema. Back up the schema separately with
``DESC SCHEMA WITH INTERNALS AND PASSWORDS``, as in the :ref:`Full Backup <backup-full-backup-snapshots>` procedure,
so that it can be recreated before the data is restored.

Cluster-wide backup
-------------------

The whole cluster can be snapshotted and backed up from one node, with
:doc:`nodetool cluster snapshot </operating-scylla/nodetool-commands/snapshot>` and :doc:`nodetool cluster backup </operating-scylla/nodetool-commands/backup>`.
The cluster snapshot is taken on all nodes and recorded in the ``system_distributed`` keyspace, and the backup then has each node upload its own SSTables
to the location given for its datacenter.

Cluster backup is designed for tablet-based keyspaces, and stores a **single copy** of the repaired data. For each tablet, SSTables whose data is covered
by the tablet's last repair are uploaded from one replica only, since the other replicas hold the same data. SSTables holding data that is not covered
by that repair, because they were never repaired or were last repaired before it, are uploaded from every replica. The backup manifest records the
replicas that were skipped, so that a restore repopulates all of them.
To get the most out of this, run :doc:`nodetool cluster repair </operating-scylla/nodetool-commands/cluster/repair>` before taking the snapshot.

The backup is laid out as follows under the prefix of each location: the SSTables go under ``sstables/``, and the manifest of each table goes to
``snapshots/<keyspace_name>/<table_name>/<snapshot_name>/manifest.json``.

**Procedure**

On any node, run:

.. code-block:: shell

   nodetool cluster snapshot --tag mysnapshot mykeyspace
   nodetool cluster backup --location mydc,s3.us-east-2.amazonaws.com,bucket-foo,backups/mysnapshot --snapshot mysnapshot --keyspaces mykeyspace

Repeat the ``--location`` option for each datacenter the keyspace replicates to. If the snapshot does not exist yet, ``nodetool cluster backup`` takes it,
so the first command is optional. A backup that failed part-way can be run again with the same parameters; SSTables that were already uploaded are skipped.
With ``--move-files``, each node deletes the SSTable files it uploaded from its local disk. Unlike the per-node backup, the other snapshot files,
such as ``manifest.json`` and ``schema.cql``, stay in the snapshot directory until the snapshot is cleared.

To restore, see :ref:`Restore a cluster backup <restore-cluster-backup>`.

Per-node backup
---------------

:doc:`nodetool backup </operating-scylla/nodetool-commands/backup>` copies the SSTables of one table, from a snapshot taken on the node it runs on,
to a bucket and prefix of your choice. Run it on each node, with a prefix that identifies the node and the snapshot, so that each node's backup
can be told apart and restored on its own. The snapshot's ``manifest.json``, listing the uploaded SSTables, is uploaded under the same prefix.

**Procedure**

On each node, take a snapshot of the keyspace, and back up each of its tables:

.. code-block:: shell

   nodetool snapshot --tag mysnapshot mykeyspace
   nodetool backup --endpoint s3.us-east-2.amazonaws.com --bucket bucket-foo --prefix node1/mysnapshot/team_roster \
     --keyspace mykeyspace --table team_roster --snapshot mysnapshot

With the ``--move-files`` option, the snapshot files are deleted from the local disk once they are uploaded, so the snapshot no longer holds disk space.

To restore, see :ref:`Restore from object storage <restore-object-storage>`.

Additional Resources
====================

* :doc:`ScyllaDB Snapshots </kb/snapshots>`


