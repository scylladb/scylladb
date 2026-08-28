Restore from a Backup and Incremental Backup
********************************************

Restoring a keyspace from a backup requires all snapshot files of the tables, and (if available) incremental backup files taken after the snapshot. Before restoring from backup, the table data must be truncated, making sure that the existing data does not overwrite the restored data.

.. include:: _common/manager.rst

-------------------------
Choosing a restore method
-------------------------

ScyllaDB supports several restore methods. Choose the one that matches where your backup files are and whether the cluster topology changed since the backup was taken:

* :ref:`Restore from object storage <restore-object-storage>` - restore SSTables backed up to S3-compatible object storage with :doc:`nodetool backup </operating-scylla/nodetool-commands/backup>`. Runs on a running cluster and works regardless of cluster topology changes since the backup.

* :ref:`Restore with load and stream <restore-load-and-stream>` - upload backed-up SSTable files to a running cluster; the data is streamed to the nodes owning it. Works regardless of cluster topology changes since the backup.

* :ref:`Restore to an identical cluster <restore-procedure>` - copy snapshot files back in place and restart the nodes. Requires a cluster with the same number of nodes and the same token distribution as at the time of the backup, and each node must be restored from the backup of the **same node**. Suitable for vnode-based keyspaces only.

For cluster-wide backup and restore, use `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/restore/>`_, which orchestrates the process across the cluster.

.. _restore-object-storage:

---------------------------
Restore from object storage
---------------------------

Use this method to restore SSTables backed up to S3-compatible object storage with :doc:`nodetool backup </operating-scylla/nodetool-commands/backup>`. The node downloads the SSTables from the bucket and streams their contents to the nodes owning the data (load and stream), so the restore works regardless of cluster topology changes since the backup, and the cluster stays online.

The object storage endpoint must be configured on the nodes, as described in :ref:`Configuring Object Storage <object-storage-configuration>`.

**Procedure**

#. Recreate the schema (if needed) and truncate the target tables, as described in the beginning of :ref:`Restore to an identical cluster <restore-procedure>`.

#. List the backed-up SSTables in the bucket under the prefix used during the backup. The restore command takes the paths of the ``TOC.txt`` components of the SSTables to restore, **relative to the prefix** -- the remainder of each object key after the prefix. Note that listing tools print full object keys, from the bucket root, so the prefix needs to be stripped. For example:

   .. code-block:: shell

      aws s3 ls --recursive s3://bucket-foo/ks/cf/24601/ | awk '/-TOC.txt$/ { print $4 }' | sed 's|^ks/cf/24601/||'

#. Run :doc:`nodetool restore </operating-scylla/nodetool-commands/restore>`, passing the endpoint, bucket, prefix, target keyspace and table, and the list of prefix-relative TOC paths:

   .. code-block:: shell

      nodetool restore --endpoint s3.us-east-2.amazonaws.com --bucket bucket-foo --prefix ks/cf/24601 \
        --keyspace ks --table cf \
        me-3gdq_0bki_2dy4w2gqj6hoso4mw1-big-TOC.txt \
        me-3gdq_0bki_2dipc1ysb2x2a3btgh-big-TOC.txt

   Alternatively, put the same prefix-relative TOC paths (newline-separated) in a file and pass it with the ``--sstables-file-list`` option:

   .. code-block:: shell

      cat > sstables.list <<EOF
      me-3gdq_0bki_2dy4w2gqj6hoso4mw1-big-TOC.txt
      me-3gdq_0bki_2dipc1ysb2x2a3btgh-big-TOC.txt
      EOF

      nodetool restore --endpoint s3.us-east-2.amazonaws.com --bucket bucket-foo --prefix ks/cf/24601 \
        --keyspace ks --table cf --sstables-file-list sstables.list

#. Monitor the restore. By default, the command waits for the restore to finish and reports its final status. With the ``--nowait`` option, it returns a task ID immediately; use the :doc:`nodetool tasks </operating-scylla/nodetool-commands/tasks/index>` commands to track progress or cancel the operation.

**Speeding up the restore**

A single ``nodetool restore`` invocation runs on one node, which downloads and streams all the listed SSTables. To parallelize the work, split the list of SSTables between the nodes and run ``nodetool restore`` on each of them. The ``--scope`` option (``node``, ``rack``, ``dc``, or ``all``) constrains where each node streams the data, so that concurrent restores don't stream the same partition to a replica more than once. See :doc:`nodetool restore </operating-scylla/nodetool-commands/restore>` for details on combining ``--scope`` with per-node SSTable lists.

With the ``--primary-replica-only`` option, each partition is streamed only to its primary replica. This reduces the amount of streamed data, but you **must** run a full cluster repair after the restore completes to replicate the data to the remaining replicas: for vnode-based keyspaces, run :doc:`nodetool repair -pr </operating-scylla/nodetool-commands/repair>` on **every** node; for tablet-based keyspaces, run :doc:`nodetool cluster repair </operating-scylla/nodetool-commands/cluster/repair>` on any single node.

.. _restore-load-and-stream:

----------------------------
Restore with load and stream
----------------------------

Use this method when the backed-up SSTable files are available on disk (for example, snapshot files copied back from external storage). The SSTables are read and their contents are streamed to the nodes owning the data, so the method works regardless of cluster topology changes since the backup. Each SSTable needs to be uploaded to only **one** node, any node, and the cluster stays online.

**Procedure**

#. Recreate the schema (if needed) and truncate the target tables, as described in the beginning of :ref:`Restore to an identical cluster <restore-procedure>`.

#. Copy the backed-up SSTable files of a table to that table's ``upload`` directory on one of the nodes, and make sure the files are owned by the ``scylla`` user and group:

   .. code-block:: shell

      sudo cp /path/to/backup/sstables/* /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/upload/
      sudo chown -R scylla:scylla /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/upload/

   You can distribute the backup files between several nodes to parallelize the restore; make sure each SSTable is uploaded to only one node.

#. Run :doc:`nodetool refresh </operating-scylla/nodetool-commands/refresh>` with the ``--load-and-stream`` option on each node holding uploaded files:

   .. code-block:: shell

      nodetool refresh mykeyspace team_players --load-and-stream

   See :ref:`Load and Stream <nodetool-refresh-load-and-stream>` for the ``--scope`` and ``--primary-replica-only`` options that constrain the set of target replicas. If ``--primary-replica-only`` is used, run a full cluster repair after the restore completes to replicate the data to the remaining replicas: for vnode-based keyspaces, run :doc:`nodetool repair -pr </operating-scylla/nodetool-commands/repair>` on **every** node; for tablet-based keyspaces, run :doc:`nodetool cluster repair </operating-scylla/nodetool-commands/cluster/repair>` on any single node.

.. _restore-procedure:

-------------------------------
Restore to an identical cluster
-------------------------------

This method places the snapshot files directly back into the table directories and restarts the nodes.

.. note::

   The following procedure assumes data is restored to the same cluster that was backed-up:

   - same number of nodes
   - same token range per node

   The procedure restores each node using the backup file of the **same node**.
   If this is not the case, one should use other restoration methods tools like :doc:`sstableloader </operating-scylla/procedures/cassandra-to-scylla-migration-process/>`. This procedure is much slower than restoring to the same topology cluster.

| From **one** of the nodes, recreate the schema.

``cqlsh -e "SOURCE '/path_to_schema/<schema_name.cql>'"``

| For example:

``cqlsh -e "SOURCE 'centos/db_schema.cql'"``

| **Only** a superuser should perform it.

| If the tables you are restoring already exist and contain data, truncate each of them, so that the existing data does not overwrite the restored data. Truncating a base table also truncates its materialized views and secondary indexes, no extra action is needed for them.

``cqlsh -e "TRUNCATE <keyspace_name>.<table_name>"``

| For example:

``cqlsh -e "TRUNCATE mykeyspace.team_players"``

Repeat the following steps for each node in the cluster:
--------------------------------------------------------

.. note::

   If you are restoring :doc:`encrypted backup files </operating-scylla/security/encryption-at-rest>`, make sure ScyllaDB is configured with the same keys that were used to encrypt the data before starting the restore process.

.. note::

      Best practise is **not** to restore :doc:`Materialized Views (MV) </features/materialized-views>` and :doc:`Secondary Indexes (SI) </features/secondary-indexes>` SSTables.
      It is recommended to:

      - Drop the MV and SI using `DROP MATERIALIZED VIEW` or `DROP INDEX`
      - Restore the base table only (see below)
      - Recreate the  MV or SI, using the original description from the CQL backup, using `CREATE MATERIALIZED VIEW` or `CREATE INDEX`
   

#. Run the :doc:`nodetool drain </operating-scylla/nodetool-commands/drain/>` command to ensure the data is flushed to the SSTables

#. Shut down the node

   .. include:: /rst_include/scylla-commands-stop-index.rst

#. Delete all the files in the commitlog. Deleting the commitlog will prevent the newer insert from overriding the restored data.

   ``sudo rm -rf /var/lib/scylla/commitlog/*``

#. Delete all the files in the keyspace_name_table. Note that by default the snapshots are created under ScyllaDB data directory ``/var/lib/scylla/data/keyspace_name/table_name-UUID/``.

   Make sure NOT to delete the existing snapshots in the process.

   For example:

   .. code-block:: shell

      sudo ll /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000

      -rw-r--r-- 1 scylla   scylla     66 Mar  5 09:19 nba-team_players-ka-1-CompressionInfo.db
      -rw-r--r-- 1 scylla   scylla    669 Mar  5 09:19 nba-team_players-ka-1-Data.db
      -rw-r--r-- 4 scylla   scylla     10 Mar  5 08:46 nba-team_players-ka-1-Digest.sha1
      -rw-r--r-- 1 scylla   scylla     24 Mar  5 09:19 nba-team_players-ka-1-Filter.db
      -rw-r--r-- 1 scylla   scylla    218 Mar  5 09:19 nba-team_players-ka-1-Index.db
      -rw-r--r-- 1 scylla   scylla     38 Mar  5 09:19 nba-team_players-ka-1-ScyllaDB.db
      -rw-r--r-- 1 scylla   scylla   4446 Mar  5 09:19 nba-team_players-ka-1-Statistics.db
      -rw-r--r-- 1 scylla   scylla     89 Mar  5 09:19 nba-team_players-ka-1-Summary.db
      -rw-r--r-- 4 scylla   scylla    101 Mar  5 08:46 nba-team_players-ka-1-TOC.txt
      drwx------ 5 scylla   scylla     69 Mar  6 08:14 snapshots
      drwx------ 2 scylla   scylla      6 Mar  5 08:40 upload

      sudo rm -f  /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/*

      rm: cannot remove ‘/var/lib/scylla/data/nba/team_roster-c019f8108fda11e8b16a000000000001/snapshots’: Is a directory
      rm: cannot remove ‘/var/lib/scylla/data/nba/team_roster-c019f8108fda11e8b16a000000000001/upload’: Is a directory

      sudo ll /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/

      drwx------ 5 scylla   scylla     69 Mar  6 08:14 snapshots
      drwx------ 2 scylla   scylla      6 Mar  5 08:40 upload


#. Select the snapshot you want to restore (usually the most recent one) 

   .. code-block:: shell

      /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>

   For example:

   .. code-block:: shell

      cd /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/snapshots/1487847672222


#. Copy the snapshots directory content to the ``/var/lib/scylla/data/keyspace_name/table_name-UUID/`` directory

   For example:

   .. code-block:: shell

      sudo cp -r * /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000

   .. warning::

      Copying files into the table's data directory is only allowed while the ScyllaDB service is **stopped**. To load SSTables into a running node, place them in the table's ``upload`` directory and use :doc:`nodetool refresh </operating-scylla/nodetool-commands/refresh>` instead.

#. If you have incremental backup files, copy them from the **backups** folder ``/var/lib/scylla/data/keyspace_name/table_name-UUID/backups`` to  the ``/var/lib/scylla/data/keyspace_name/table_name-UUID/`` directory

   For example:

   .. code-block:: shell

      sudo cp -r /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000/backups/* /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000

#. Make sure that all files are owned by the ``scylla`` user and group:

   .. code-block:: shell

      sudo chown -R scylla:scylla /var/lib/scylla/data/mykeyspace/team_players-6e856600017f11e790f4000000000000

#. Start the node

   .. include:: /rst_include/scylla-commands-start-index.rst

After performing the above on all nodes, run :doc:`nodetool repair </operating-scylla/nodetool-commands/repair>` on the cluster. This makes sure that the data is consistent on all nodes and between each node.
