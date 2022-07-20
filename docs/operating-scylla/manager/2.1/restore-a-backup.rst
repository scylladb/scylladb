================
Restore a Backup
================

.. include:: /operating-scylla/manager/_common/note-versions.rst

This document provides information on how to restore data from backups that were taken using the Scylla Manager.

There are two restore scenarios:

#. Backup to the same topology cluster.
   For example, restore data to the same source cluster.
#. Backup to a different topology cluster.
   For example, restore data to a smaller or bigger cluster, a cluster with a different rack or DC topology, or different token distribution.

**Workflow**

#. `Prepare for restore`_
#. `Upload data to Scylla`_

Prepare for restore
===================

No matter which backup scenario you are using, the procedures in this workflow apply.

**Workflow**

#. `Make sure Scylla cluster is up`_
#. `Install Scylla Manager`_
#. `Register the cluster with the Scylla Manager`_
#. `Identify relevant snapshot`_
#. `Restore the schema`_

Make sure Scylla cluster is up
------------------------------

Make sure that your Scylla cluster is up and that there are no issues with networking, disk space, or memory.
If you need help, you can check official documentation on :doc:`operational procedures for cluster management </operating-scylla/procedures/cluster-management/index>`.

Install Scylla Manager
----------------------

You need a working Scylla Manager setup to list backups. If you don't have it installed, please follow official instructions on :doc:`how to install Scylla Manager <install>` first.

Nodes must have access to the locations of the backups as per instructions in the official documentation for :ref:`installing Scylla Manager Agent <manager-2.1-prepare-nodes-for-backup>`.

Register the cluster with the Scylla Manager
--------------------------------------------

This section only applies to situations where a registered cluster that was originally used for the backup is missing from the Scylla Manager.
In that case, a new cluster must be registered before you can access the backups created with the old one.
This example demonstrates adding a cluster named "cluster1" with initial node IP 18.185.31.99, instructs Scylla Manager not to schedule a default repair, and  forcing uuid of the new cluster to ebec29cd-e768-4b66-aac3-8e8943bcaa76:

   .. code-block:: none

      sctool cluster add --host 18.185.31.99 --name cluster1 --without-repair -id ebec29cd-e768-4b66-aac3-8e8943bcaa76
      ebec29cd-e768-4b66-aac3-8e8943bcaa76
      __  
      /  \     Cluster added! You can set it as default, by exporting its name or ID as env variable:
      @  @     $ export SCYLLA_MANAGER_CLUSTER=ebec29cd-e768-4b66-aac3-8e8943bcaa76
      |  |     $ export SCYLLA_MANAGER_CLUSTER=cluster1
      || |/    
      || ||    Now run:
      |\_/|    $ sctool status -c cluster1
      \___/    $ sctool task list -c cluster1

Cluster is created, and we can proceed to list old backups.
If the uuid of the old cluster is lost, there is a workaround with ``--all-clusters`` parameter.
In that case, just register the cluster and proceed to the next step.

Identify relevant snapshot
--------------------------

**Procedure**

#. List all available backups and choose the one you would like to restore.
   Run: :ref:`sctool backup list <sctool-backup-list>`, to lists all backups for the cluster.
   This command will list backups only created with provided cluster (``-c clust1``).
   If you don't have uuid of the old cluster, you can use ``--all-clusters`` to list all backups from all clusters that are available in the target location:

   .. code-block:: none

      sctool backup list -c cluster1 --all-clusters -L s3:backup-bucket
      Cluster: 7313fda0-6ebd-4513-8af0-67ac8e30077b

      Snapshots:
      - sm_20200513131519UTC (563.07GiB)
      - sm_20200513080459UTC (563.07GiB)
      - sm_20200513072744UTC (563.07GiB)
      - sm_20200513071719UTC (563.07GiB)
      - sm_20200513070907UTC (563.07GiB)
      - sm_20200513065522UTC (563.07GiB)
      - sm_20200513063046UTC (563.16GiB)
      - sm_20200513060818UTC (534.00GiB)
      Keyspaces:
      - system_auth (4 tables)
      - system_distributed (2 tables)
      - system_schema (12 tables)
      - system_traces (5 tables)
      - user_data (100 tables)

   Here, for example, we have eight different snapshots to choose from.
   Snapshot tags encode the date they were taken in UTC time zone.
   For example, ``sm_20200513131519UTC`` was taken on 13/05/2020 at 13:15 and 19 seconds UTC.
   The data source for the listing is the cluster backup locations.
   Listing may take some time, depending on how big the cluster is and how many backups there are.

.. _restore-backup-restore-schema:

Restore the schema
------------------

Scylla Manager 2.1 can store schema with your backup.
To extract schema files for each keyspace from the backup, please refer to the official documentation for :doc:`extracting schema from the backup <extract-schema-from-backup>`. For convenience, here is the continuation of our example with the list of steps for restoring schema:

#. Download schema from the backup store to the current dir. It's in the first line of the ``backup_files.out`` output:

   .. code-block:: none

      sctool backup files --cluster my-cluster -L s3:backup-bucket -T sm_20200513104924UTC --with-version | head -n 1 | xargs -n2 aws s3 cp
      download: s3://backup-bucket/backup/schema/cluster/7313fda0-6ebd-4513-8af0-67ac8e30077b/task_001ce624-9ac2-4076-a502-ec99d01effe4_tag_sm_20200513104924UTC_schema.tar.gz to ./task_001ce624-9ac2-4076-a502-ec99d01effe4_tag_sm_20200513104924UTC_schema.tar.gz

#. Extract schema files by decompressing archive:

   .. code-block:: none

      mkdir ./schema
      tar -xf task_001ce624-9ac2-4076-a502-ec99d01effe4_tag_sm_20200513104924UTC_schema.tar.gz -C ./schema
      ls ./schema
      system_auth.cql  system_distributed.cql  system_schema.cql  system_traces.cql  user_data.cql


* If you do *not* have the schema file available, you can `extract the schema from system table <https://manager.docs.scylladb.com/branch-2.2/restore/extract-schema-from-metadata.html>`_.

Full schema restore procedure can be found at :ref:`steps 1 to 5 <restore-procedure>`.
For convenience, here is the list of steps for our example (WARNING: these can be destructive operations):

#. Run the ``nodetool drain`` command to ensure the data is flushed to the SSTables.

#. Shut down the node:

   .. code-block:: none

      sudo systemctl stop scylla-server

#. Delete all files in the commitlog:

   .. code-block:: none

      sudo rm -rf /var/lib/scylla/commitlog/*

#. Delete all the files in the user_data.data_* tables (only files, not directories):

   .. code-block:: none

      sudo rm -f  /var/lib/scylla/data/user_data/data_0-6e856600017f11e790f4000000000000/*

If cluster is added with CQL credentials (see :doc:`Add Cluster <add-a-cluster>` for reference) Scylla Manager would backup schema in CQL format.
To obtain CQL schema from a particular backup, use ``sctool backup files`` command, for example:

.. code-block:: none

   sctool backup files -c my-cluster -L s3:backups -T sm_20191210145143UTC

The first output line is a path to schemas archive, for example:

.. code-block:: none

   s3://backups/backup/schema/cluster/ed63b474-2c05-4f4f-b084-94541dd86e7a/task_287791d9-c257-4850-aef5-7537d6e69d90_tag_sm_20200506115612UTC_schema.tar.gz      ./

This archive contains a single CQL file for each keyspace in the backup.

.. code-block:: none

    tar -ztvf task_287791d9-c257-4850-aef5-7537d6e69d90_tag_sm_20200506115612UTC_schema.tar.gz
    -rw------- 0/0            2366 2020-05-08 14:38 system_auth.cql
    -rw------- 0/0             931 2020-05-08 14:38 system_distributed.cql
    -rw------- 0/0           11557 2020-05-08 14:38 system_schema.cql
    -rw------- 0/0            4483 2020-05-08 14:38 system_traces.cql

To restore the schema, you need to execute the files with cqlsh command.

**Procedure**

#. Download schema archive

   .. code-block:: none

      aws s3 cp s3://backups/backup/schema/cluster/ed63b474-2c05-4f4f-b084-94541dd86e7a/task_287791d9-c257-4850-aef5-7537d6e69d90_tag_sm_20200506115612UTC_schema.tar.gz ./

#. Extract CQL files from archive

   .. code-block:: none

      tar -xzvf task_287791d9-c257-4850-aef5-7537d6e69d90_tag_sm_20200506115612UTC_schema.tar.gz

#. Copy CQL files for desired keyspaces to a cluster node
#. On node execute CQL files using cqlsh

   .. code-block:: none

      cqlsh -f my_keyspace.cql

Upload data to Scylla
=====================

You can either upload the data:

* `To the same cluster`_: with the same nodes, topology, and the same token distribution **OR**
* `To a new cluster`_: of any number of nodes


To the same cluster
-------------------

List the backup files
.....................

List the backup files needed on each node and save the list to a file.

If you are listing old backups from the new cluster use ``--all-clusters`` parameter.

.. code-block:: none

   sctool backup files -c cluster1 --snapshot-tag sm_20200513131519UTC \
   --with-version \
   --location s3:backup-bucket \
    > backup_files.out

Snapshot information is now stored in ``backup_files.out`` file.
Each line of the ``backup_files.out`` file contains mapping between the path to the SSTable file in the backup bucket, and it's mapping to keyspace/table.
If Scylla Manager is configured to store database schemas with the backups, then the first line in the file listing is the path to the schema archive.

For example:

.. code-block:: none

   s3://backup-bucket/backup/sst/cluster/7313fda0-6ebd-4513-8af0-67ac8e30077b/dc/AWS_EU_CENTRAL_1/node/92de78b1-6c77-4788-b513-2fff5a178fe5/keyspace/user_data/table/data_65/a2667040944811eaaf9d000000000000/la-72-big-Index.db 	 user_data/data_65-a2667040944811eaaf9d000000000000

Path contains metadata, for example:

* Cluster ID - 7313fda0-6ebd-4513-8af0-67ac8e30077b
* Data Center - AWS_EU_CENTRAL_1
* Directory - /var/lib/scylla/data/user_data/data_65-a2667040944811eaaf9d000000000000/
* Keyspace - user_data

.. code-block:: none

   sctool backup files -c prod-cluster --snapshot-tag sm_20191210145027UTC \
   --with-version > backup_files.out

Each line describes a backed-up file and where it should be downloaded. For example

.. code-block:: none

   s3://backups/backup/sst/cluster/1d781354-9f9f-47cc-ad45-f8f890569656/dc/dc1/node/ece658c2-e587-49a5-9fea-7b0992e19607/keyspace/auth_service/table/roles/5bc52802de2535edaeab188eecebb090/mc-2-big-CompressionInfo.db      auth_service/roles-5bc52802de2535edaeab188eecebb090

This file has to be copied to:

* Cluster - 1d781354-9f9f-47cc-ad45-f8f890569656
* Data Center - dc1
* Node - ece658c2-e587-49a5-9fea-7b0992e19607
* Directory - /var/lib/scylla/data/auth_service/roles-5bc52802de2535edaeab188eecebb090/upload

Download the backup files
.........................

This step must be executed on **each node** in the cluster.

#. Copy ``backup_files.out`` file as ``/tmp/backup_files.out`` on the node.

#. Run ``nodetool status`` to get to know the node ID.

#. Download data into table directories.
   As the file is kept in S3 so we can use S3 CLI to download it (this step may be different with other storage providers).
   Grep can be used to filter specific files to restore.
   With node UUID we can filter files only for a single node.
   With a keyspace name, we can filter files only for a single keyspace.

   .. code-block:: none

      cd /var/lib/scylla/data

      # Filter only files for a single node.
      grep ece658c2-e587-49a5-9fea-7b0992e19607 /tmp/backup_files.out | xargs -n2 aws s3 cp

#. Make sure that all files are owned by the Scylla user and group.
   We must ensure that permissions are right after copy:

   .. code-block:: none

      sudo chown -R scylla:scylla /var/lib/scylla/data/user_data/

#. Start the Scylla nodes:

   .. code-block:: none

      sudo systemctl start scylla-server

Repair
......

After performing the above on all nodes, repair the cluster with Scylla Manager Repair.
This makes sure that the data is consistent on all nodes and between each node.

To a new cluster
----------------

In order to restore a backup to a cluster that has a different topology, you have to use an external tool called :doc:`sstableloader </operating-scylla/procedures/cassandra-to-scylla-migration-process>`.
This procedure is much slower than restoring to the same topology cluster.

**Procedure**

#. Start up the nodes if they are not running after schema restore:

   .. code-block:: none

      sudo systemctl start scylla-server

#. List all the backup files and save the list to a file.

   Use ``--all-clusters`` if you are restoring from the cluster that no longer exists.

   .. code-block:: none

      sctool backup files -c cluster1 --snapshot-tag sm_20200513131519UTC --location s3:backup-bucket > backup_files.out

#. Copy ``backup_files.out`` file as ``/tmp/backup_files.out`` on the host where ``sstableloader`` is installed.

#. Download all files created during backup into temporary location:

   .. code-block:: none

      mkdir snapshot
      cd snapshot
      # Create temporary directory structure.
      cat /tmp/backup_files.out | awk '{print $2}' | xargs mkdir -p
      # Download snapshot files.
      cat /tmp/backup_files.out | xargs -n2 aws s3 cp

#. Execute the following command for each table by providing a list of node IP addresses and path to sstable files on node that has sstableloader installed:

   .. code-block:: none

      # Loads table user_data.data_0 into four node cluster.
      sstableloader -d '35.158.14.221,18.157.98.72,3.122.196.197,3.126.2.205' ./user_data/data_0 --username scylla --password <password>

After tables are restored, verify the validity of your data by running queries on your database.
