======
Backup
======

.. include:: /operating-scylla/manager/_common/note-versions.rst

Using sctool, you can backup and restore your managed Scylla clusters under Scylla Manager.
Backups are scheduled in the same manner as repairs. You can start, stop, and track backup operations on demand.
Scylla Manager can backup to Amazon S3 and S3 compatible API storage providers such as Ceph or MinIO.


Benefits of using Scylla Manager backups
========================================

Scylla Manager automates the backup process and allows you to configure how and when a backup occurs.
The advantages of using Scylla Manager for backup operations are:

* Data selection - backup a single table or an entire cluster, the choice is up to you
* Data deduplication - prevents multiple uploads of the same SSTable
* Data retention - purge old data automatically when all goes right, or failover when something goes wrong
* Data throttling - control how fast you upload or Pause/resume the backup
* Lower disruption to the workflow of the Scylla Manager Agent due to cgroups and/or CPU pinning 
* No cross-region traffic - configurable upload destination per datacenter

The backup process
==================

The backup procedure consists of multiple steps executed sequentially.
It runs parallel on all nodes unless you limit it with the ``--snapshot-parallel`` or ``--upload-parallel`` :ref:`flag <sctool-backup-parameters>`.

#. **Snapshot** - Take a :term:`snapshot <Snapshot>` of data on each node (according to backup configuration settings).
#. **Schema** - (Optional) Upload the schema CQL to the backup storage destination, this requires that you added the cluster with ``--username`` and ``--password`` flags. See :doc:`Add Cluster <add-a-cluster>` for reference.
#. **Upload** - Upload the snapshot to the backup storage destination.
#. **Manifest** - Upload the manifest file containing metadata about the backup.
#. **Purge** - If the retention threshold has been reached, remove the oldest backup from the storage location.

Prepare nodes for backup
========================

#. Create a storage location for the backup.
   Currently, Scylla Manager supports `Amazon S3 buckets <https://aws.amazon.com/s3/>`_ .
   You can use an S3 bucket that you already created.
   We recommend using an S3 bucket in the same region where your nodes are to minimize cross-region data transfer costs.
   In multi-dc deployments, you should create a bucket per datacenter, each located in the datacenter's region.
#. Choose how you want to configure access to the S3 Bucket.
   You can use an IAM role (recommended), or you can add your AWS credentials to the agent configuration file.
   The latter method is less secure as you will be propagating each node with this security information, and in cases where you need to change the key, you will have to replace it on each node.

**To use an IAM Role**

#. Create an `IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide//iam-roles-for-amazon-ec2.html>`_ for the S3 bucket which adheres to your company security policy.
#. `Attach the IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide//iam-roles-for-amazon-ec2.html#attach-iam-role>`_ to **each EC2 instance (node)** in the cluster.

Sample IAM policy for *scylla-manager-backup* bucket:

.. code-block:: none

   {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads"
                ],
                "Resource": [
                    "arn:aws:s3:::scylla-manager-backup"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": [
                    "arn:aws:s3:::scylla-manager-backup/*"
                ]
            }
        ]
   }

**To add your AWS credentials the Scylla Manager Agent configuration file**

Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

#. Uncomment the ``s3:`` line, for parameters, note the two spaces in front, it's a yaml file.
#. Uncomment and set ``access_key_id`` and ``secret_access_key``, refer to :ref:`AWS Credentials Configuration <manager-2.1-aws-credentials>` for details.
#. If NOT running in AWS EC2 instance, uncomment and set ``region`` to a region where you created the S3 bucket.

Troubleshooting
---------------

To troubleshoot Node to S3 connectivity issues, you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location s3:<your S3 bucket name>

Schedule a backup
=================

The most recommended way to run a backup is across an entire cluster.
Backups can be scheduled to run on single or multiple datacenters, keyspaces, or tables.
The backup procedure can be customized, allowing you to plan your backups according to your IT policy.
All parameters can be found in the :ref:`sctool reference <sctool-backup>`.
If you want to check if all of your nodes can connect to the backup storage location, see `Perform a Dry Run of a Backup`_.

**Prerequisites**

#. Backup locations (S3 buckets) created.
#. Access rights to backup locations granted to Nodes, see `Prepare Nodes for Backup`_.

Create a scheduled backup
-------------------------

Use the example below to run the sctool backup command.

.. code-block:: none

   sctool backup -c <id|name> -L <list of locations> [-s <date>] [-i <time-unit>]

where:

* ``-c`` - the :ref:`name <sctool-cluster-add>` you used when you created the cluster
* ``-L`` - points to backup storage location in ``s3:<your S3 bucket name>`` format or ``<your DC name>:s3:<your S3 bucket name>`` if you want to specify a location for a datacenter
* ``-s`` - the time you want the backup to begin
* ``-i`` - the time interval you want to use in between consecutive backups

If you want to run the backup only once, see `Create an ad-hoc backup`_.
In case when you want the backup to start immediately, but you want it to schedule it to repeat at a determined interval, leave out the start flag (``-s``) and set the interval flag (``-i``) to the time you want the backup to reoccur.

Schedule a daily backup
.......................

This command will schedule a backup on 9th Dec 2019 at 15:15:06 UTC time zone, a backup will be repeated every day, and all the data will be stored in S3 under the ``my-backups`` bucket.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups' -s '2019-12-09T15:16:05Z' -i 24h
   backup/3208ff15-6e8f-48b2-875c-d3c73f545410

Command returns the task ID (backup/3208ff15-6e8f-48b2-875c-d3c73f545410, in this case).
This ID can be used to query the status of the backup task, to defer the task to another time, or to cancel the task See :ref:`Managing Tasks <sctool-managing-tasks>`.

Schedule a daily, weekly, and monthly backup
............................................
This command series will schedule a backup on 9th Dec 2019 at 15:15:06 UTC time zone and will repeat the backup every day (keeping the last 7 days), every week (keeping the previous week), and every month (keeping the previous month).
All the data will be stored in S3 under the ``my-backups`` bucket.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 7 -s '2019-12-09T15:16:05Z' -i 24h

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 2 -s '2019-12-09T15:16:05Z' -i 7d

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 1 -s '2019-12-09T15:16:05Z' -i 30d

Schedule a backup for a specific DC, keyspace, or table
--------------------------------------------------------
In order to schedule a backup of a particular datacenter, you have to specify ``-dc`` parameter.
You can specify more than one DC, or use a glob pattern to match multiple DCs or exclude some of them.

For example, you have the following DCs in your cluster: dc1, dc2, dc3

Backup one specific DC
......................

In this example you backup the only dc1 every 2 days.

.. code-block:: none

   sctool backup -c prod-cluster --dc 'dc1' -L 's3:dc1-backups' -i 2d


Backup all DCs except for those specified
.........................................

.. code-block:: none

   sctool backup -c prod-cluster -i 30d --dc '*,!dc2' -L 's3:my-backups'

Backup to a specific location per DC
....................................

If your data centers are located in different regions, you can also specify different locations.
If your buckets are created in the same regions as your data centers, you may save some bandwidth costs.

.. code-block:: none

   sctool backup -c prod-cluster -i 30d --dc 'eu-dc,us-dc' -L 's3:eu-dc:eu-backups,s3:us-dc:us-backups'

Backup a specific keyspace or table
...................................

In order to schedule a backup of a particular keyspace or table, you have to provide ``-K`` parameter.
You can specify more than one keyspace/table or use a glob pattern to match multiple keyspaces/tables or exclude them.

.. code-block:: none

   sctool backup -c prod-cluster -i 30d -K 'auth_service.*,!auth_service.lru_cache' --dc 'dc1' -L 's3:dc1-backups'

Create an ad-hoc backup
-----------------------

An ad-hoc backup runs immediately and does not repeat.
This procedure shows the most frequently used backup commands.
Additional parameters can be used. Refer to :ref:`backup parameters <sctool-backup-parameters>`.

**Procedure**

To run an immediate backup on the prod-cluster cluster, saving the backup in my-backups, run the following command
replacing the ``-c`` cluster flag with your cluster's cluster name or ID and replace the ``-L`` flag with your backup's location:

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups'

Perform a dry run of a backup
-----------------------------

We recommend to use ``--dry-run`` parameter prior to scheduling a backup.
It's a useful way to verify whether all necessary prerequisites are fulfilled.
Add the parameter to the end of your backup command, so if it works, you can erase it and schedule the backup with no need to make any other changes.

Dry run verifies if nodes are able to access the backup location provided.
If it's not accessible, an error message will be displayed, and the backup is not be scheduled.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:test-bucket' --dry-run
   NOTICE: dry run mode, backup is not scheduled

   Error: failed to get backup target: location is not accessible
    192.168.100.23: failed to access s3:test-bucket make sure that the location is correct and credentials are set
    192.168.100.22: failed to access s3:test-bucket make sure that the location is correct and credentials are set
    192.168.100.21: failed to access s3:test-bucket make sure that the location is correct and credentials are set

The dry run gives you the chance to resolve all configuration or access issues before executing an actual backup.

If the dry run completes successfully, a summary of the backup is displayed. For example:

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:backups' --dry-run
   NOTICE: dry run mode, backup is not scheduled

   Data Centers:
     - dc1
     - dc2
   Keyspaces:
     - system_auth all (2 tables)
     - system_distributed all (1 table)
     - system_traces all (5 tables)
     - auth_service all (3 tables)

   Disk size: ~10.4GB

   Locations:
     - s3:backups
   Bandwidth Limits:
     - Unlimited
   Snapshot Parallel Limits:
     - All hosts in parallel
   Upload Parallel Limits:
     - All hosts in parallel

   Retention: Last 3 backups

List the contents of a specific backup
=======================================

List all backups in s3
----------------------

Lists all backups currently in storage that are managed by Scylla Manager.

.. code-block:: none

   sctool backup list -c prod-cluster
   Snapshots:
     - sm_20191210145143UTC
     - sm_20191210145027UTC
     - sm_20191210144833UTC
   Keyspaces:
     - system_auth (2 tables)
     - system_distributed (1 table)
     - system_traces (5 tables)
     - auth_service (3 tables)

List files that were uploaded during a specific backup
-------------------------------------------------------

You can list all files that were uploaded during a particular backup.

To list the files use:

.. code-block:: none

   sctool backup files -c prod-cluster --snapshot-tag sm_20191210145027UTC

   s3://backups/backup/sst/cluster/1d781354-9f9f-47cc-ad45-f8f890569656/dc/dc1/node/ece658c2-e587-49a5-9fea-7b0992e19607/keyspace/auth_service/table/roles/5bc52802de2535edaeab188eecebb090/mc-2-big-CompressionInfo.db      auth_service/roles
   s3://backups/backup/sst/cluster/1d781354-9f9f-47cc-ad45-f8f890569656/dc/dc1/node/ece658c2-e587-49a5-9fea-7b0992e19607/keyspace/auth_service/table/roles/5bc52802de2535edaeab188eecebb090/mc-2-big-Data.db         auth_service/roles
   s3://backups/backup/sst/cluster/1d781354-9f9f-47cc-ad45-f8f890569656/dc/dc1/node/ece658c2-e587-49a5-9fea-7b0992e19607/keyspace/auth_service/table/roles/5bc52802de2535edaeab188eecebb090/mc-2-big-Digest.crc32    auth_service/roles
   [...]

Additional resources
--------------------

:doc:`Scylla Snapshots </kb/snapshots>`

Delete backup snapshot
=========================

If you decide that you don't want to wait until a particular snapshot expires according to its retention policy, there is a command which allows you to delete a single snapshot from a provided location.

This operation is aware of the Manager deduplication policy and will not delete any SSTable file referenced by another snapshot.

.. warning:: This operation is irreversible! Use it with great caution!

.. code-block:: none

   sctool backup delete -c prod-cluster -L s3:backups --snapshot-tag sm_20191210145027UTC

Once a snapshot is deleted, it won't show up in the backup listing anymore.
