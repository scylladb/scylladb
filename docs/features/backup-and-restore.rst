===========================
Backup And Restore Overview
===========================

Backup and restore are critical components of data management, ensuring that your data is safe and can be recovered in case of loss or corruption. This document provides an overview of the backup and restore process, including best practices, tools, procedures, metrics and more


Process Overview
----------------
**The Backup process** is managed by ScyllaDB Manager as a whole.
The overview given here is for a single node, ScyllaDB Manager is responsible to orchestrate backups across the cluster.
For backup, a snapshot is created, and then the data is copied to a remote location - normally an S3 bucket, Google Cloud Storage, or a similar service.

**The Restore process** is also managed by ScyllaDB manager and it involves copying the data back from the remote location to an empty ScyllaDB node.
Restoring to a live cluster is not yet supported.

Backup Process
--------------

#. **Snapshot Creation**: A snapshot of the data is created on the ScyllaDB node.
   This is a point-in-time copy of the data.
#. **Upload Data**: The snapshot data is transferred to a remote storage location,
   such as an S3 bucket or Google Cloud Storage. You can upload data in two ways:

   * **rclone** - the tool responsible for the upload is the scylla manager agent
     that runs on the node.

          - It runs side by side with scylla and therefore may interfere
            with ScyllaDB performance.
          - It supports many cloud storage providers.

   * **Native upload** - ScyllaDB itself is responsible for the upload.

          - It takes into consideration ScyllaDB performance and does
            not interfere with it.
          - It supports only S3 compatible storage providers.

      See the `ScyllaDB Manager backup documentation <https://manager.docs.scylladb.com/stable/backup/index.html>`_
      for more details on how to configure the upload method.

#. **Native Configuration**: 

   * For `native` backup to work without interference to users' workload, it is
     best to limit io-scheduling. See :ref:`stream_io_throughput_mb_per_sec <confprop_stream_io_throughput_mb_per_sec>` for details.     
   * For `native` backup to work, ScyllaDB node must have access to the S3 bucket.
     See :ref:`Configuring Object Storage <object-storage-configuration>` for details.

Restore Process
---------------
The restore process is managed completely by ScyllaDB Manager.
No special configuration is needed.
Restore may be executed by rclone or natively, not depending on the backup method used.
See `ScyllaDB Manager restore documentation <https://manager.docs.scylladb.com/stable/restore/index.html>`_ for more details on how to restore data.
