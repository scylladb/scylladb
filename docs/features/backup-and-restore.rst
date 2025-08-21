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

   * **rclone** - the tool responsible for the upload is the scylla manger agent
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
     best to limit io-scheduling. See `scylla.yaml` `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_
     for more details.
   * For `native` backup to work, ScyllaDB node must have access to the S3 bucket.
     See `object_storage_endpoints <https://github.com/scylladb/scylladb/blob/master/docs/dev/object_storage.md>`_ for more details. 

Restore Process
---------------
The restore process is managed completely by ScyllaDB Manager.
No special configuration is needed.
Restore may be executed by rclone or natively, not depending on the backup method used.
See `ScyllaDB Manager restore documentation <https://manager.docs.scylladb.com/stable/restore/index.html>`_ for more details on how to restore data.

Metrics
-------
ScyllaDB Manager provides metrics for monitoring the backup and restore processes
    * scylla_s3_nr_connections: Total number of connections
    * scylla_s3_nr_active_connections: Total number of connections with running requests
    * scylla_s3_total_new_connections: Total number of new connections created so far
    * scylla_s3_total_read_requests: Total number of object read requests
    * scylla_s3_total_write_requests: Total number of object write requests
    * scylla_s3_total_read_bytes: Total number of bytes read from objects
    * scylla_s3_total_write_bytes: Total number of bytes written to objects
    * scylla_s3_total_read_latency_sec: Total time spent reading data from objects
    * scylla_s3_total_write_latency_sec: Total time spend writing data to objects
