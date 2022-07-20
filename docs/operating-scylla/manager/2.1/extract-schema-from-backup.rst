==============================
Extract schema from the backup
==============================

.. include:: /operating-scylla/manager/_common/note-versions.rst

.. versionadded:: 2.1 Scylla Manager

The first step to restoring a Scylla Manager backup is to restore the CQL schema from a text file.
Scylla Manager version 2.1 creates a backup up of matching schema along with the snapshot.
If you created the backup with Scylla Manager version 2.0 or you didn't provide credentials for a schema backup in Scylla Manager version 2.1, follow the instructions in how to restore your schema from system table(deleted document).

If not, follow these steps to restore the schema from the Scylla Manager backup that has the schema stored along with the snapshot:

**Procedure**

#. List available backups:

   .. code-block:: none

      sctool backup list --cluster my-cluster --location s3:backup-bucket

#. List files located in snapshot you want to restore. The first line contains a path to the schema so pipe it to ``aws s3 cp`` for download it to the current directory. For example:

   .. code-block:: none

      sctool backup files --cluster my-cluster -L s3:backup-bucket -T sm_20200513104924UTC --with-version | head -n 1 | xargs -n2 aws s3 cp
      download: s3://backup-bucket/backup/schema/cluster/7313fda0-6ebd-4513-8af0-67ac8e30077b/task_001ce624-9ac2-4076-a502-ec99d01effe4_tag_sm_20200513104924UTC_schema.tar.gz to ./task_001ce624-9ac2-4076-a502-ec99d01effe4_tag_sm_20200513104924UTC_schema.tar.gz


#. Create a directory to store the schema files and extract the archive containing the schema.

   .. code-block:: none

      mkdir ./schema
      tar -xf task_001ce624-9ac2-4076-a502-ec99d01effe4_tag_sm_20200513104924UTC_schema.tar.gz -C ./schema
      ls ./schema
      system_auth.cql  system_distributed.cql  system_schema.cql  system_traces.cql  user_data.cql

Listed files are schema files for each keyspace in the backup. You can use each cql file to restore needed keyspace and continue the :ref:`restore procedure <restore-backup-restore-schema>`
