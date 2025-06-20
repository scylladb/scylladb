Failed Schema Sync
===================

The total binary footprint of all ScyllaDB columns, tables, etc., must fit a single commit log segment size 
divided by two. If this requirement is not met, schema sync may fail. The error message may resemble the following:

.. code:: console

    Oct 06 22:12:47 ip-172-21-1-122 scylla[1213356]:  [shard 0] storage_service - Fail to pull schema from 172.21.2.210: std::invalid_argument (Mutation of 27837439 bytes is too large for the maximum size of 16777216)
    Oct 06 22:12:47 ip-172-21-1-122 scylla[1213356]:  [shard 0] migration_manager - Pulling schema from 172.21.2.210:0 

In such a case, you may need to increase the commitlog segment size limit. To do this, 
go to ``/etc/scylla/scylla.yaml`` and set the ``commitlog_segment_size_in_mb`` parameter to a higher value.

.. note::
    The ``commitlog_segment_size_in_mb`` parameter must be set to the same value on **all nodes** in a cluster. 