=======================================
ScyllaDB Migration Tools: An Overview
=======================================

The following migration tools are available for migrating to ScyllaDB from compatible databases, 
such as Apache Cassandra, or from other Scylla clusters (ScyllaDB Open Source or Enterprise):

* From SSTable to SSTable
    - Based on ScyllaDB refresh
    - On a large scale, it requires tooling to upload / transfer files from location to location.
* From SSTable to CQL.
    - :doc:`sstableloader</operating-scylla/admin-tools/sstableloader/>`
* From CQL to CQL
    - `Spark Migrator <https://github.com/scylladb/scylla-migrator>`_.  The Spark migrator allows you to easily transform the data before pushing it to the destination DB.

* From DynamoDB to Scylla Alternator
    - `Spark Migrator <https://github.com/scylladb/scylla-migrator>`_.  The Spark migrator allows you to easily transform the data before pushing it to the destination DB.
