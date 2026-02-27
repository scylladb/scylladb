====================================
Scylla Migration Tools: An Overview
====================================

The following migration tools are available for migrating to Scylla from Compatible databases, like Apache Cassandra or other Scylla Clusters (Open Source or Enterprise):

* From SSTable to SSTable
    - Based on Scylla refresh
    - On a large scale, it requires tooling to upload / transfer files from location to location.
    - For example, `unirestore <https://github.com/scylladb/field-engineering/tree/master/unirestore>`_: SSTable to SSTable
* From SSTable to CQL.
    - :doc:`sstableloader</operating-scylla/admin-tools/sstableloader/>`
* From CQL to CQL
    - `Spark Migrator <https://github.com/scylladb/scylla-migrator>`_.  The Spark migrator allows you to easily transform the data before pushing it to the destination DB.

* From DynamoDB to Scylla Alternator
    - `Spark Migrator <https://github.com/scylladb/scylla-migrator>`_.  The Spark migrator allows you to easily transform the data before pushing it to the destination DB.
