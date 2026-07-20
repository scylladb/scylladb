=====================
ScyllaDB Java Driver 
=====================

ScyllaDB Java Driver is forked from `DataStax Java Driver <https://github.com/datastax/java-driver>`_ with enhanced capabilities, taking advantage of ScyllaDB's unique architecture.

The ScyllaDB Java driver is shard aware and contains extensions for a ``tokenAwareHostPolicy``.
Using this policy, the driver can select a connection to a particular shard based on the shard’s token. 
As a result, latency is significantly reduced because there is no need to pass data between the shards. 

Use the ScyllaDB Java driver for better compatibility and support for ScyllaDB with Java-based applications.

Read the `documentation <https://java-driver.docs.scylladb.com/>`_ to get started or visit the `Github project <https://github.com/scylladb/java-driver>`_.

The driver architecture is based on layers. At the bottom lies the driver core. 
This core handles everything related to the connections to a ScyllaDB cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple, relatively low-level API on top of which higher-level layers can be built. 

The ScyllaDB Java Driver is a drop-in replacement for the DataStax Java Driver.
As such, no code changes are needed to use this driver.

Using CDC with Java
-------------------

When writing applications, you can now use our  `Java Library <https://github.com/scylladb/scylla-cdc-java>`_ to simplify writing applications that read from ScyllaDB CDC.

More information
----------------
* `ScyllaDB Java Driver Docs <https://java-driver.docs.scylladb.com/>`_ 
* `ScyllaDB Java Driver project page on GitHub <https://github.com/scylladb/java-driver/>`_ - Source Code
* `ScyllaDB University: Coding with Java <https://university.scylladb.com/courses/using-scylla-drivers/lessons/coding-with-java-part-1/>`_ - a three-part lesson with in-depth examples from  executing a few basic CQL statements with a ScyllaDB cluster using the Java driver, to the different data types that you can use in your database tables and how to store these binary files in ScyllaDB with a simple Java application. 

