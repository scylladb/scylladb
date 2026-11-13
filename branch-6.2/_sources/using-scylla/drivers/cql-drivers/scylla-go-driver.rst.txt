==================
ScyllaDB Go Driver
==================

The `ScyllaDB Go driver <https://github.com/scylladb/gocql>`_ is shard aware and contains extensions for a tokenAwareHostPolicy supported by ScyllaDB 2.3 and onwards.
It is is a fork of the `GoCQL Driver <https://github.com/gocql/gocql>`_ but has been enhanced with capabilities that take advantage of ScyllaDB's unique architecture.
Using this policy, the driver can select a connection to a particular shard based on the shard’s token. 
As a result, latency is significantly reduced because there is no need to pass data between the shards. 

The protocol extension spec is `available here <https://github.com/scylladb/scylla/blob/master/docs/dev/protocol-extensions.md>`_. 
The ScyllaDB Go Driver is a drop-in replacement for gocql. 
As such, no code changes are needed to use this driver. 
All you need to do is rebuild using the ``replace`` directive in your ``mod`` file.

**To download and install the driver**, visit the `Github project <https://github.com/scylladb/gocql>`_.


Using CDC with Go
-----------------

When writing applications, you can now use our `Go Library <https://github.com/scylladb/scylla-cdc-go>`_ to simplify writing applications that read from ScyllaDB CDC.

More information 
----------------

* `ScyllaDB Gocql Driver project page on GitHub <https://github.com/scylladb/gocql>`_ - contains the source code as well as a readme and documentation files.
* `ScyllaDB University: Golang and ScyllaDB <https://university.scylladb.com/courses/using-scylla-drivers/lessons/golang-and-scylla-part-1/>`_
   A three-part lesson with in-depth examples from  executing a few basic CQL statements with a ScyllaDB cluster using the Gocql driver, to the different data types that you can use in your database tables and how to store these binary files in ScyllaDB with a simple Go application.
