====================
Scylla Python Driver
====================

The Scylla Python driver is shard aware and contains extensions for a ``tokenAwareHostPolicy``.
Using this policy, the driver can select a connection to a particular shard based on the shard’s token. 
As a result, latency is significantly reduced because there is no need to pass data between the shards. 

Read the `documentation <https://python-driver.docs.scylladb.com/>`_ to get started or visit the Github project `Scylla Python driver <https://github.com/scylladb/python-driver/>`_.

As the Scylla Python Driver is a drop-in replacement for DataStax Python Driver, no code changes are needed to use the driver. 
Use the Scylla Python driver for better compatibility and support for Scylla with Python-based applications.


More information 
----------------

* `Scylla Python Driver Documentation <https://python-driver.docs.scylladb.com/>`_
* `Scylla Python Driver on GitHub <https://github.com/scylladb/python-driver/>`_
* `ScyllaDB University: Coding with Python <https://university.scylladb.com/courses/using-scylla-drivers/lessons/coding-with-python/>`_ 
