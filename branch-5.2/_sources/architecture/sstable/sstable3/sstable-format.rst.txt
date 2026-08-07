SSTable 3.0 Format in ScyllaDB
===============================

Scylla supports the same SSTable format as Apache Cassandra 3.0.
You can simply place SSTables from a Cassandra data directory into a Scylla uploads directory
and use the ``nodetool refresh`` command to ingest their data into the table.

Looking more carefully, you will see that Scylla maintains more,
smaller, SSTables than Cassandra does. On Scylla, each core manages its
own subset of SSTables. This internal sharding allows each core (shard)
to work more efficiently, avoiding the complexity and delays of multiple
cores competing for the same data

.. include:: /rst_include/architecture-index.rst

.. include:: /rst_include/apache-copyrights.rst
