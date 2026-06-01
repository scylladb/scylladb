SSTable 3.0 Format in ScyllaDB
===============================

ScyllaDB supports the same SSTable format as Apache Cassandra 3.0.
You can simply place SSTables from a Cassandra data directory into a ScyllaDB uploads directory
and use the ``nodetool refresh`` command to ingest their data into the table.

Looking more carefully, you will see that ScyllaDB maintains more,
smaller, SSTables than Cassandra does. On ScyllaDB, each core manages its
own subset of SSTables. This internal sharding allows each core (shard)
to work more efficiently, avoiding the complexity and delays of multiple
cores competing for the same data.

SSTable Format Variants
------------------------

ScyllaDB 3.x SSTables come in three format variants, selected via the ``sstable_format``
parameter in ``scylla.yaml``:

``ms``
  Introduces a trie-based SSTable index.
  For details, see :doc:`SSTable ms Index (Trie-Based) <sstable-ms-index>`.

``me``
  The baseline 3.x format, default from ScyllaDB 2022.2 through 2026.1.

``md``
  An earlier 3.x variant. Only used when upgrading from an existing ``md`` cluster.
  The ``sstable_format`` parameter is ignored if set to ``md``.

Existing SSTables are not rewritten automatically when upgrading to 2026.2.
They are upgraded to the ``ms`` format on the next compaction.

.. include:: /rst_include/architecture-index.rst

.. include:: /rst_include/apache-copyrights.rst