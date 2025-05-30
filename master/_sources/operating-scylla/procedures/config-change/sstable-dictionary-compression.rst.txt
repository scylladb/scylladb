==============================================
Shared-Dictionary Compression for SSTables
==============================================

Scylla now supports dictionary-based compression for SSTables, which improves compression ratios by sharing compression dictionaries across compression chunks.

Background
----------

Traditional SSTable compression works on a chunk-by-chunk basis, with each chunk compressed independently. Dictionary-based compression improves this by using a shared dictionary across all compression chunks, providing better compression ratios by leveraging patterns that appear across chunks.

Benefits
--------

* Better compression ratios
* Optimized for specific table data patterns

How to Use Dictionary Compression
---------------------------------

Enabling Dictionary Compression
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable dictionary compression for a table, set its ``sstable_compression`` property to one of the following values, depending on your choice of algorithm:

* ``LZ4WithDictsCompressor``
* ``ZstdWithDictsCompressor``

For example, using CQL::

    ALTER TABLE keyspace.table
    WITH compression = {'sstable_compression': 'ZstdWithDictsCompressor'};

Once enabled, Scylla will automatically train a dictionary for the table when sufficient data is available.

Manual Dictionary Training
^^^^^^^^^^^^^^^^^^^^^^^^^^

You can manually trigger dictionary training using the REST API::

    curl -X POST "http://node-address:10000/storage_service/retrain_dict?keyspace=mykeyspace&table=mytable"

Estimating Compression Ratios
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To choose the best compression configuration, you can estimate compression ratios using the REST API::

    curl -X GET "http://node-address:10000/storage_service/estimate_compression_ratios?keyspace=mykeyspace&table=mytable"

This will return a report with estimated compression ratios for various combinations of compression
parameters (algorithm, chunk size, zstd level, dictionary).

Configuration Options
---------------------

Several options related to this feature are available in ``scylla.yaml``,
but they shouldn't be useful during regular usage. Most of them control the
frequency of automatic dictionary training in various ways.

Refer to :doc:`Configuration Parameters </reference/configuration-parameters/>`
for details.

Compatibility
-------------

SSTables compressed with dictionary compression are not readable by older
Scylla versions or by Cassandra. If a downgrade to a backward-compatible format
is necessary:

1. Disable dictionary compression for new SSTables by switching all
   dictionary-compressed tables to Cassandra-compatible compressors.
2. Apply the new setting by using ``nodetool upgradesstables -a`` to rewrite
   all SSTables.
