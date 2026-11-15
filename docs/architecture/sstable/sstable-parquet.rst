Parquet SSTables
================

An SSTable can store its data in a Parquet file instead of the native Data file.
The other components of the SSTable, such as the index and the bloom filter, stay the same.
Such SSTables have the format ``pq`` and the version ``aa``, so their files are named
``aa-<generation>-pq-<component>``.

Choosing the format
-------------------

The ``sstable_data_format`` :ref:`cluster configuration option <cluster-config>` chooses the
format of new SSTables of a table. Its values are ``native`` (the default) and ``parquet``.
It can be set for a table, a keyspace, or the whole cluster:

.. code-block:: cql

   ALTER TABLE ks.tbl WITH sstable_data_format = 'parquet';
   ALTER KEYSPACE ks WITH sstable_data_format = 'parquet';
   ALTER CLUSTER WITH sstable_data_format = 'parquet';

A table value overrides a keyspace value, which overrides a cluster value.
Setting the option to ``NULL`` removes the value at that scope.
A materialized view and a secondary index follow the value of their base table.

You can set the option only when every node in the cluster runs a version that supports Parquet
SSTables. Until then, no node writes them.

Some tables ignore the option and always use the native format:

* The tables of the system keyspaces, such as ``system``, ``system_schema``, ``system_auth``,
  ``system_distributed``, and ``system_traces``.
* Tables with ``COMPACT STORAGE``. Such a table also refuses to load a Parquet SSTable.

When the option takes effect
----------------------------

A change of the option needs no restart.
It applies to the next SSTable that the table writes: in a memtable flush, a compaction, a
repair, or a streaming operation.
Existing SSTables keep their format until a compaction rewrites them.
When a node restarts, it reads the option before it loads its SSTables and replays its commit log,
so the SSTables that it writes then have the table's format too.
A node in maintenance mode does not read the option before that, and writes those SSTables in the
native format.
A table can hold SSTables of both formats at once, and reads work in either case.
Each SSTable is read according to its own format, also when it arrives through streaming,
restore, or ``nodetool refresh`` in a table that does not use Parquet.

Converting a table
------------------

To convert the existing SSTables of a table, set the option and then run
:doc:`nodetool upgradesstables </operating-scylla/nodetool-commands/upgradesstables>` on every node:

.. code-block:: console

   nodetool upgradesstables ks tbl

``upgradesstables`` rewrites every SSTable whose format differs from the table's choice.
To convert a table back to the native format, set the option to ``native``, or to ``NULL`` if no
broader scope chooses Parquet, and run ``upgradesstables`` again.

Compression
-----------

A Parquet file compresses each page on its own. The table's ``compression`` option chooses the
algorithm:

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - ``sstable_compression``
     - Parquet page compression
   * - None (``compression = {}``)
     - None
   * - ``LZ4Compressor``, ``LZ4WithDictsCompressor``
     - LZ4
   * - ``SnappyCompressor``
     - Snappy
   * - ``DeflateCompressor``
     - Gzip
   * - ``ZstdCompressor``, ``ZstdWithDictsCompressor``
     - Zstd, at the table's ``compression_level``

The default compression of a user table is ``LZ4WithDictsCompressor``, which gives LZ4 pages.
To get Zstd pages, choose ``ZstdCompressor`` or ``ZstdWithDictsCompressor``:

.. code-block:: cql

   ALTER TABLE ks.tbl WITH compression = {'sstable_compression': 'ZstdCompressor'};

Parquet SSTables use no trained compression dictionaries.
The ``chunk_length_in_kb`` sub-option has no effect on them, because pages replace the compressed
chunks of the native format.

Integrity
---------

Each page of a Parquet file carries a checksum, and every read verifies it.
The checksum of the file's footer is stored in ``Scylla.db``, and the footer is verified when the
SSTable is opened.
A Parquet SSTable has no ``CRC.db`` and no ``CompressionInfo.db``.

These operations check a Parquet SSTable:

* ``nodetool scrub --mode=VALIDATE`` checks the checksum of every page, the footer, and the digest of
  the whole file. It reads the data and checks the order of partitions and rows, and it checks the
  index against the data.
* ``scylla sstable validate-checksums`` checks the checksum of every page, the footer, and the
  digest.
* ``nodetool scrub`` in the other modes reads the data, as for native SSTables. A damaged page
  stops the read at the first row of that page.
