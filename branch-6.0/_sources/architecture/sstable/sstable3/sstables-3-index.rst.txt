SSTables 3.0 Index File Format
==============================

The SSTables index file, together with the :doc:`SSTables summary file </architecture/sstable/sstable3/sstables-3-summary/>` , provides a way to efficiently locate a partition with a specific key or some position within a partition in the :doc:`SSTables data file</architecture/sstable/sstable3/sstables-3-data-file-format/>`.

Broadly, the index file lists the keys in the data file in order, giving for each key its position in the data file. The summary file, which is held in memory, contains samples of these keys, pointing to their position in the index file. So to efficiently search for a specific key, we use the summary to find a (relatively short) range of positions in the index file which may hold this key, then read this range from the index file and look for the specific key. The details on how to further pinpoint specific columns inside very long partitions are explained below.

The Index File Layout
.....................

Building Blocks
---------------

The :ref:`building blocks <sstables-3-building-blocks>` of the new index format are same as those of the new data format. 
This includes ``varints``, ``optional`` values and delta encoding.

Description
-----------

The index file format changes in v3.0 from the :doc:`old format </architecture/sstable/sstable2/sstable-format>` are moderate compared to how significantly has changed the data file layout. Still, there are some new fields and entities that are worth a detailed look. Such fields are the ``offsets`` array added to ``index_entry`` and ``end_open_marker`` structure added to ``promoted_index_block``.

The index file is a long sequence of entries:

.. code:: cpp

   struct index_file {
       struct index_entry entries[]; 
   };

   struct index_entry {
       be16 key_length;
       char key[key_length];
       varint position; // decoded into a 64-bit integer
       varint promoted_index_length; // decoded into a 32-bit integer
       byte promoted_index[promoted_index_length];
   };

Here, key is the SSTable partition key, and position is the position of that partition in the data file.

For large partitions, finding a specific range of columns requires more than just starting position to be efficient. For those, the so-called promoted index is stored along. It samples partition at roughly ``column_index_size_in_kb`` (by default, 64 KB) blocks and gives the clustering prefixes range for each block.

The reason for the strange name ``promoted index`` is that it originally started as a separately stored column index, until in `Cassandra 1.2`_, when it was "promoted" to live inside the SSTables index file, reducing the number of seeks required to find a given column from 3 to 2.

.. _`Cassandra 1.2`: https://issues.apache.org/jira/browse/CASSANDRA-2319

The ``promoted_index_length`` stores the number of bytes right after this field and up to the end of the current ``index_entry``. If ``promoted_index_length`` is 0, there is no promoted index for this entry and nothing follows it.
 
The structure of promoted index (when ``promoted_index_length != 0``) is as follows:

.. code:: cpp

   struct promoted_index {
       varint partition_header_length; //decoded into a 64-bit integer
       struct deletion_time deletion_time;
       varint promoted_index_blocks_count; // decoded into a 32-bit integer
       struct promoted_index_block blocks[promoted_index_blocks_count];
       be32 offsets[promoted_index_blocks_count];
   }

The first field, ``partition_header_length``, stores the length of serialized partition key, partition tombstone and static row, if present, in the data file. This allows to skip over the partition prefix straight to the first clustered row.



The `deletion time` structure is described in :doc:`SSTables 3.0 data file format</architecture/sstable/sstable3/sstables-3-data-file-format/>` article.  It is simply a copy of the partition ``deletion_time``, either live or a partition tombstone. We need this copy inside the index, because normally the deletion_time of a partition appears in its very beginning in the data file, but with the promoted index we intend to skip directly to the middle of a very large partition, so we don't want to also read the beginning of the partition just to read its partition tombstone.

After deletion time mark, the number of promoted index blocks is stored as a `varint`. It can be zero for small partitions or >= 2 (apparently, it makes no sense to store a single promoted index block).

Promoted Index Blocks
---------------------

The promoted index block looks like:

.. code:: cpp

   struct promoted_index_block {
       struct clustering_prefix first_name;
       struct clustering_prefix last_name;
       varint offset;
       varint delta_width;
       byte end_open_marker_present;
       optional<struct deletion_time> end_open_marker;
   };

Clustering Prefixes
-------------------

Here, ``first_name`` and ``last_name`` are the serialised prefixes of clustering columns marking the boundaries of the block. They are stored as follows:

.. code:: cpp

   struct clustering_prefix {
       byte kind;
       optional<be16> size;
       struct clustering_block clustering_blocks[];
   };

The first byte, `kind`, stores the serialised ordinal of the value from the ``bound_kind`` enum (see :doc:`SSTables 3.0 data file format</architecture/sstable/sstable3/sstables-3-data-file-format/>`) for the enum definition).
It can be either 4 (for `CLUSTERING`) when corresponds to a row or one of 0, 1 and 2 (for ``EXCL_END_BOUND``, ``INCL_START_BOUND`` or ``EXCL_END_INCL_START_BOUNDARY`` accordingly) for a range tombstone marker.

The next field, ``size``, is only present for range tombstone markers, i.e., when ``kind != 4``. It contains the actual number of values in the clustering prefix (the prefix length). For rows, it is not needed as rows have all the clustering columns values (full clustering prefix) so the count is taken from the table schema definition.

The sequence of clustering blocks is serialised in exactly the same way as clustering blocks in rows or range tombstone markers in data files. You can refer to the detailed description :ref:`here <sstables-3-building-blocks>` 


Other Promoted Index Blocks Fields
----------------------------------

The ``offset`` field stores the start of the block relative to the beginning of the current partition in the data file.

The `width` field is a delta-encoded value representing the length of the current promoted index block in the index file. The size of promoted index blocks is a configurable value set through the ``column_index_size_in_kb`` option in the config file. It defaults to 64KB and this is the base value that `width` delta is taken from.

As of today, the base value is not stored along in the index file and always equals 64KB == 65536.

Note that since the actual promoted index block size can be configured to be less than 64K, it can so happen that delta-encoding width would result in a negative value. Because of that, the stored value should be treated as **signed**.  

The next byte after `width` called ``end_open_marker_present`` is effectively a `boolean` which tells us whether the following ``end_open_marker`` structure is serialised or not. If ``end_open_marker_present`` is 1, ``end_open_marker`` should be read.

The ``end_open_marker`` structure is present in case if the end bound of the current promoted index block falls in between two range tombstone markers denoting a range. 
In other words, that means that the block of ``unfiltereds`` in the data file that corresponds to this promoted index block contains a range tombstone start marker but its counterpart (range tombstone end marker) lies somewhere past that block. 
It is set for a range tombstone marker of either the ``INCL_START_BOUND`` or the ``EXCL_END_INCL_START_BOUNDARY`` kind and contains the open bound deletion time.

The ``end_open_marker`` has to do with how range tombstones are organised in SSTables 3.0. and is closely connected with the merging logic of storage engine code. That code expects all the data it reads from SSTables to come strictly ordered and it expects range tombstone markers to always come in pairs (the combined "boundary" type marker is just treated as two markers - an end marker for the previous RT followed immediately by a start marker for a new RT.
So when a slice of data is being read, it may contain an end RT marker without the corresponding start RT marker. In order to properly handle this, reader code returns a start RT marker corresponding to the slice start bound with the deletion time taken from the ``end_open_marker`` structure see: SSTableIterator.java.handlePreSliceData_ 

.. _SSTableIterator.java.handlePreSliceData: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/columniterator/SSTableIterator.java#L125

The higher-level reason for this design is that C* 3.0 storage design aims to be as iterator-based as possible and as such doesn't re-arrange the data it reads (and without start RT marker, such re-arrangement would be due).

Lastly, there is an `offsets` array which stores the offsets for all the promoted index blocks.
Its size is the same as the value stored in ``promoted_index_blocks_count``. The first offset is always 0, and others are the offsets of corresponding promoted index blocks from the beginning of ``promoted_index.blocks``.

It is possible to read those offsets (commonly referred to as "offsets map") directly without reading the entire promoted index. This, in turn, allows searching through the promoted index using binary search which results in O(log N) reads instead of O (N). 

References
..........

The following code parts in Cassandra deal with index serialisation/de-serialisation:

* `ColumnIndex.java, buildRowIndex`_ 

* `RowIndexEntry.java`_ 

* `IndexInfo.java`_

.. _`ColumnIndex.java, buildRowIndex`: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/ColumnIndex.java

.. _`IndexInfo.java`: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/io/sstable/IndexInfo.java

.. _`RowIndexEntry.java`: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/RowIndexEntry.java


Additional Information
----------------------

The offset map reduces the complexity of search in the promoted index from O(N) to O(log N) using the offsets map.
There are some better alternatives that should further improve the performance of index-related operations using BTree-like structures. They are not released yet but planned for Cassandra 4. See `CASSANDRA-9754`_.

.. _`CASSANDRA-9754`: https://issues.apache.org/jira/browse/CASSANDRA-9754

.. include:: /rst_include/apache-copyrights.rst
