SSTables 3.0 Data File Format
=============================

This article aims at describing the format of data files introduced in Cassandra 3.0. The data file is the file that actually stores the data managed by the database. As important as it is, this file does not come alone. There are other files that altogether constitute a set of components sufficient for manipulating the stored data.
The following table outlines the types of files used by Cassandra SSTables 3.0:

============================================================  ===========================  =======================================================================================  
File Type                                                     Typical Name                 Description
============================================================  ===========================  =======================================================================================    
Compression information                                       mc-1-big-CompressionInfo.db  Contains information about the compression algorithm, if used
------------------------------------------------------------  ---------------------------  ---------------------------------------------------------------------------------------
Data file                                                     mc-1-big-Data.db             Stores the actual data
------------------------------------------------------------  ---------------------------  ---------------------------------------------------------------------------------------
Checksum file                                                 mc-1-big-Digest.crc32        Contains a checksum of the data file 
------------------------------------------------------------  ---------------------------  ---------------------------------------------------------------------------------------
Bloom filter                                                  mc-1-big-Filter.db           Contains a Bloom filter to check whether particular data may be stored in the data file
------------------------------------------------------------  ---------------------------  ---------------------------------------------------------------------------------------
Index                                                         mc-1-big-Index.db            Contains the primary index of data for easier search
------------------------------------------------------------  ---------------------------  ---------------------------------------------------------------------------------------
Statistics                                                    mc-1-big-Statistics.db       Stores aggregated statistics about the data 
------------------------------------------------------------  ---------------------------  ---------------------------------------------------------------------------------------
Summary                                                       mc-1-big-Summary.db          Provides sampling of the index file (can be seen as "coarse-grained" index) 
------------------------------------------------------------  ---------------------------  ---------------------------------------------------------------------------------------
Table of contents                                             mc-1-big-TOC.txt             Lists all the files for the current SSTable  
============================================================  ===========================  ======================================================================================= 

This document focuses on the data file format but also refers to other components in parts where information stored in them affects the way we read/write the data file.

Note that the file on-disk format applies to all "m*" SSTable format versions ("mc", "md", and "me").

* The "md" format only fixed the semantics of the ``(min|max)_clustering_key`` fields in the SSTable Statistics file, 
  which are now valid for describing the accurate range of clustering prefixes present in the SSTable.
* The "me" format added the ``host_id`` of the host writing the SStable to the SSTable Statistics file. 
  It is used to qualify the commit log replay position that is also stored in the SSTable Statistics file.

See :doc:`SSTables 3.0 Statistics File Format </architecture/sstable/sstable3/sstables-3-statistics>` for more details.

Overview
........

The storage format has been significantly revamped in Cassandra 3.0 compared to the 2.x series. The primary driver for that was the fact that the previous storage format has been devised long before CQL and did not reflect its concepts and abstractions. This, in turn, hindered various fixes and led to suboptimal disk space usage.

To understand what that means, you can refer to the :doc:`SSTables 2.x data file format </architecture/sstable/sstable2/sstable-data-file>` description. In brief, in 2.x every data file is a sequence of partitions (called "rows" in pre-CQL terminology) where each partition is more or less a sequence of cells (or, more precisely, atoms with a majority of them being cells). There is no notion of columns or rows (in CQL terminology) at this level. Each cell has its full name comprised of the clustering prefix (values of all the clustering columns) followed by the non-PK column name. 

This scheme causes a lot of duplication since for every row in a given partition determined by a set of clustering columns values those values are stored in names of all the cells that belong to this row. For composite primary keys with long clustering columns values that would mean a lot of disk space wasted for no good reason. Another consequence is that the storage engine has to recognize and group cells from the row itself without knowing in advance their count or overall size.

SSTables 3.0 format addresses this by re-arranging the way data is stored. Now every partition is comprised of rows. Each row is defined by its clustering columns values and consists of a number of cells that all share those clustering columns values as their name prefix. So now one can reason about the data file as a sequence of partitions where partition consists of rows that consist of cells, whereas before it was a sequence of partitions with each partition consisting of cells. This is oversimplified but helpful for understanding the high-level picture of changes.

SSTables 3.0 data file format relies heavily on the table schema. It contains information about PK and non-PK columns, corresponding data types and width (fixed-width vs. variable-width), clustering columns ordering and so on.

Let's examine in more detail how the data representation looks in 3.0.

.. _sstables-3-building-blocks:

Building Blocks
...............

Before we describe the data format in every detail, let's define a few concepts that are used throughout the whole SSTables 3.0 specification. They act as building blocks for the new format and are responsible for noticeable space savings compared to the old one.

Variable Length Integers (Varints)
----------------------------------

Variable length integers are inspired by Google Protocol Buffers internal representation for serialised integers. They can save a significant amount of memory/space if the majority of serialised integer values are relatively small.

The internal representation of varints is explained in great detail in this `article`_. 
The main idea is that variable-length integers occupy between 1 and 9 bytes and smaller values require fewer bytes so the format is efficient for storing many relatively small values.

.. _article: https://haaawk.github.io/2018/02/26/sstables-variant-integers.html

In the structures below, `varint` will indicate an integer stored in its variable-length representation on the disk according to the described encoding.

Delta Encoding
--------------

A lot of values stored in a data file represent timestamps or TTL which values are microseconds since UNIX epoch. Those values are quite large and as such don't benefit much from being stored as varints. The idea that allows reducing timestamp/TTL values footprint is to only store the full value of the minimal timestamp/TTL per a set of objects. For other objects that have a different timestamp/TTL, we store its delta from the minimal value. The delta is typically much smaller and thus has a better chance to occupy fewer bytes upon serialisation.
Varints can be used for storing deltas just fine.

Optional Items
--------------

In order to save more disk space, some items can be omitted during serialisation. The presence or absence of a particular optional item is typically indicated by a flag or, in some cases, can be easily deduced in some way from the preceding data.
These items are marked as optional below. Keep in mind this is **not** `optional`_  but rather merely a convention to mark the items that may not be present in a particular data file.

.. _`optional`: http://en.cppreference.com/w/cpp/utility/optional

The Data File Layout
....................

The data file itself, just as before, is no more than a plain sequence of serialised partitions:

.. code:: cpp

   struct data_file {
       struct partition[];
   };

Partition
---------

References:

* `Clusterable.java`_  

..  _`Clusterable.java`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/Clusterable.java/

* `ClusteringPrefix`_ 

..  _`ClusteringPrefix`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java/

* `ClusteringComparator`_ 

..  _`ClusteringComparator`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringComparator.java/

* `Unfiltered.java`_ 

..  _`Unfiltered.java`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/Unfiltered.java/

When talking about partitions in SSTables, one needs to keep in mind that those partitions contain information about the updates to data and **not** the final state of data. That is, every partition in SSTables is a collection of records (commonly referred to as **mutations**) that have been applied sequentially to modify (insert, update or delete) the data in the data partition with a given partition key.

A partition consists of a header, an optional static row and a sequence of clusterable objects:

.. code:: cpp

   struct partition {
       struct partition_header header;
       optional<struct row> static_row; // Has IS_STATIC flag set
       struct unfiltered unfiltereds[];
   };


The ``partition_header`` contains partition key and deletion information, just as in 2.x formats.

The optional ``static_row`` is only present if there have been inserts to the table's static column(s). If no inserts are present in the Memtable that is flushed into the SSTable, the static row is not present even though the table schema may contain static columns. Static row structure is given below, for now, it suffices to say that it only differs from regular (non-static) rows in that it doesn't have a clustering prefix.

An unfiltered object is an object that can be ordered by the clustering prefix using a ``clustering_comparator`` according to its clustering values and the schema-induced ordering.

.. code:: cpp

   struct unfiltered {
   };


Any unfiltered data in a partition is either a row or a ``range_tombstone_marker``. We will examine both of them below.

Partition Header
----------------

Reference: 

`ColumnIndex.java, writePartitionHeader`_

.. _`ColumnIndex.java, writePartitionHeader`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ColumnIndex.java/

The `partition_header` format has not changed since 2.x and is defined as:

.. code:: cpp

   struct partition_header {
       be16 key_length;
       byte key[key_length];
       struct deletion_time deletion_time;
   };

The ``deletion_time`` structure determines whether this is a partition tombstone - i.e., whether the whole partition has been deleted, and if so, when:

.. code:: cpp

   struct deletion_time {
       be32 local_deletion_time;
       be64 marked_for_delete_at;
   };

The special value LIVE = (MAX_BE32, MIN_BE64), i.e., the bytes `7F FF FF FF 80 00 00 00 00 00 00 00`, is the default for live, undeleted, partitions. ``marked_for_delete_at`` is a timestamp (typically in microseconds since the UNIX epoch) after which data should be considered deleted. If set to MIN_BE64, the data has not been marked for deletion at all. ``local_deletion_time`` is the local server timestamp (in seconds since the UNIX) epoch when this tombstone was created - this is only used for purposes of purging the tombstone after ``gc_grace_seconds`` have elapsed.

Row
---

References:

`UnfilteredSerializer.java, serialize`_ 

.. _`UnfilteredSerializer.java, serialize`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L125/

A `row` is represented with the following layout:

.. code:: cpp

   struct row {
       byte flags;
       optional<byte> extended_flags;
       // only present for non-static rows
       optional<struct clustering_block[]> clustering_blocks; 
       varint row_body_size;
       varint prev_unfiltered_size; // for backward traversing
       optional<struct liveness_info> liveness_info;
       optional<struct delta_deletion_time> deletion_time;
       optional<varint[]> missing_columns;
       cell[] cells;
   };

Row Flags
---------

The first byte `flags` is a bitwise-or of the following flags:

.. code:: cpp

   enum class row_flags {
       // Signal the end of the partition. Nothing follows a <flags> field with that flag.
       END_OF_PARTITION = 0x01, 
       // Whether the encoded unfiltered is a marker or a row. All following flags apply only to rows.
       IS_MARKER = 0x02, 
       // Whether the encoded row has a timestamp (i.e. its liveness_info is not empty).
       HAS_TIMESTAMP = 0x04, 
       // Whether the encoded row has some expiration info (i.e. if its liveness_info contains TTL and local_deletion).
       HAS_TTL = 0x08, 
       // Whether the encoded row has some deletion info.
       HAS_DELETION = 0x10, 
       // Whether the encoded row has all of the columns from the header present.
       HAS_ALL_COLUMNS = 0x20, 
       // Whether the encoded row has some complex deletion for at least one of its complex columns.
       HAS_COMPLEX_DELETION = 0x40, 
       // If present, another byte is read containing the "extended flags" below.
       EXTENSION_FLAG = 0x80 
   };


If `EXTENSION_FLAG` is set, the following byte `extended_flags` is a bitwise-or of the following flags:

.. code:: cpp

   enum class row_extended_flags {
       // Whether the encoded row is a static. If there is no extended flag, the row is assumed not static.
       IS_STATIC = 0x01, 
       // Whether the row deletion is shadowable. If there is no extended flag (or no row deletion), the deletion is assumed not shadowable. This flag is deprecated - see CASSANDRA-11500.
       // This flag is not supported by ScyllaDB and SSTables that have this flag set fail to be loaded.
       HAS_SHADOWABLE_DELETION_CASSANDRA = 0x02, 
       // A ScyllaDB-specific flag (not supported by Cassandra) that indicates the presence of a shadowable tombstone.
       // See below for details
       HAS_SHADOWABLE_DELETION_SCYLLA = 0x80, 
   };

As mentioned earlier, every partition may or may not have a static row. If present, the static row precedes any other rows or range tombstone markers and has ``EXTENSION_FLAG`` set in ``flags`` and ``IS_STATIC`` set in ``extended_flags``. As you might expect, the static row will not have any clustering information just by its definition.

Clustering Blocks
-----------------

If ``IS_STATIC`` is not set or ``extended_flags`` byte is missing, the row has a list of clustering blocks that represents the values of clustering columns.

.. code:: cpp

   struct clustering_block {
       varint clustering_block_header;
       simple_cell[] clustering_cells;
   };


To build the clustering blocks, all the clustering columns are split into batches of 32 (or less for the last batch). 
To calculate the ``clustering_block_header``, for each column in batch, the information about if its cell is null or empty is encoded using 2 bits in a 64-bit integer. The higher bit is set if the cell value is null or the smaller set if it is empty.
A ``null`` cell means that this column doesn't have a value in the current row. An empty cell means that the value exists but is empty in a way (e.g., if the data type is ``text`` or `blob`, this indicates it has no bytes and a zero length in it). Note that for rows, clustering cells are never ``null``. But this encoding is also used for range tombstone markers that can contain only a prefix of clustering cells with others being treated as ``null`` in this case.
The number of clustering blocks is not stored in the data file as it can be easily deduced from the schema.

The resulting integer is then stored as ``varint clustering_block_header``. Next, the cells from the current batch are serialised using simple cells serialisation which is described below.

Note that we don't store the number of clustering cells as we take this information from table schema.

Sizes 
-----

After the clustering blocks, the size of the serialised row is calculated and stored as a `varint`. This is the number of bytes between the byte after the last clustering block (inclusive) and the `flags` byte of the next `unfiltered` (exclusive) or the end of the file.

Next `varint` is the size of the previous unfiltered (not necessarily a row, can be a range tombstone marker), supposedly stored to allow backwards traversal.

Row Content
-----------

After the sizes, next goes the ``liveness_info`` which is defined as:

.. code:: cpp

   struct liveness_info {
       varint delta_timestamp;
       optional<varint> delta_ttl;
       optional<varint> delta_local_deletion_time;
   };


It is present if ``HAS_TIMESTAMP`` flag is set. This liveness information is what allows us to distinguish between a dead row (it has no live cells and its primary key liveness info is empty) and a live row but where all non-PK columns are null (it has no live cells, but its primary key liveness is not empty). Please note that the liveness info only applies to the primary key columns and has no impact on the row content.
In a way, ``liveness_info`` is a less hacky successor to CQL row marker used in 2.x data format.

In liveness_info, as well as in some other places, timestamps, TTLs and deletion time values are stored as `varint`s using delta encoding. But an astute reader may ask a reasonable question: "What are the base values these deltas are taken from?". It turns out that a Memtable maintains aggregated statistics about all the information put into it and tracks the minimal timestamp, TTL and local deletion time values across all the records. These values are used as bases for delta encoding when the Memtable is flushed onto the disk. The values themselves are stored in `-Statistics.db` file and deserialised from there before the data file is read. For more details, refer to `EncodingStats.java`_ and `Memtable.java`_, 

.. _`EncodingStats.java`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/EncodingStats.java

.. _`Memtable.java`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/Memtable.java#L232


If the TTL information was included in the modification statement, the ``HAS_TTL`` flag is set. In this case, both TTL and local deletion time values are stored as ``varints`` using deltas taken from the corresponding minimal values from Memtable stats.

For dead row markers or expired liveness info, a special value -1 of TTL is set.

If the row is deleted, `HAS_DELETION` flag is set. In this case, ``deletion_time`` is delta-encoded as follows:

.. code:: cpp

   struct delta_deletion_time {
       varint delta_marked_for_delete_at;
       varint delta_local_deletion_time;
   };


Note that the order here is different from partition header: in ``partition_header``, ``local_deletion_time`` is serialised first and then followed by ``marked_for_delete_at``. Here, delta-encoded ``marked_for_delete_at`` precedes delta-encoded ``local_deletion_time``.

Shadowable Tombstones
---------------------

Cassandra only maintains up to one tombstone for a row. In case if it is shadowable, Cassandra sets the corresponding HAS_SHADOWABLE_DELETION_CASSANDRA flag.

It turns out that this approach is imperfect and there are known issues with the current shadowable deletions support in Cassandra (see https://issues.apache.org/jira/browse/CASSANDRA-13826 for details).
To address those, ScyllaDB maintains a separate shadowable tombstone in addition to the regular one. That means a row can have up to two tombstones in ScyllaDB-written SSTables. If the second tombstone is present, the ScyllaDB-specific extended flag HAS_SHADOWABLE_DELETION_SCYLLA is set. 

Note that Cassandra does not know this flag and would consider those files invalid. This is deemed to be safe to do because shadowable tombstones can only appear in Materialized Views tables and those are not supposed to be ever exported and imported between ScyllaDB and Cassandra.

Missing Columns Encoding
------------------------

Reference:

`Columns.java, serializeSubset`_ 

.. _`Columns.java, serializeSubset`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/Columns.java#L444


Next, if `HAS_ALL_COLUMNS` flag is **not** set, the `missing_columns` field contains information about which columns are _missing_. Note that the `HAS_ALL_COLUMNS` flag doesn't necessarily mean that the row has all the columns from the table schema definition. Each Memtable keeps track of filled columns on updates so when serialised into SSTables, the columns of a particular row are compared against the superset of filled columns in the Memtable. For example, if your table has 5 non-clustering columns ('a' through 'e'), all the records in the current Memtable have only some of ('a', 'b', 'c') filled and the current row has all 'a', 'b' and 'c', it will have the `HAS_ALL_COLUMNS` flag set even though it doesn't have columns 'd' and 'e'. This information (the list of all filled columns) is also stored in `-Statistics.db`.

We have a _superset_ of columns, which is a list of all non-PK columns in the table, and a _current set of columns, which is a list of columns filled within this row. The encoding procedure is optimised towards small column sets (<64 columns) and employs slightly more complex encoding for larger sets.
When the superset columns count is < 64, a 64-bit integer is used as a bitmap with its bits set for **missing** columns and stored as a single `varint`. 

For larger supersets, the delta between the superset columns count and the current row columns count is written as the first `varint`.
The procedure then differs based on whether the number of columns in the row is less than half of the size of the superset or not.
If `columns.count() < superset.count() / 2`, the **present** columns indices are written as `varints`, otherwise the **missing** columns indices are written. The logic is clear - we just write whatever appears to be less in count.

Although the field is named `missing_columns`, one can see from the algorithm described above that in some cases the values stored are indices of present columns, not missing ones. This may be a bit confusing, but it helps to reason about it in the following way: whatever is stored can be used to get the list of missing columns.

As of today, ScyllaDB treats the whole set of columns as a superset regardless of whether all columns are ever filled or not. `See for details`_.

.. _`See for details`: https://github.com/scylladb/scylla/issues/3901

Lastly, all the cells of the current row are encoded.

Cells
-----

References:

* `BufferCell.java, Serializer.serialize`_ 

* `UnfilteredSerializer, writeComplexColumn`_ 

.. _`BufferCell.java, Serializer.serialize`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L236

.. _`UnfilteredSerializer, writeComplexColumn`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L204


.. code:: cpp

   struct cell {
   };


Any non-clustering column can be either "simple", which means it can only have a single cell associated to it in any row, or "complex", where an arbitrary number of cells can be associated. As of today, "complex" columns are those declared for non-frozen collections. All clustering columns are "simple" because non-frozen collections are allowed for non-PK columns only - if you remember, we said earlier that the cells in a clustering block are serialised using `simple_cell` serialisation.
> Note that since we encode the information about filled columns, every cell is non-null by definition. They can still be empty though.

A cell is recognised as simple or complex based the definition in the table schema for the column that owns this cell.

Simple Cells
------------

A serialised simple sell looks like:

.. code:: cpp

   struct simple_cell
   : cell {
       byte flags;
       optional<varint> delta_timestamp;
       optional<varint> delta_local_deletion_time;
       optional<varint> delta_ttl;
       optional<cell_path> path; // only in cells nested into complex_cells
       optional<struct cell_value> value;
   };
 
First, the `flags` byte is a bitwise-or of the following flags:
   
.. code:: cpp

   enum class cell_flags {
       IS_DELETED_MASK = 0x01, // Whether the cell is a tombstone or not.
       IS_EXPIRING_MASK = 0x02, // Whether the cell is expiring.
       HAS_EMPTY_VALUE_MASK = 0x04, // Whether the cell has an empty value. This will be the case for a tombstone in particular.
       USE_ROW_TIMESTAMP_MASK = 0x08, // Whether the cell has the same timestamp as the row this is a cell of.
       USE_ROW_TTL_MASK = 0x10, // Whether the cell has the same TTL as the row this is a cell of.
   };


`IS_DELETED_MASK` and `IS_EXPIRING_MASK` flags are mutually exclusive for an obvious reason that the same cell can either be deleted or live but expiring.

If `USE_ROW_TIMESTAMP_MASK` flag is **not** set, i.e., the cell timestamp differs from that of the row, the delta-encoded timestamp is stored as a `varint`.

If the cell is a tombstone (`IS_DELETED_MASK` is set) or expiring (`IS_EXPIRING_MASK` is set) and its local deletion time and TTL differ from those of the row (`USE_ROW_TTL_MASK` is **not** set), cell delta-encoded local deletion time is stored as a `varint`.

If the cell is expiring (`IS_EXPIRING_MASK` is set) and its TTL differs from that of the row (`USE_ROW_TTL_MASK` is **not** set), cell delta-encoded TTL is stored as a `varint`.

A regular simple cell that belongs to a simple column doesn't have a `path` item. This one only appears in cells wrapped by a `complex_cell` (see below).

Finally, if the cell value is not empty (`HAS_EMPTY_VALUE_MASK` is **not** set), it is serialised. It can be a fixed-width value (for fixed-width CQL data types like `int` or `boolean`) or a variable-width one (for CQL data types like `text` or `blob`). Those are encoded differently since we don't need to store the length of fixed-width values (it can be deduced from the table schema definition) but have to for variable-length cells.

.. code:: cpp

   struct cell_value {
       optional<varint> length;
       byte value[];
   };


Complex Cells
-------------

A complex cell acts as a kind of a container for multiple simple cells distinguished by so-called `cell_path`:

.. code:: cpp

   struct complex_cell
   : cell {
       optional<struct delta_deletion_time> complex_deletion_time;
       varint items_count;
       struct simple_cell[items_count];
   };

First, let's describe what a "complex deletion" means. A "complex deletion" is a deletion applied to all the items (or sub-cells) of a complex cell. For instance, for a collection, this corresponds to a full collection deletion. Note that if complex_deletion is absent, individual items (sub-cells) can still be deleted within a complex cell. 

The presence or absence of `complex_deletion_time` is determined by the `HAS_COMPLEX_DELETION` flag in row `flags`. Interestingly, this flag is set if any of row complex columns has a complex deletion, so in practice, it will be written for all the complex columns if at least one of them has been entirely deleted in the current row.

Next, the number of items of the current complex cell (aka sub-cells) is stored as a `varint`.

Lastly, we have the complex cell items serialised one by one. They, in fact, represent simple cells that have an additional `path` component that allows to distinguish them.

.. code:: cpp

   struct cell_path {
       varint length;
       byte value[length];
   };

As of today, the only implementation of the `cell_path` is the one for collections so it always has a single value which is:

* an auto-generated `timeuuid` for lists
* the current map key for maps
* the actual value for sets (the `complex_cell_item.value` is empty in this case)

Range Tombstone Marker
----------------------

References:

* `RangeTombstoneMarker.java`_  

* `RangeTombstoneBoundMarker.java`_  

* `RangeTombstoneBoundaryMarker.java`_ 

.. _`RangeTombstoneMarker.java`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/RangeTombstoneMarker.java

.. _`RangeTombstoneBoundMarker.java`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/RangeTombstoneBoundMarker.java

.. _`RangeTombstoneBoundaryMarker.java`: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/RangeTombstoneBoundaryMarker.java

In SSTables, we have a notion of range tombstones that represent tombstones that cover a slice/range of rows.
Since 3.0, they are stored as pairs of range tombstone markers indicating its start and its end so that each tombstone corresponds to two ordered `unfiltered` objects or ``range_tombstone_marker`` type. This is done to simplify merging. The ordering of ``unfiltered`` by their clustering prefixes makes sure that a range tombstone start marker precedes any rows covered by the range and a range tombstone end marker goes after them.
Given that, it becomes clear that as a reader advances through a data file, it has to maintain only up to one range tombstone deletion mark. If the deletion mark is filled, all the rows read are considered deleted until the corresponding end marker is met.

Every range tombstone marker can be either a `range_tombstone_bound_marker`, which represents a single bound of a slice or a `range_tombstone_boundary_marker`, which represents a boundary between two adjacent range tombstones and notifies an "open end, closed start" type of bound. The latter makes for both disk space savings (requires 1 range tombstone marker instead of 2) and simplifies the merging logic of the database storage engine.

.. code:: cpp

   struct range_tombstone_marker {
       byte flags = IS_MARKER;
       byte kind_ordinal;
       be16 bound_values_count;
       struct clustering_block[] clustering_blocks;
       varint marker_body_size;
       varint prev_unfiltered_size;
   };


The first byte represents `flags` which for a range tombstone marker always contain a single ``IS_MARKER`` flag.
Next byte is a slice bound kind ordinal:

.. code:: cpp

   enum class bound_kind : uint8_t {
       EXCL_END_BOUND = 0,
       INCL_START_BOUND = 1,
       EXCL_END_INCL_START_BOUNDARY = 2,
       STATIC_CLUSTERING = 3,
       CLUSTERING = 4,
       INCL_END_EXCL_START_BOUNDARY = 5,
       INCL_END_BOUND = 6,
       EXCL_START_BOUND = 7
   };

It can be one of {0, 1, 6, 7} for a ``range_tombstone_bound_marker`` and either 2 or 5 for a ``range_tombstone_boundary_marker``.
Next, the two-byte integer value of the number of non-null columns in the clustering prefix is stored, followed by clustering blocks that are serialised in the exact same way as for rows.
> For rows, the length of the clustering prefix is not stored as it is always full, meaning that all its clustering columns are non-`null`. For a range tombstone marker, however, the trailing columns can be `null` and so we need to know how many cells are encoded in the clustering blocks.

After that, the size of the marker body is stored as a `varint`, followed by another `varint` that contains the size of the previous unfiltered (row or range tombstone marker).

Next fields depend on whether this range tombstone marker is a bound or a boundary.

.. code:: cpp

   struct range_tombstone_bound_marker
   : range_tombstone_marker {
       struct delta_deletion_time deletion_time;
   };

   struct range_tombstone_boundary_marker
   : range_tombstone_marker {
       struct delta_deletion_time end_deletion_time;
       struct delta_deletion_time start_deletion_time;
   };

For a bound marker, it stores its delta-encoded deletion time, and for boundary marker, we store two delta-encoded deletion time structures, one for the end bound and another for the start bound within the boundary.

Shadowable Deletion
-------------------

Initially, an extended `HAS_SHADOWABLE_DELETION` flag has been introduced in 3.0 format to solve a tricky problem described in [CASSANDRA-10261](https://issues.apache.org/jira/browse/CASSANDRA-10261). Later some other problems have been discovered ([CASSANDRA-11500](https://issues.apache.org/jira/browse/CASSANDRA-11500)) which led to a more generic approach that deprecated shadowable tombstones and used expired liveness info instead.

As a result, this flag is not supposed to be written for new SSTables by Cassandra.
ScyllaDB tracks the presence of this flag and fails to load files that have it set.


References
----------

* `Putting some structure in the storage engine <https://www.datastax.com/2015/12/storage-engine-30>`_ - An overview of the main features of the SSTables 3.0 format including some measurements of disk space savings compared to 2.x.

* `Introduction To The Apache Cassandra 3.x Storage Engine <https://thelastpickle.com/blog/2016/03/04/introductiont-to-the-apache-cassandra-3-storage-engine.html>`_ - A detailed article on SSTables 3.0 data file format with lots of references to the relevant code parts. This article has a few inaccuracies like complex cells format but otherwise is a great read with a lot of useful details. 
 
* `About Deletes and Tombstones in Cassandra <http://thelastpickle.com/blog/2016/07/27/about-deletes-and-tombstones.html>`_ - An article that covers tombstones in SSTables 3.0 in a great detail.

* `Overview of CASSANDRA-8099 changes <https://apache.googlesource.com/cassandra/+show/cassandra-3.0.0-rc2/guide_8099.md>`_ - An engineering description of changes in SSTables data format in Cassandra 3.0. Explains well a variety of higher-level concepts and sheds light on some lower-level implementation details.

* There is hardly any documentation that would be more accurate and up-to-date than the actual `source code <https://github.com/apache/cassandra/tree/cassandra-3.0>`_.
  
.. include:: /rst_include/apache-copyrights.rst
   