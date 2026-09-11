SSTables 3.0 Statistics File Format
===================================

This file stores metadata for SSTable. There are 4 types of metadata:

1. **Validation metadata** - used to validate SSTable correctness.
2. **Compaction metadata** - used for compaction.
3. **Statistics** - some information about SSTable which is loaded into memory and used for faster reads/compactions.
4. **Serialization header** - keeps information about SSTable schema.

General structure
.................

The file is composed of two parts. First part is a table of content which allows quick access to a selected metadata. Second part is a sequence of metadata stored one after the other.
Let's define array template that will be used in this document.

.. code:: cpp

   struct array<LengthType, ElementType> {
       LengthType number_of_elements;
       ElementType elements[number_of_elements];
   }


Table of content

.. code:: cpp

   using toc = array<be32<int32_t>, toc_entry>;

   struct toc_entry {
       // Type of metadata
       // | Type                 | Integer representation |
       // |----------------------|------------------------|
       // | Validation metadata  | 0                      |
       // | Compaction metadata  | 1                      |
       // | Statistics           | 2                      |
       // | Serialization header | 3                      |
       be32<int32_t> type;
       // Offset, in the file, at which this metadata entry starts
       be32<int32_t> offset;
   }

The toc array is sorted by the type field of its members.

Validation metadata entry
.........................

.. code:: cpp
  
   struct validation_metadata {
       // Name of partitioner used to create this SSTable.
       // Represented by UTF8 string encoded using modified UTF-8 encoding.
       // You can read more about this encoding in:
       // https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#modified-utf-8
       // https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#readUTF()
       Modified_UTF-8_String partitioner_name;
       // The probability of false positive matches in the bloom filter for this SSTable
       be64<double> bloom_filter_fp_chance;
   }

Compaction metadata entry
.........................

.. code:: cpp

   // Serialized HyperLogLogPlus which can be used to estimate the number of partition keys in the SSTable.
   // If this is not present then the same estimation can be computed using Summary file.
   // Encoding is described in:
   // https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/cardinality/HyperLogLogPlus.java
   using compaction_metadata = array<be32<int32_t>, be8>;

Statistics entry
................

This entry contains some parts of ``EstimatedHistogram``, ``StreamingHistogram`` and ``CommitLogPosition`` types. Let's have a look at them first.

EstimatedHistogram
------------------

.. code:: cpp

   // Each bucket represents values from (previous bucket offset, current offset].
   // Offset for last bucket is +inf.
   using estimated_histogram = array<be32<int32_t>, bucket>;

   struct bucket {
       // Offset of the previous bucket
       // In the first bucket this is offset of the first bucket itself because there's no previous bucket.
       // The offset of the first bucket is repeated in second bucket as well.
       be64<int64_t> prev_bucket_offset;
       // This bucket value
       be64<int64_t> value;
   }

StreamingHistogram
------------------

.. code:: cpp

   struct streaming_histogram {
       // Maximum number of buckets this historgam can have
       be32<int32_t> bucket_number_limit;
       array<be32<int32_t>, bucket> buckets;
   }

   struct bucket {
       // Offset of this bucket
       be64<double> offset;
       // Bucket value
       be64<int64_t> value;
   }

CommitLogPosition
-----------------

.. code:: cpp

   struct commit_log_position {
       be64<int64_t> segment_id;
       be32<int32_t> position_in_segment;
   }


Whole entry
-----------

.. code:: cpp

   struct statistics {
       // In bytes, uncompressed sizes of partitions
       estimated_histogram partition_sizes;
       // Number of cells per partition
       estimated_histogram column_counts;
       commit_log_position commit_log_upper_bound;
       // Typically in microseconds since the unix epoch, although this is not enforced
       be64<int64_t> min_timestamp;
       // Typically in microseconds since the unix epoch, although this is not enforced
       be64<int64_t> max_timestamp;
       // In seconds since the unix epoch
       be32<int32_t> min_local_deletion_time;
       // In seconds since the unix epoch
       be32<int32_t> max_local_deletion_time;
       be32<int32_t> min_ttl;
       be32<int32_t> max_ttl;
       // compressed_size / uncompressed_size
       be64<double> compression_rate;
       // Histogram of cell tombstones.
       // Keys are local deletion times of tombstones
       streaming_histogram tombstones;
       be32<int32_t> level;
       // The difference, measured in milliseconds, between repair time and midnight, January 1, 1970 UTC
       be64<int64_t> repaired_at;
       // Minimum and Maximum clustering key prefixes present in the SSTable (valid since the "md" SSTable format).
       // Note that:
       // - Clustering rows always have the full clustering key.
       // - Range tombstones may have a partial clustering key prefix.
       // - Partition tombstones implicitly apply to the full, unbound clustering range.
       // Therefore, an empty (min|max)_clustering_key denotes a respective unbound range,
       // derived either from an open-ended range tombstone, or from a partition tombstone.
       clustering_bound min_clustering_key;
       clustering_bound max_clustering_key;
       be8<bool> has_legacy_counters;
       be64<int64_t> number_of_columns;
       be64<int64_t> number_of_rows;
    
       // Version MA of SSTable 3.x format ends here.
       // It contains only one commit log position interval - [NONE = new CommitLogPosition(-1, 0), upper bound of commit log]
    
       commit_log_position commit_log_lower_bound;
    
       // Version MB of SSTable 3.x format ends here.
       // It contains only one commit log position interval - [lower bound of commit log, upper bound of commit log].
    
       array<be32<int32_t>, commit_log_interval> commit_log_intervals;

       // Versions MC and MD of SSTable 3.x format end here.

       // UUID of the host that wrote the SSTable.
       // Qualifies all commitlog positions in the SSTable Statistics file.
       
       UUID host_id;
   }

   using clustering_bound = array<be32<int32_t>, clustering_column>;
   using clustering_column = array<be16<uint16_t>, be8>;

   struct commit_log_interval {
       commit_log_position start;
       commit_log_position end;
   }


Serialization header
....................

.. code:: cpp

   struct serialization_header {
       vint<uint64_t> min_timestamp;
       vint<uint32_t> min_local_deletion_time;
       vint<uint32_t> min_ttl;
       // If partition key has one column then this is the type of this column.
       // Otherwise, this is a CompositeType that contains types of all partition key columns.
       type partition_key_type;
       array<vint<uint32_t>, type> clustering_key_types;
       columns static_columns;
       columns regular_columns;
   }

   using columns = array<vint<uint32_t>, column>;

   struct column {
       array<vint<uint32_t>, be8> name;
       type column_type;
   }

   // UTF-8 string
   using type = array<vint<uint_32_t>, be8>;

Type encoding
-------------

Type is just a byte buffer with an unsigned variant integer (32-bit) length. It is a UTF-8 string. All leading spaces, tabs and newlines are skipped. Null or empty string is a bytes type. First segment of non-blank characters should contain only alphanumerical characters and special chars like ``'-'``, ``'+'``, ``'.'``, ``'_'``, ``'&'``. This is the name of the type. If type name does not contain any '.' then it gets "org.apache.cassandra.db.marshal." prepended to itself. Then an "instance" static field is taken from this class. If the first non-blank character that follows type name is '(' then "getInstance" static method is invoked instead. Remaining string is passed to this method as a parameter. There are following types:

=======================  ============
Type                     Parametrized
=======================  ============
Ascii Type               No
-----------------------  ------------
Boolean Type             No
-----------------------  ------------  
Bytes Type               No
-----------------------  ------------
Byte Type                No
-----------------------  ------------
ColumnToCollection Type  Yes
-----------------------  ------------
Composite Type           Yes
-----------------------  ------------
CounterColumn Type       No
-----------------------  ------------
Date Type                No
-----------------------  ------------
Decimal Type             No
-----------------------  ------------
Double Type              No
-----------------------  ------------
Duration Type            No 
-----------------------  ------------
DynamicComposite Type    Yes
-----------------------  ------------
Empty Type               No
-----------------------  ------------
Float Type               No
-----------------------  ------------
Frozen Type              Yes
-----------------------  ------------
InetAddress Type         No
-----------------------  ------------
Int32 Type               No
-----------------------  ------------
Integer Type             No
-----------------------  ------------
LexicalUUID Type         No
-----------------------  ------------
List Type                Yes
-----------------------  ------------
Long Type                No
-----------------------  ------------
Map Type                 Yes
-----------------------  ------------
PartitionerDefinedOrder  Yes
-----------------------  ------------
Reversed Type            Yes
-----------------------  ------------
Set Type                 Yes
-----------------------  ------------
Short Type               No
-----------------------  ------------
SimpleDate Type          No
-----------------------  ------------
Timestamp Type           No
-----------------------  ------------
Time Type                No
-----------------------  ------------
TimeUUID Type            No
-----------------------  ------------
Tuple Type               Yes
-----------------------  ------------
User Type                Yes
-----------------------  ------------
UTF8 Type                No
-----------------------  ------------
UUID Type                No
=======================  ============


.. include:: /rst_include/apache-copyrights.rst
