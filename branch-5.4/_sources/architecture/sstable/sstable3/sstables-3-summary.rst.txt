SSTables 3.0 Summary File Format
================================

SSTables summary file contains samples of keys that are used for the first, coarse-grained, stage of search through the index. Every summary entry points to a sequence of index entries commonly referred to as an index page where the key looked for is located. So with the summary, it is possible to only read and search through a small part of the index.

The summary file is meant to be read and stored entirely in memory, therefore it is aimed to be reasonably small. This incurs some trade-off between the size of the summary and the cardinality of an index page (number of index entries covered by it). The less the cardinality is, the better precision the initial lookup through summary gives, but this also tends to increase the size of the summary. The balance between the two is regulated by the sampling level which rules how many index entries should one summary entry cover.

SSTables Summary File Layout
............................

The summary file format in SSTables 3.0 has minimal changes compared to the previous version of the format described here: :doc:`SSTable 2.0 format </architecture/sstable/sstable2/sstable-format/>`
The only noticeable change is that in 3.0 the summary file does not store information about segment boundaries. See `CASSANDRA-8630`_ for more details about this change.

.. _`CASSANDRA-8630`: https://issues.apache.org/jira/browse/CASSANDRA-8630

Other than that, the data format remains the same. Note that, unlike data and index files, the summary file does not make use of variable-length integers:

.. code:: cpp

   struct summary {
       struct summary_header header;
       struct summary_entries_block summary_entries;
       struct serialized_key first;
       struct serialized_key last;
   };


Summary Header
--------------

First goes the summary `header` which is laid out as follows:
   
.. code:: cpp
   
   struct summary_header {
       be32 min_index_interval;
       be32 entries_count;
       be64 summary_entries_size;
       be32 sampling_level;
       be32 size_at_full_sampling;
   };


The ``min_index_interval`` is a lower bound for the average number of partitions in between each index summary entry. A lower value means that more partitions will have an entry in the index summary when at the full sampling level.

The ``entries_count`` is the number of `offsets` and ``entries`` in the ``summary_entries`` structure (see below).

The ``summary_entries_size`` is the full size of the ``summary_entries`` structure (see below).

The ``sampling_level`` is a value between 1 and ``BASE_SAMPLING_LEVEL`` (which equals to 128)  that represents how many of the original index summary entries ``((1 / indexInterval) * numKeys)`` have been retained. Thus, this summary contains ``(samplingLevel / BASE_SAMPLING_LEVEL) * ((1 / indexInterval) * numKeys))`` entries.

The ``size_at_full_sampling`` is the number of entries the Summary **would** have if the sampling level would be equal to ``min_index_interval``.

Summary Entries
---------------

.. code:: cpp

   struct summary_entries_block {
       uint32 offsets[header.entries_count];
       struct summary_entry entries[header.entries_count];
   };


The ``offsets`` array contains offsets of corresponding entries in the ``entries`` array below. The offsets are taken from the beginning of the ``summary_entries_block`` so ``offsets[0] == sizeof(uint32) * header.entries_count`` as the first entry begins right after the array of offsets.

Note that ``offsets`` are written in the native order format although typically all the integers in SSTables files are written in big-endian. In Scylla, they are always written in little-endian order to allow interoperability with 1. Summary files written by Cassandra on the more common little-endian machines, and 2. Summary files written by Scylla on the rarer big-endian machines.

Here is how a summary entry looks:

.. code:: cpp

   struct summary_entry {
       byte key[]; // variable-length.
       be64 position;
   };


Every ``summary_entry`` holds a `key` and a ``position`` of the index entry with that key in the index file.
Note that ``summary_entry`` does not store key length but it can be deduced from the offsets. The length of the last ``summary_entry`` in the `entries` array can be calculated using its offset and ``header.summary_entries_size``.

Keys
----

The last two entries in the ``summary`` structure are  ``serialized_keys`` that look like:

.. code:: cpp

   struct serialized_key {
       be32 size;
       byte key[size];
   };


They store the first and the last keys in the index/data file.

References

* `IndexSummary.java`_ 

* `SSTableReader.java, saveSummary`_ 

.. _`IndexSummary.java`: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/io/sstable/IndexSummary.java

.. _`SSTableReader.java, saveSummary`: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java


.. include:: /rst_include/apache-copyrights.rst
