Nodetool sstableinfo
====================

**sstableinfo** ``[<keyspace> <table>...]`` - Gets information about SStables per keyspace/table.

Syntax
------

.. code-block:: console

   nodetool sstableinfo [<keyspace> <table>...]

Examples
--------

For example:

.. code-block:: shell

   nodetool sstableinfo

Example output:

.. code-block:: shell

   keyspace : ks
      table : tbl
   sstables :
          0 :
                 data size : 90
               filter size : 332
                index size : 24
                     level : 0
                      size : 5746
                generation : 3gec_0mu7_5az0024x96bfm476r6
                   version : me
                 timestamp : 2024-03-11T08:13:19Z
       extended properties :
                compression_parameters :
                           sstable_compression : org.apache.cassandra.io.compress.LZ4Compressor
          1 :
                 data size : 290
               filter size : 232
                index size : 124
                     level : 0
                      size : 6746
                generation : 3gec_0mu7_6bz0024x96bfm476r6
                   version : me
                 timestamp : 2024-03-10T08:13:19Z
                properties :
                foo : bar
   keyspace : ks
      table : tbl2
   sstables :
          0 :
                 data size : 44
               filter size : 172
                index size : 8
                     level : 0
                      size : 5481
                generation : 3gec_0mu8_5vrgh24x96bfm476r6
                   version : me
                 timestamp : 2024-03-11T08:13:20Z
       extended properties :
                compression_parameters :
                           sstable_compression : org.apache.cassandra.io.compress.LZ4Compressor
   keyspace : ks2
      table : tbl

See Also
--------

.. include:: nodetool-index.rst
