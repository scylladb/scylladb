SSTable2json
============


This tool allows you to converts SSTable into a JSON format file.
SSTable2json supported when using Scylla 2.x or lower version.
In newer versions, the tool is named :doc:`SSTabledump </operating-scylla/admin-tools/sstabledump>`.

.. note:: 

   It is not recommended to use this tool in the production cluster.

Use the full path when executing the command.

For example:

.. code-block:: shell

   sstable2json /var/lib/scylla/data/keyspace1/standard1-7119946056b211e98e85000000000001

Example output:

.. code-block:: shell

  [
  {
    "partition" : {
      "key" : [ "3137343334334f4f4d30" ],
      "position" : 0
    },
    "rows" : [
      {
        "type" : "static_block",
        "position" : 281,
        "cells" : [
          { "name" : "\"C0\"", "value" : "0xb7789e7cdf2af541061f207714a3b8e14c72f74e663bd5c2577ac329bcb3161cf10c", "tstamp" : "2019-04-04T08:22:24.336001Z" },
          { "name" : "\"C1\"", "value" : "0xe8ed77f078a23e37f8a7246ccd8cd4099585c7031e242529e5070246860d7a1b1e85", "tstamp" : "2019-04-04T08:22:24.336001Z" },
          { "name" : "\"C2\"", "value" : "0x3b836d4333d2d5a02a63ced47596bfb5f80ecb8e80686061c3daaba87380994b7b61", "tstamp" : "2019-04-04T08:22:24.336001Z" },
          { "name" : "\"C3\"", "value" : "0x9220219581df87ff131306b8bf793c14ae8ebf8c8af1b638827ebfcab85660a378b8", "tstamp" : "2019-04-04T08:22:24.336001Z" },
          { "name" : "\"C4\"", "value" : "0x5b4c972cdeb330035b82dc0b1daa9051fff7956d45e3c6c2b21dfb1fd2bb43fb1146", "tstamp" : "2019-04-04T08:22:24.336001Z" }
        ]
      }
    ]
