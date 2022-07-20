SSTable Index
=============

.. versionadded:: 4.4 Scylla Open Source

*scylla-sstable-index* is a tool which lists all partitions contained in an SSTable index.
As all partitions in an SSTable are indexed, this tool can be used to find out
what partitions are contained in a given SSTable.

The command syntax is as follows:

.. code-block:: none

   ./scylla-sstable-index [--type=<CassandraType>] <--sstable> [path/to/scylla/datadir]

Where:

* The ``--type`` or ``-t`` flag is used to specify the types making up the partition key of the table to which the index file belongs.
  This is needed so the tool can parse the SSTable keys found in the index. You will need to pass ``--type|-t`` for each type in the partition key.
  For the type names, the Cassandra type class notation has to be used. You can use the short form, i.e. without the org.apache.cassandra.db.marshal. prefix.
  For a complete mapping of CQL types to their respective Cassandra type class notation, see `CQL3 Type Mapping <https://github.com/scylladb/scylla/blob/master/docs/dev/cql3-type-mapping.md>`_.

* The SSTable index file path can be passed both as a positional argument ``[path/to/scylla/datadir]``  or with the  ``--sstable`` flag.

The output has the following format:

``$pos: $human_readable_value (pk{$raw_hex_value})``

Where:

* ``$pos:`` the position of the partition in the (decompressed) data file
* ``$human_readable_value:`` the human readable partition key
* ``$raw_hex_value:`` the raw hexadecimal value of the binary representation of the partition key


Example
-------

If you have the following schema:

.. code-block:: none

    CREATE TABLE my_keyspace.my_table (
        id1 int,
        id2 text,
        value text,
        PRIMARY KEY ((id1, id2))
    );

and you run:

.. code-block:: none

   $ build/dev/tools/scylla-sstable-index --type=Int32Type --type=UTF8Type --sstable /path/to/scylla/datadir/my_keyspace/my_table-1e13d620ea2811ebb808c477e839c4d7/md-1-big-Index.db

the output would be:

.. code-block:: none

   0: 3:third (pk{00040000000300057468697264})
   45: 2:second (pk{00040000000200067365636f6e64})
   91: 1:first (pk{00040000000100056669727374})

.. tip:: Using the tool output you can use the raw Hex values to cross reference the :doc:`Scylla Logs </troubleshooting/log-level>` and find information on your SSTable.