Scylla SStable
==============

.. versionadded:: 5.0

Introduction
-------------

This tool allows you to examine the content of SStables by performing operations such as dumping the content of SStables,
generating a histogram, validating the content of SStables, and more. See `Supported Operations`_ for the list of available operations.

Run ``scylla sstable --help`` for additional information about the tool and the operations.

This tool is similar to SStableDump_, with notable differences:

* Built on the ScyllaDB C++ codebase, it supports all SStable formats and components that ScyllaDB supports.
* Expanded scope: this tool supports much more than dumping SStable data components (see `Supported Operations`_).
* More flexible on how schema is obtained and where SStables are located: SStableDump_ only supports dumping SStables located in their native data directory. To dump an SStable, one has to clone the entire ScyllaDB data directory tree, including system table directories and even config files. ``scylla sstable`` can dump sstables from any path with multiple choices on how to obtain the schema, see Schema_.

Currently, SStableDump_ works better on production systems as it automatically loads the schema from the system tables, unlike ``scylla sstable``, which has to be provided with the schema explicitly. On the other hand ``scylla sstable`` works better for off-line investigations, as it can be used with as little as just a schema definition file and a single sstable. In the future we plan on closing this gap -- adding support for automatic schema-loading for ``scylla sstable`` too -- and completely supplant SStableDump_ with ``scylla sstable``.

.. _SStableDump: /operating-scylla/admin-tools/sstabledump

Usage
------

Syntax
^^^^^^

The command syntax is as follows:

.. code-block:: console

   scylla sstable <operation> <path to SStable>


You can specify more than one SStable.

Schema
^^^^^^
All operations need a schema to interpret the SStables with.
Currently, there are two ways to obtain the schema:

* ``--schema-file FILENAME`` - Read the schema definition from a file.
* ``--system-schema KEYSPACE.TABLE`` - Use the known definition of built-in tables (only works for system tables).

By default, the tool uses the first method: ``--schema-file schema.cql``; i.e. it assumes there is a schema file named ``schema.cql`` in the working directory.
If this fails, it will exit with an error.

The schema file should contain all definitions needed to interpret data belonging to the table.

Example ``schema.cql``:

.. code-block:: cql

    CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'mydc1': 1, 'mydc2': 4};

    CREATE TYPE ks.mytype (
        f1 int,
        f2 text
    );

    CREATE TABLE ks.cf (
        pk int,
        ck text,
        v1 int,
        v2 mytype,
        PRIMARY KEY (pk, ck)
    );

Note:

* In addition to the table itself, the definition also has to includes any user defined types the table uses.
* The keyspace definition is optional, if missing one will be auto-generated.
* The schema file doesn't have to be called ``schema.cql``, this is just the default name. Any file name is supported (with any extension).

Dropped columns
***************

The examined sstable might have columns which were dropped from the schema definition. In this case providing the up-do-date schema will not be enough, the tool will fail when attempting to process a cell for the dropped column.
Dropped columns can be provided to the tool in the form of insert statements into the ``system_schema.dropped_columns`` system table, in the schema definition file. Example:

.. code-block:: cql

    INSERT INTO system_schema.dropped_columns (
        keyspace_name,
        table_name,
        column_name,
        dropped_time,
        type
    ) VALUES (
        'ks',
        'cf',
        'v1',
        1631011979170675,
        'int'
    );

    CREATE TABLE ks.cf (pk int PRIMARY KEY, v2 int);

System tables
*************

If the examined table is a system table -- it belongs to one of the system keyspaces (``system``, ``system_schema``, ``system_distributed`` or ``system_distributed_everywhere``) -- you can just tell the tool to use the known built-in definition of said table. This is possible with the ``--system-schema`` flag. Example:

.. code-block:: console

    scylla sstable dump-data --system-schema system.local ./path/to/md-123456-big-Data.db

Supported Operations
^^^^^^^^^^^^^^^^^^^^^^^
The ``dump-*`` operations output JSON. For ``dump-data``, you can specify another output format.

* ``dump-data`` - Dumps the content of the SStable. You can use it with additional parameters:

   * ``--merge`` - Allows you to process multiple SStables as a unified stream (if not specified, multiple SStables are processed one by one). 
   * ``--partition={{<partition key>}}`` or ``partitions-file={{<partition key>}}`` - Allows you to narrow down the scope of the operation to specified partitions. To specify the partition(s) you want to be processed, provide partition keys in the hexdump format used by ScyllaDB (the hex representation of the raw buffer).
   * ``--output-format=<format>`` - Allows you to specify the output format: ``json`` or ``text``.

* ``dump-index`` - Dumps the content of the SStable index.
* ``dump-compression-info`` - Dumps the SStable compression information, including compression parameters and mappings between 
  compressed and uncompressed data.
* ``dump-summary`` - Dumps the summary of the SStable index.
* ``dump-statistics`` - Dumps the statistics of the SStable, including metadata about the data component.
* ``dump-scylla-metadata`` - Dumps the SStable's scylla-specific metadata.
* ``writetime-histogram`` - Generates a histogram of all the timestamps in the SStable. You can use it with a parameter:

   * ``--bucket=<unit>`` - Allows you to specify the unit of time to be used as bucket (years, months, weeks, days, or hours).

* ``validate`` - Validates the content of the SStable with the mutation fragment stream validator.
* ``validate-checksums`` - Validates SStable checksums (full checksum and per-chunk checksum) against the SStable data.
* ``decompress`` - Decompresses the data component of the SStable (the ``*-Data.db`` file) if compressed. The decompressed data is written to a ``*-Data.decompressed`` file.

Examples
^^^^^^^^
Dumping the content of the SStable:

.. code-block:: console

   scylla sstable dump-data /path/to/md-123456-big-Data.db

Dumping the content of two SStables as a unified stream:

.. code-block:: console

   scylla sstable dump-data --merge /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db


Validating the specified SStables:

.. code-block:: console

   scylla sstable validate /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db
