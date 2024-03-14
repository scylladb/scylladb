Scylla SStable
==============

Introduction
-------------

This tool allows you to examine the content of SStables by performing operations such as dumping the content of SStables,
generating a histogram, validating the content of SStables, and more. See `Supported Operations`_ for the list of available operations.

Run ``scylla sstable --help`` for additional information about the tool and the operations.

This tool is similar to SStableDump_, with notable differences:

* Built on the ScyllaDB C++ codebase, it supports all SStable formats and components that ScyllaDB supports.
* Expanded scope: this tool supports much more than dumping SStable data components (see `Supported Operations`_).
* More flexible on how schema is obtained and where SStables are located: SStableDump_ only supports dumping SStables located in their native data directory. To dump an SStable, one has to clone the entire ScyllaDB data directory tree, including system table directories and even config files. ``scylla sstable`` can dump sstables from any path with multiple choices on how to obtain the schema, see Schema_.

``scylla sstable`` was developed to supplant SStableDump_ as ScyllaDB-native tool, better tailored for the needs of ScyllaDB.

.. _SStableDump: /operating-scylla/admin-tools/sstabledump

Usage
------

The command syntax is as follows:

.. code-block:: console

   scylla sstable <operation> <path to SStable>


You can specify more than one SStable.

Schema
------

All operations need a schema to interpret the SStables with.
This tool tries to auto-detect the location of the ScyllaDB data directories and the name of the table the SStable belongs to.
If the SStable is located in a ScyllaDB data directory, it works out-of-the-box, without any additional input from the user.
If the SStable is located at an external path, you need to specify the names of the keyspace and table to which the SStable belongs. In addition, some hints as to where the ScyllaDB data directory is located may also be required.

The schema can be obtained in the following ways:

* Auto-detected - If the SStable is located in the table's directory within the ScyllaDB data directory.
* ``--keyspace=KEYSPACE --table=TABLE`` - If the SStable is located at an external location, but the ScyllaDB data directory or the config file are located at the standard location. The tool also reads the ``SCYLLA_CONF`` and ``SCYLLA_HOME`` environment variables to try to locate the configuration file.
* ``--schema-file FILENAME`` - Read the schema definition from a file.
* ``--system-schema --keyspace=KEYSPACE --table=TABLE`` - Use the known definition of built-in tables (only works for system tables).
* ``--scylla-data-dir SCYLLA_DATA_DIR_PATH --keyspace=KEYSPACE --table=TABLE`` - Read the schema tables from the data directory at the provided location, needs the keyspace and table name to be provided with ``--keyspace`` and ``--table``.
* ``--scylla-yaml-file SCYLLA_YAML_FILE_PATH --keyspace=KEYSPACE --table=TABLE`` - Read the schema tables from the data directory path obtained from the configuration, needs the keyspace and table name to be provided with ``--keyspace`` and ``--table``.

By default (no schema-related options are provided), the tool will try the following sequence:

* Try to load schema from ``schema.cql``.
* Try to deduce the ScyllaDB data directory path and table names from the SStable path.
* Try to load the schema from the ScyllaDB data directory path, obtained from the configuration file, at the standard location (``./conf/scylla.yaml``).
  ``SCYLLA_CONF`` and ``SCYLLA_HOME`` environment variables are also checked.
  If the configuration file cannot be located, the default ScyllaDB data directory path (``/var/lib/scylla``) is used.
  For this to succeed, the table name has to be provided via ``--keyspace`` and ``--table``.

The tool stops after the first successful attempt. If none of the above succeed, an error message will be printed.
A user provided schema in ``schema.cql`` (if present) always takes precedence over other methods. This is deliberate, to allow to manually override the schema to be used.

schema.cql
^^^^^^^^^^

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
~~~~~~~~~~~~~~~

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
^^^^^^^^^^^^^

If the examined table is a system table -- it belongs to one of the system keyspaces (``system``, ``system_schema``, ``system_distributed`` or ``system_distributed_everywhere``) -- you can just tell the tool to use the known built-in definition of said table. This is possible with the ``--system-schema`` flag. Example:

.. code-block:: console

    scylla sstable dump-data --system-schema system.local ./path/to/md-123456-big-Data.db

.. _scylla-sstable-sstable-content:

SStable Content
---------------

.. _SStable: /architecture/sstable

All operations target either one specific sstable component or all of them as a whole.
For more information about the sstable components and the format itself, visit SStable_.

On a conceptual level, the data in SStables is represented by objects called mutation fragments. There are the following kinds of fragments:

* ``partition-start`` (1) - represents the start of a partition, contains the partition key and partition tombstone (if any);
* ``static-row`` (0-1) - contains the static columns if the schema (and the partition) has any;
* ``clustering-row`` (0-N) - contains the regular columns for a given clustering row; if there are no clustering columns, a partition will have exactly one of these;
* ``range-tombstone-change`` (0-N) - contains a (either start or end) bound of a range deletion;
* ``partition-end`` (1) - represents the end of the partition;

Numbers in parentheses represent the number of the fragment type in a partition.

Data from the sstable is parsed into these fragments.
This format allows you to represent a small part of a partition or an arbitrary number of partitions, even the entire content of an SStable.
The ``partition-start`` and ``partition-end`` fragments are always present, even if a single row is read from a partition.
If the stream contains multiple partitions, these follow each other in the stream, the ``partition-start`` fragment of the next partition following the ``partition-end`` fragment of the previous one.
The stream is strictly ordered:

* Partitions are ordered according to their token (hashes);
* Fragments in the partition are ordered according to their order presented in the listing above, ``clustering-row`` and ``range-tombstone-change`` fragments can be intermingled, see below.
* Clustering fragments (``clustering-row`` and ``range-tombstone-change``) are ordered between themselves according to the clustering order defined by the schema.

Supported Operations
--------------------

.. _scylla-sstable-dump-data-operation:

dump-data
^^^^^^^^^

Dumps the content of the data component (the component that contains the data-proper
of the SStable). This operation might produce a huge amount of output. In general, the
human-readable output will be larger than the binary file.

It is possible to filter the data to print via the ``--partitions`` or
``--partitions-file`` options. Both expect partition key values in the hexdump
format.

Supports both a text and JSON output. The text output uses the built-in Scylla
printers, which are also used when logging mutation-related data structures.

The schema of the JSON output is the following:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := $NON_MERGED_ROOT | $MERGED_ROOT

    $NON_MERGED_ROOT := { "$sstable_path": $SSTABLE, ... } // without --merge

    $MERGED_ROOT := { "anonymous": $SSTABLE } // with --merge

    $SSTABLE := [$PARTITION, ...]

    $PARTITION := {
        "key": {
            "token": String,
            "raw": String, // hexadecimal representation of the raw binary
            "value": String
        },
        "tombstone: $TOMBSTONE, // optional
        "static_row": $COLUMNS, // optional
        "clustering_elements": [
            $CLUSTERING_ROW | $RANGE_TOMBSTONE_CHANGE,
            ...
        ]
    }

    $TOMBSTONE := {
        "timestamp": Int64,
        "deletion_time": String // YYYY-MM-DD HH:MM:SS
    }

    $COLUMNS := {
        "$column_name": $REGULAR_CELL | $COUNTER_SHARDS_CELL | $COUNTER_UPDATE_CELL | $FROZEN_COLLECTION | $COLLECTION,
        ...
    }

    $REGULAR_CELL := {
        "is_live": Bool, // is the cell live or not
        "type": "regular",
        "timestamp": Int64,
        "ttl": String, // gc_clock::duration - optional
        "expiry": String, // YYYY-MM-DD HH:MM:SS - optional
        "value": String // only if is_live == true
    }

    $COUNTER_SHARDS_CELL := {
        "is_live": true,
        "type": "counter-shards",
        "timestamp": Int64,
        "value": [$COUNTER_SHARD, ...]
    }

    $COUNTER_SHARD := {
        "id": String, // UUID
        "value": Int64,
        "clock": Int64
    }

    $COUNTER_UPDATE_CELL := {
        "is_live": true,
        "type": "counter-update",
        "timestamp": Int64,
        "value": Int64
    }

    $FROZEN_COLLECTION is the same as a $REGULAR_CELL, with type = "frozen-collection".

    $COLLECTION := {
        "type": "collection",
        "tombstone": $TOMBSTONE, // optional
        "cells": [
            {
                "key": String,
                "value": $REGULAR_CELL
            },
            ...
        ]
    }

    $CLUSTERING_ROW := {
        "type": "clustering-row",
        "key": {
            "raw": String, // hexadecimal representation of the raw binary
            "value": String
        },
        "tombstone": $TOMBSTONE, // optional
        "shadowable_tombstone": $TOMBSTONE, // optional
        "marker": { // optional
            "timestamp": Int64,
            "ttl": String, // gc_clock::duration
            "expiry": String // YYYY-MM-DD HH:MM:SS
        },
        "columns": $COLUMNS
    }

    $RANGE_TOMBSTONE_CHANGE := {
        "type": "range-tombstone-change",
        "key": { // optional
            "raw": String, // hexadecimal representation of the raw binary
            "value": String
        },
        "weight": Int, // -1 or 1
        "tombstone": $TOMBSTONE
    }

dump-index
^^^^^^^^^^

Dumps the content of the index component. It the partition-index of the data
component, which is effectively a list of all the partitions in the SStable, with
their starting position in the data component and, optionally, a promoted index.
The promoted index contains a sampled index of the clustering rows in the partition.
Positions (both that of partition and that of rows) are valid for uncompressed
data.

The content is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": $SSTABLE, ... }

    $SSTABLE := [$INDEX_ENTRY, ...]

    $INDEX_ENTRY := {
        "key": {
            "raw": String, // hexadecimal representation of the raw binary
            "value": String
        },
        "pos": Uint64
    }

dump-compression-info
^^^^^^^^^^^^^^^^^^^^^

Dumps the content of the compression-info component. It contains compression
parameters and maps positions into the uncompressed data to that into compressed
data. Note that compression happens over chunks with configurable size, so to
get data at a position in the middle of a compressed chunk, the entire chunk has
to be decompressed.

The content is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": $SSTABLE, ... }

    $SSTABLE := {
        "name": String,
        "options": {
            "$option_name": String,
            ...
        },
        "chunk_len": Uint,
        "data_len": Uint64,
        "offsets": [Uint64, ...]
    }

.. _scylla sstable dump-summary:

dump-summary
^^^^^^^^^^^^

Dumps the content of the summary component. The summary is a sampled index of the
content of the index-component (an index of the index). The sampling rate is chosen
such that this file is small enough to be kept in memory even for very large
SStables.

The content is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": $SSTABLE, ... }

    $SSTABLE := {
        "header": {
            "min_index_interval": Uint64,
            "size": Uint64,
            "memory_size": Uint64,
            "sampling_level": Uint64,
            "size_at_full_sampling": Uint64
        },
        "positions": [Uint64, ...],
        "entries": [$SUMMARY_ENTRY, ...],
        "first_key": $KEY,
        "last_key": $KEY
    }

    $SUMMARY_ENTRY := {
        "key": $DECORATED_KEY,
        "position": Uint64
    }

    $DECORATED_KEY := {
        "token": String,
        "raw": String, // hexadecimal representation of the raw binary
        "value": String
    }

    $KEY := {
        "raw": String, // hexadecimal representation of the raw binary
        "value": String
    }

.. _scylla sstable dump-statistics:

dump-statistics
^^^^^^^^^^^^^^^

Dumps the content of the statistics component. It contains various metadata about the
data component. In the SStable 3 format, this component is critical for parsing
the data component.

The content is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": $SSTABLE, ... }

    $SSTABLE := {
        "offsets": {
            "$metadata": Uint,
            ...
        },
        "validation": $VALIDATION_METADATA,
        "compaction": $COMPACTION_METADATA,
        "stats": $STATS_METADATA,
        "serialization_header": $SERIALIZATION_HEADER // >= MC only
    }

    $VALIDATION_METADATA := {
        "partitioner": String,
        "filter_chance": Double
    }

    $COMPACTION_METADATA := {
        "ancestors": [Uint, ...], // < MC only
        "cardinality": [Uint, ...]
    }

    $STATS_METADATA := {
        "estimated_partition_size": $ESTIMATED_HISTOGRAM,
        "estimated_cells_count": $ESTIMATED_HISTOGRAM,
        "position": $REPLAY_POSITION,
        "min_timestamp": Int64,
        "max_timestamp": Int64,
        "min_local_deletion_time": Int64, // >= MC only
        "max_local_deletion_time": Int64,
        "min_ttl": Int64, // >= MC only
        "max_ttl": Int64, // >= MC only
        "compression_ratio": Double,
        "estimated_tombstone_drop_time": $STREAMING_HISTOGRAM,
        "sstable_level": Uint,
        "repaired_at": Uint64,
        "min_column_names": [Uint, ...],
        "max_column_names": [Uint, ...],
        "has_legacy_counter_shards": Bool,
        "columns_count": Int64, // >= MC only
        "rows_count": Int64, // >= MC only
        "commitlog_lower_bound": $REPLAY_POSITION, // >= MC only
        "commitlog_intervals": [$COMMITLOG_INTERVAL, ...] // >= MC only
    }

    $ESTIMATED_HISTOGRAM := [$ESTIMATED_HISTOGRAM_BUCKET, ...]

    $ESTIMATED_HISTOGRAM_BUCKET := {
        "offset": Int64,
        "value": Int64
    }

    $STREAMING_HISTOGRAM := {
        "$key": Uint64,
        ...
    }

    $REPLAY_POSITION := {
        "id": Uint64,
        "pos": Uint
    }

    $COMMITLOG_INTERVAL := {
        "start": $REPLAY_POSITION,
        "end": $REPLAY_POSITION
    }

    $SERIALIZATION_HEADER_METADATA := {
        "min_timestamp_base": Uint64,
        "min_local_deletion_time_base": Uint64,
        "min_ttl_base": Uint64",
        "pk_type_name": String,
        "clustering_key_types_names": [String, ...],
        "static_columns": [$COLUMN_DESC, ...],
        "regular_columns": [$COLUMN_DESC, ...],
    }

    $COLUMN_DESC := {
        "name": String,
        "type_name": String
    }

dump-scylla-metadata
^^^^^^^^^^^^^^^^^^^^

Dumps the content of the scylla-metadata component. Contains Scylla-specific
metadata about the data component. This component won't be present in SStables
produced by Apache Cassandra.

The content is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": $SSTABLE, ... }

    $SSTABLE := {
        "sharding": [$SHARDING_METADATA, ...],
        "features": $FEATURES_METADATA,
        "extension_attributes": { "$key": String, ...}
        "run_identifier": String, // UUID
        "large_data_stats": {"$key": $LARGE_DATA_STATS_METADATA, ...}
        "sstable_origin": String
    }

    $SHARDING_METADATA := {
        "left": {
            "exclusive": Bool,
            "token": String
        },
        "right": {
            "exclusive": Bool,
            "token": String
        }
    }

    $FEATURES_METADATA := {
        "mask": Uint64,
        "features": [String, ...]
    }

    $LARGE_DATA_STATS_METADATA := {
        "max_value": Uint64,
        "threshold": Uint64,
        "above_threshold": Uint
    }

.. _scylla-sstable-validate-operation:

validate
^^^^^^^^

Validates the content of the sstable on the mutation-fragment level, see `sstable content <scylla-sstable-sstable-content_>`_ for more details.
Any parsing errors will also be detected, but after successful parsing the validation will happen on the fragment level.
The following things are validated:

* Partitions are ordered in strictly monotonic ascending order.
* Fragments are correctly ordered.
* Clustering elements are ordered according in a strictly increasing clustering order as defined by the schema.
* All range deletions are properly terminated with a corresponding end bound.
* The stream ends with a partition-end fragment.

Any errors found will be logged with error level to ``stderr``.

The validation result is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": $RESULT }

    $RESULT := {
        "errors": Uint64,
        "valid": Bool,
    }

scrub
^^^^^

Rewrites the SStable, skipping or fixing corrupt parts. Not all kinds of corruption can be skipped or fixed by scrub.
It is limited to ordering issues on the partition, row, or mutation-fragment level. See `sstable content <scylla-sstable-sstable-content_>`_ for more details.

Scrub has several modes:

* **abort** - Aborts the scrub as soon as any error is found (recognized or not). This mode is only included for the sake of completeness. We recommend using the **validate** mode so that all errors are reported.
* **skip** - Skips over any corruptions found, thus omitting them from the output. Note that this mode can result in omitting more than is strictly necessary, but it guarantees that all detectable corruptions will be omitted.
* **segregate** - Fixes partition/row/mutation-fragment out-of-order errors by segregating the output into as many SStables as required so that the content of each output SStable is properly ordered.
* **validate** - Validates the content of the SStable, reporting any corruptions found. Writes no output SStables. In this mode, scrub has the same outcome as the `validate operation <scylla-sstable-validate-operation_>`_ - and the validate operation is recommended over scrub.

Output SStables are written to the directory specified via ``--output-directory``. They will be written with the ``BIG`` format and the highest supported SStable format, with generations chosen by scylla-sstable. Generations are chosen such
that they are unique among the SStables written by the current scrub.

The output directory must be empty; otherwise, scylla-sstable will abort scrub. You can allow writing to a non-empty directory by setting the ``--unsafe-accept-nonempty-output-dir`` command line flag.
Note that scrub will be aborted if an SStable cannot be written because its generation clashes with a pre-existing SStable in the output directory.

validate-checksums
^^^^^^^^^^^^^^^^^^

There are two kinds of checksums for SStable data files:

* The digest (full checksum), stored in the ``Digest.crc32`` file. It is calculated over the entire content of ``Data.db``.
* The per-chunk checksum. For uncompressed SStables, it is stored in ``CRC.db``; for compressed SStables, it is stored inline after each compressed chunk in ``Data.db``.

During normal reads, ScyllaDB validates the per-chunk checksum for compressed SStables.
The digest and the per-chunk checksum of uncompressed SStables are currently not checked on any code paths.

This operation reads the entire ``Data.db`` and validates both kinds of checksums against the data.
Errors found are logged to stderr. The output contains a bool for each SStable that is true if the SStable matches all checksums.

The content is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": Bool, ... }

decompress
^^^^^^^^^^

Decompress Data.db if compressed (no-op if not compressed). The decompressed data is written to Data.db.decompressed.
For example, for the SStable:

.. code-block:: console
    :class: hide-copy-button

    md-12311-big-Data.db

the output will be:

.. code-block:: console
    :class: hide-copy-button

    md-12311-big-Data.db.decompressed

write
^^^^^

Writes an SStable based on a JSON representation of the content.
The JSON representation has to have the same schema as that of a single SStable from the output of the `dump-data operation <dump-data_>`_ (corresponding to the ``$SSTABLE`` symbol).
The easiest way to get started with writing your own SStable is to dump an existing SStable, modify the JSON then invoke this operation with the result.
You can feed the output of dump-data to write by filtering the output of the former with ``jq .sstables[]``:

.. code-block:: console

    scylla sstable dump-data --system-schema system_schema.columns /path/to/me-14-big-Data.db | jq .sstables[] > input.json
    scylla sstable write --system-schema system_schema.columns --input-file ./input.json --generation 0
    scylla sstable dump-data --system-schema system_schema.columns ./me-0-big-Data.db | jq .sstables[] > dump.json

At the end of the above, ``input.json`` and ``dump.json`` will have the same content.

Note that `write` doesn't yet support all the features of the ScyllaDB storage engine. The following are not supported:

* Counters.
* Non-strictly atomic cells, including frozen multi-cell types like collections, tuples, and UDTs.

Parsing uses a streaming JSON parser, it is safe to pass in input files of any size.

The output SStable will use the BIG format, the highest supported SStable format, and the specified generation (``--generation``).
By default, it is placed in the local directory, which can be changed with ``--output-dir``.
If the output SStable clashes with an existing SStable, the write will fail.

The output is validated before being written to the disk.
The validation done here is similar to that done by the `validate operation <validate_>`_.
The level of validation can be changed with the ``--validation-level`` flag.
Possible validation-levels are:

* ``partition_region`` - Only checks fragment types, e.g., that a partition-end is followed by partition-start or EOS.
* ``token`` - In addition, checks the token order of partitions.
* ``partition_key`` - Full check on partition ordering.
* ``clustering_key`` - In addition, checks clustering element ordering.

Note that levels are cumulative - each contains all the checks of the previous levels, too.
By default, the strictest level is used.
This can be relaxed, for example, if you want to produce intentionally corrupt SStables for tests.

shard-of
^^^^^^^^

Print out the shards which own the specified SSTables.

The content is dumped in JSON, using the following schema:

.. code-block:: none
    :class: hide-copy-button

    $ROOT := { "$sstable_path": $SHARD_IDS, ... }

    $SHARD_IDS := [$SHARD_ID, ...]

    $SHARD_ID := Uint

script
^^^^^^

Reads the SStable(s) and passes the resulting `fragment stream <scylla-sstable-sstable-content_>`_ to the script specified by `--script-file`.
Currently, only scripts written in `Lua <http://www.lua.org/>`_ are supported.
It is possible to pass command line arguments to the script, via the ``--script-arg`` command line flag.
The format of this argument is the following:

.. code-block:: none
    :class: hide-copy-button

    --script-arg $key1=$value1:$key2=$value2

Alternatively, you can provide each key-value pair via a separate ``--script-arg``:

.. code-block:: none
    :class: hide-copy-button

    --script-arg $key1=$value1 --script-arg $key2=$value2

Command line arguments will be received by the `consume_stream_start() <scylla-consume-stream-start-method_>`_ API method.

.. _scylla-consume-api:

ScyllaDB Consume API
~~~~~~~~~~~~~~~~~~~~~~

These methods represent the glue code between scylla-sstable's C++ code and the Lua script.
Conceptually a script is an implementation of a consumer interface. The script has to implement only the methods it is interested in. Each method has a default implementation in the interface, which simply drops the respective `mutation fragment <scylla-sstable-sstable-content_>`_.
For example, a script only interested in partitions can define only `consume_partition_start() <scylla-consume-partition-start-method_>`_ and nothing else.
Therefore a completely empty script is also valid, although not very useful.
Below you will find the listing of the API methods.
These methods (if provided by the script) will be called by the scylla-sstable runtime for the appropriate events and fragment types.

.. _scylla-consume-stream-start-method:

consume_stream_start(args)
""""""""""""""""""""""""""

* Part of the Consume API. Called on the very start of the stream.
* Parameter is a Lua table containing command line arguments for the script, passed via ``--script-arg``.
* Can be used to initialize global state.

.. _scylla-consume-sstable-start-method:

consume_sstable_start(sst)
""""""""""""""""""""""""""

* Part of the Consume API.
* Called on the start of each stable. 
* The parameter is of type `Scylla.sstable <scylla-sstable-type_>`_. 
* When SStables are merged (``--merge``), the parameter is ``nil``.

Returns whether to stop. If ``true``, `consume_sstable_end() <scylla-consume-sstable-end-method_>`_ is called, skipping the content of the sstable (or that of the entire stream if ``--merge`` is used). If ``false``, consumption follows with the content of the sstable.

.. _scylla-consume-partition-start-method:

consume_partition_start(ps)
"""""""""""""""""""""""""""

* Part of the Consume API. Called on the start of each partition. 
* The parameter is of type `Scylla.partition_start <scylla-partition-start-type_>`_.
* Returns whether to stop. If ``true``, `consume_partition_end() <scylla-consume-partition-end-method_>`_ is called, skipping the content of the partition. If ``false``, consumption follows with the content of the partition.

consume_static_row(sr)
""""""""""""""""""""""

* Part of the Consume API. 
* Called if the partition has a static row. 
* The parameter is of type `Scylla.static_row <scylla-static-row-type_>`_.
* Returns whether to stop. If ``true``, `consume_partition_end() <scylla-consume-partition-end-method_>`_ is called, and the remaining content of the partition is skipped. If ``false``, consumption follows with the remaining content of the partition.

consume_clustering_row(cr)
""""""""""""""""""""""""""

* Part of the Consume API. 
* Called for each clustering row. 
* The parameter is of type `Scylla.clustering_row <scylla-clustering-row-type_>`_.
* Returns whether to stop. If ``true``, `consume_partition_end() <scylla-consume-partition-end-method_>`_ is called, the remaining content of the partition is skipped. If ``false``, consumption follows with the remaining content of the partition.

consume_range_tombstone_change(crt)
"""""""""""""""""""""""""""""""""""

* Part of the Consume API.
* Called for each range tombstone change. 
* The parameter is of type `Scylla.range_tombstone_change <scylla-range-tombstone-change-type_>`_.
* Returns whether to stop. If ``true``, `consume_partition_end() <scylla-consume-partition-end-method_>`_ is called, the remaining content of the partition is skipped. If ``false``, consumption follows with the remaining content of the partition.

.. _scylla-consume-partition-end-method:

consume_partition_end()
"""""""""""""""""""""""

* Part of the Consume API.
* Called at the end of the partition.
* Returns whether to stop. If ``true``, `consume_sstable_end() <scylla-consume-sstable-end-method_>`_ is called,  the remaining content of the SStable is skipped. If ``false``, consumption follows with the remaining content of the SStable.

.. _scylla-consume-sstable-end-method:

consume_sstable_end()
"""""""""""""""""""""

* Part of the Consume API.
* Called at the end of the SStable.
* Returns whether to stop. If true, `consume_stream_end() <scylla-consume-stream-end-method_>`_ is called, the remaining content of the stream is skipped. If false, consumption follows with the remaining content of the stream.

.. _scylla-consume-stream-end-method:

consume_stream_end()
""""""""""""""""""""

* Part of the Consume API. 
* Called at the very end of the stream.

Scylla LUA API
~~~~~~~~~~~~~~

In addition to the `ScyllaDB Consume API <scylla-consume-api_>`_, the Lua bindings expose various types and methods that allow you to work with ScyllaDB types and values.
The listing uses the following terminology:

* Attribute - a simple attribute accessible via ``obj.attribute_name``;
* Method - a method operating on an instance of said type, invocable as ``obj:method()``;
* Magic method - magic methods defined in the metatable which define behaviour of these objects w.r.t. `Lua operators and more <http://www.lua.org/manual/5.4/manual.html#2.4>`_;

The format of an attribute description is the following:

.. code-block:: none
    :class: hide-copy-button

    attribute_name (type) - description

and that of a method:

.. code-block:: none
    :class: hide-copy-button

    method_name(arg1_type, arg2_type...) (return_type) - description

Magic methods have their signature defined by Lua and so that is not described here (these methods are not used directly anyway).

.. _scylla-atomic-cell-type:

Scylla.atomic_cell
""""""""""""""""""

Attributes:

* timestamp (integer)
* is_live (boolean) - is the cell live?
* type (string) - one of: ``regular``, ``counter-update``, ``counter-shards``, ``frozen-collection`` or ``collection``.
* has_ttl (boolean) - is the cell expiring?
* ttl (integer) - time to live in seconds, ``nil`` if cell is not expiring.
* expiry (`Scylla.gc_clock_time_point <scylla-gc-clock-time-point-type_>`_) - time at which cell expires, ``nil`` if cell is not expiring.
* deletion_time (`Scylla.gc_clock_time_point <scylla-gc-clock-time-point-type_>`_) - time at which cell was deleted, ``nil`` unless cell is dead or expiring.
* value:

    - ``nil`` if cell is dead.
    - appropriate Lua native type if type == ``regular``.
    - integer if type == ``counter-update``.
    - `Scylla.counter_shards_value <scylla-counter-shards-value-type_>`_ if type == ``counter-shards``.

A counter-shard table has the following keys:

* id (string)
* value (integer)
* clock (integer)

.. _scylla-clustering-key-type:

Scylla.clustering_key
"""""""""""""""""""""

Attributes:

* components (table) - the column values (`Scylla.data_value <scylla-data-value-type_>`_) making up the composite clustering key.

Methods:

* to_hex - convert the key to its serialized format, encoded in hex.

Magic methods:

* __tostring - can be converted to string with tostring(), uses the built-in operator<< in Scylla.

.. _scylla-clustering-row-type:

Scylla.clustering_row
"""""""""""""""""""""

Attributes:

* key ($TYPE) - the clustering key's value as the appropriate Lua native type.
* tombstone (`Scylla.tombstone <scylla-tombstone-type_>`_) - row tombstone, ``nil`` if no tombstone.
* shadowable_tombstone (`Scylla.tombstone <scylla-tombstone-type_>`_) - shadowable tombstone of the row tombstone, ``nil`` if no tombstone.
* marker (`Scylla.row_marker <scylla-row-marker-type_>`_) - the row marker, ``nil`` if row doesn't have one.
* cells (table) - table of cells, where keys are the column names and the values are either of type `Scylla.atomic_cell <scylla-atomic-cell-type_>`_ or `Scylla.collection <scylla-collection-type_>`_.

See also:

* `Scylla.unserialize_clustering_key() <scylla-unserialize-clustering-key-method_>`_.

.. _scylla-collection-type:

Scylla.collection
"""""""""""""""""

Attributes:

* type (string) - always ``collection`` for collection.
* tombstone (`Scylla.tombstone <scylla-tombstone-type_>`_) - ``nil`` if no tombstone.
* cells (table) - the collection cells, each collection cell is a table, with a ``key`` and ``value`` attribute. The key entry is the key of the collection cell for actual collections (list, set and map) and is of type `Scylla.data-value <scylla-data-value-type_>`_. For tuples and UDT this is just an empty string. The value entry is the value of the collection cell and is of type `Scylla.atomic-cell <scylla-atomic-cell-type_>`_. 

.. _scylla-collection-cell-value-type:

Scylla.collection_cell_value
""""""""""""""""""""""""""""

Attributes:

* key (sstring) - collection cell key in human readable form.
* value (`Scylla.atomic_cell <scylla-atomic-cell-type_>`_) - collection cell value.

.. _scylla-column-definition-type:

Scylla.column_definition
""""""""""""""""""""""""

Attributes:

* id (integer) - the id of the column.
* name (string) - the name of the column.
* kind (string) - the kind of the column, one of ``partition_key``, ``clustering_key``, ``static_column`` or ``regular_column``.

.. _scylla-counter-shards-value-type:

Scylla.counter_shards_value
"""""""""""""""""""""""""""

Attributes:

* value (integer) - the total value of the counter (the sum of all the shards).
* shards (table) - the shards making up this counter, a lua list containing tables, representing shards, with the following key/values:

    - id (string) - the shard's id (UUID).
    - value (integer) - the shard's value.
    - clock (integer) - the shard's logical clock.

Magic methods:

* __tostring - can be converted to string with tostring().

.. _scylla-data-value-type:

Scylla.data_value
"""""""""""""""""

Attributes:

* value - the value represented as the appropriate Lua type

Magic methods:

* __tostring - can be converted to string with tostring().

.. _scylla-gc-clock-time-point-type:

Scylla.gc_clock_time_point
""""""""""""""""""""""""""

A time point belonging to the gc_clock, in UTC.

Attributes:

* year (integer) - [1900, +inf).
* month (integer) - [1, 12].
* day (integer) - [1, 31].
* hour (integer) - [0, 23].
* min (integer) - [0, 59].
* sec (integer) - [0, 59].

Magic methods:

* __eq - can be equal compared.
* __lt - can be less compared.
* __le - can be less-or-equal compared.
* __tostring - can be converted to string with tostring().

See also:

* `Scylla.now() <scylla-now-method_>`_.
* `Scylla.time_point_from_string() <scylla-time-point-from-string-method_>`_.

.. _scylla-json-writer-type:

Scylla.json_writer
""""""""""""""""""

A JSON writer object, with both low-level and high-level APIs.
The low-level API allows you to write custom JSON and it loosely follows the API of `rapidjson::Writer <https://rapidjson.org/classrapidjson_1_1_writer.html>`_ (upon which it is implemented).
The high-level API is for writing `mutation fragments <scylla-sstable-sstable-content_>`_ as JSON directly, using the built-in JSON conversion logic that is used by `dump-data <dump-data_>`_ operation.

Low level API Methods:

* null() - write a null json value.
* bool(boolean) - write a bool json value.
* int(integer) - write an integer json value.
* double(number) - write a double json value.
* string(string) - write a string json value.
* start_object() - start a json object.
* key(string) - write the key of a json object.
* end_object() - write the end of a json object.
* start_array() - write the start of a json array.
* end_array() - write the end of a json array.

High level API Methods:

* start_stream() - start the stream, call at the very beginning.
* start_sstable() - start an sstable.
* start_partition() - start a partition.
* static_row() - write a static row to the stream.
* clustering_row() - write a clustering row to the stream.
* range_tombstone_change() - write a range tombstone change to the stream.
* end_partition() - end the current partition.
* end_sstable() - end the current sstable.
* end_stream() - end the stream, call at the very end.

.. _scylla-new-json-writer-method:

Scylla.new_json_writer()
""""""""""""""""""""""""

Create a `Scylla.json_writer <scylla-json-writer-type_>`_ instance.

.. _scylla-new-position-in-partition-method:

Scylla.new_position_in_partition()
""""""""""""""""""""""""""""""""""

Creates a `Scylla.position_in_partition <scylla-position-in-partition-type_>`_ instance.

Arguments:

* weight (integer) - the weight of the key.
* key (`Scylla.clustering_key <scylla-clustering-key-type_>`_) - the clustering key, optional.

.. _scylla-new-ring-position-method:

Scylla.new_ring_position()
""""""""""""""""""""""""""

Creates a `Scylla.ring_position <scylla-ring-position-type_>`_ instance.

Has several overloads:

* ``Scylla.new_ring_position(weight, key)``.
* ``Scylla.new_ring_position(weight, token)``.
* ``Scylla.new_ring_position(weight, key, token)``.

Where:

* weight (integer) - the weight of the key.
* key (`Scylla.partition_key <scylla-partition-key-type_>`_) - the partition key.
* token (integer) - the token (of the key if a key is provided).

.. _scylla-now-method:

Scylla.now()
""""""""""""

Create a `Scylla.gc_clock_time_point <scylla-gc-clock-time-point-type_>`_ instance, representing the current time.

.. _scylla-partition-key-type:

Scylla.partition_key
""""""""""""""""""""

Attributes:

* components (table) - the column values (`Scylla.data_value <scylla-data-value-type_>`_) making up the composite partition key.

Methods:

* to_hex - convert the key to its serialized format, encoded in hex.

Magic methods:

* __tostring - can be converted to string with tostring(), uses the built-in operator<< in Scylla.

See also:

* :ref:`Scylla.unserialize_partition_key() <scylla-unserialize-partition-key-method>`.
* :ref:`Scylla.token_of() <scylla-token-of-method>`.

.. _scylla-partition-start-type:

Scylla.partition_start
""""""""""""""""""""""

Attributes:

* key - the partition key's value as the appropriate Lua native type.
* token (integer) - the partition key's token.
* tombstone (`Scylla.tombstone <scylla-tombstone-type_>`_) - the partition tombstone, ``nil`` if no tombstone.

.. _scylla-position-in-partition-type:

Scylla.position_in_partition
""""""""""""""""""""""""""""

Currently used only for clustering positions.

Attributes:

* key (`Scylla.clustering_key <scylla-clustering-key-type_>`_) - the clustering key, ``nil`` if the position in partition represents the min or max clustering positions.
* weight (integer) - weight of the position, either -1 (before key), 0 (at key) or 1 (after key). If key attribute is ``nil``, the weight is never 0.

Methods:

* tri_cmp - compare this position in partition to another position in partition, returns -1 (``<``), 0 (``==``) or 1 (``>``).

See also:

* `Scylla.new_position_in_partition() <scylla-new-position-in-partition-method_>`_.

.. _scylla-range-tombstone-change-type:

Scylla.range_tombstone_change
"""""""""""""""""""""""""""""

Attributes:

* key ($TYPE) - the clustering key's value as the appropriate Lua native type.
* key_weight (integer) - weight of the position, either -1 (before key), 0 (at key) or 1 (after key).
* tombstone (`Scylla.tombstone <scylla-tombstone-type_>`_) - tombstone, ``nil`` if no tombstone.

.. _scylla-ring-position-type:

Scylla.ring_position
""""""""""""""""""""

Attributes:

* token (integer) - the token, ``nil`` if the ring position represents the min or max ring positions.
* key (`Scylla.partition_key <scylla-partition-key-type_>`_) - the partition key, ``nil`` if the ring position represents a position before/after a token.
* weight (integer) - weight of the position, either -1 (before key/token), 0 (at key) or 1 (after key/token). If key attribute is ``nil``, the weight is never 0.

Methods:

* tri_cmp - compare this ring position to another ring position, returns -1 (``<``), 0 (``==``) or 1 (``>``).

See also:

* `Scylla.new_ring_position() <scylla-new-ring-position-method_>`_.

.. _scylla-row-marker-type:

Scylla.row_marker
"""""""""""""""""

Attributes:

* timestamp (integer).
* is_live (boolean) - is the marker live?
* has_ttl (boolean) - is the marker expiring?
* ttl (integer) - time to live in seconds, ``nil`` if marker is not expiring.
* expiry (`Scylla.gc_clock_time_point <scylla-gc-clock-time-point-type_>`_) - time at which marker expires, ``nil`` if marker is not expiring.
* deletion_time (`Scylla.gc_clock_time_point <scylla-gc-clock-time-point-type_>`_) - time at which marker was deleted, ``nil`` unless marker is dead or expiring.

.. _scylla-schema-type:

Scylla.schema
"""""""""""""

Attributes:

* partition_key_columns (table) - list of `Scylla.column_definition <scylla-column-definition-type_>`_ of the key columns making up the partition key.
* clustering_key_columns (table) - list of `Scylla.column_definition <scylla-column-definition-type_>`_ of the key columns making up the clustering key.
* static_columns (table) - list of `Scylla.column_definition <scylla-column-definition-type_>`_ of the static columns.
* regular_columns (table) - list of `Scylla.column_definition <scylla-column-definition-type_>`_ of the regular columns.
* all_columns (table) - list of `Scylla.column_definition <scylla-column-definition-type_>`_ of all columns.

.. _scylla-sstable-type:

Scylla.sstable
""""""""""""""

Attributes:

* filename (string) - the full path of the sstable Data component file;

.. _scylla-static-row-type:

Scylla.static_row
"""""""""""""""""

Attributes:

* cells (table) - table of cells, where keys are the column names and the values are either of type `Scylla.atomic_cell <scylla-atomic-cell-type_>`_ or `Scylla.collection <scylla-collection-type_>`_.

.. _scylla-time-point-from-string-method:

Scylla.time_point_from_string()
"""""""""""""""""""""""""""""""

Create a `Scylla.gc_clock_time_point <scylla-gc-clock-time-point-type_>`_ instance from the passed in string.
Argument is string, using the same format as the CQL timestamp type, see https://en.wikipedia.org/wiki/ISO_8601.

.. _scylla-token-of-method:

Scylla.token_of()
"""""""""""""""""

Compute and return the token (integer) for a `Scylla.partition_key <scylla-partition-key-type_>`_.

.. _scylla-tombstone-type:

Scylla.tombstone
""""""""""""""""

Attributes:

* timestamp (integer)
* deletion_time (`Scylla.gc_clock_time_point <scylla-gc-clock-time-point-type_>`_) - the point in time at which the tombstone was deleted.

.. _scylla-unserialize-clustering-key-method:

Scylla.unserialize_clustering_key()
"""""""""""""""""""""""""""""""""""

Create a `Scylla.clustering_key <scylla-clustering-key-type_>`_ instance.

Argument is a string representing serialized clustering key in hex format.

.. _scylla-unserialize-partition-key-method:

Scylla.unserialize_partition_key()
""""""""""""""""""""""""""""""""""

Create a `Scylla.partition_key <scylla-partition-key-type_>`_ instance.

Argument is a string representing serialized partition key in hex format.

Examples
~~~~~~~~

You can find example scripts at https://github.com/scylladb/scylladb/tree/master/tools/scylla-sstable-scripts.

Examples
--------
Dumping the content of the SStable:

.. code-block:: console

   scylla sstable dump-data /path/to/md-123456-big-Data.db

Dumping the content of two SStables as a unified stream:

.. code-block:: console

   scylla sstable dump-data --merge /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db


Validating the specified SStables:

.. code-block:: console

   scylla sstable validate /path/to/md-123456-big-Data.db /path/to/md-123457-big-Data.db
