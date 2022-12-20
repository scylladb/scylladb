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

The command syntax is as follows:

.. code-block:: console

   scylla sstable <operation> <path to SStable>


You can specify more than one SStable.

Schema
------
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
^^^^^^^^^^^^^^^

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
    )",
                dump_index_operation},
    /* dump-compression-info */
        {"dump-compression-info",
                "Dump content of sstable compression info(s)",
    R"(
    Dumps the content of the compression-info component. Contains compression
    parameters and maps positions into the uncompressed data to that into compressed
    data. Note that compression happens over chunks with configurable size, so to
    get data at a position in the middle of a compressed chunk, the entire chunk has
    to be decompressed.
    For more information about the sstable components and the format itself, visit
    https://docs.scylladb.com/architecture/sstable/.

    The content is dumped in JSON, using the following schema:

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
