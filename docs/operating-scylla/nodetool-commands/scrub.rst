Nodetool scrub
==============

NAME
....

**scrub** - Help identify and fix corrupted SSTable. Not all kinds of corruption can be skipped or fixed by scrub.
Remove faulty data, eliminate tombstoned rows that have surpassed the table's gc_grace period, and fix out-of-order rows and partitions.


SYNOPSIS
........

.. code-block:: shell

              nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                   [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                   [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                   [(-ns | --no-snapshot)]
                   [(-s | --skip-corrupted)]
                   [(-m <scrub_mode> | --mode <scrub_mode>)]
                   [(-q <quarantine_mode> | --quarantine-mode <quarantine_mode>)]
                   [--drop-unfixable-sstables]
                   [--] <keyspace> [<table...>]

   Supported scrub modes: ABORT, SKIP, SEGREGATE, VALIDATE
   Supported quarantine modes: INCLUDE, EXCLUDE, ONLY

OPTIONS
.......

====================================================================  ==================================================================================================================
Parameter                                                             Description
====================================================================  ==================================================================================================================
-ns / --no-snapshot                                                   Do not take a snapshot of all scrubbed tables before starting scrub (default false).
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
-s / --skip-corrupted                                                 Skip corrupted rows or partitions even when scrubbing counter tables.
                                                                      (Deprecated, use '--mode' instead. default false)
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
-m <scrub_mode> / --mode <scrub_mode>                                 How to handle corrupt data (one of: ABORT|SKIP|SEGREGATE|VALIDATE, default ABORT; overrides '--skip-corrupted')
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
-q <quarantine_mode> / --quarantine-mode <quarantine_mode>            How to handle quarantined SSTables (one of: INCLUDE|EXCLUDE|ONLY, default INCLUDE)
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
--drop-unfixable-sstables                                             Drop unfixable SSTables instead of aborting the entire scrub (only valid with --mode=SEGREGATE)
====================================================================  ==================================================================================================================

``--`` This option can be used to separate command-line options from the list of argument, (useful when arguments might be mistaken for command-line options.

``<keyspace>`` The keyspace to scrub.

``[<table...>]`` Optional. One or more tables to scrub.  By default, all tables in the keyspace are scrubbed.

SCRUB MODES
...........

====================================================================  ==================================================================================================================
Scrub mode                                                            Description
====================================================================  ==================================================================================================================
ABORT                                                                 Abort scrubbing when the first validation error occurs. (default).
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
SKIP                                                                  Skip corrupted rows or partitions. (equivalent to the legacy --skip-corrupted option).
                                                                      **Warning**: This mode can cause data loss by removing invalid data portions or entire
                                                                      SSTables if severely corrupted (e.g., digest mismatch detected).
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
SEGREGATE                                                             Sort out-of-order rows or partitions by segregating them into additional SSTables.
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
VALIDATE                                                              Read-only mode: report any corruptions found while scrubbing but do not fix them.
                                                                      By default, corrupt SSTables are moved into a "quarantine" subdirectory so they will not be subject to compaction.
====================================================================  ==================================================================================================================

QUARANTINE MODES
................

====================================================================  ==================================================================================================================
Quarantine mode                                                       Description
====================================================================  ==================================================================================================================
INCLUDE                                                               Process both regular and quarantined SSTables (default).
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
EXCLUDE                                                               Process only regular (non-quarantined) SSTables.
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
ONLY                                                                  Process only quarantined SSTables.
====================================================================  ==================================================================================================================

Examples
........

Scrub **all** tables in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool scrub mykeyspace


Scrub **a** specific table (mytable) in a keyspace (mykeyspace)

.. code-block:: shell

   > nodetool scrub mykeyspace mytable

Scrub **a** specific table (mytable) in a keyspace (mykeyspace) in SEGREGATE mode

.. code-block:: shell

   > nodetool scrub -m SEGREGATE mykeyspace mytable

Scrub **a** specific table (mytable) in a keyspace (mykeyspace) in VALIDATE mode without taking a preliminary snapshot

.. code-block:: shell

   > nodetool scrub -m VALIDATE --no-snapshot mykeyspace mytable

Scrub **a** specific table (mytable) in a keyspace (mykeyspace) in SEGREGATE mode dropping unfixable SSTables

.. code-block:: shell

   > nodetool scrub -m SEGREGATE --drop-unfixable-sstables mykeyspace mytable

Procedures for Removing Bad SSTables
.....................................

Method 1: Quarantine and Drop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Step 1**: Run scrub in VALIDATE mode to identify and quarantine corrupted SSTables:

.. code-block:: shell

   > nodetool scrub -m VALIDATE keyspace_name table_name

This will move corrupted SSTables to a ``quarantine`` directory.
The ``quarantine`` directory is a sub-directory of the table's respective data directory.

**Step 2** (Optional): Preserve quarantined SSTables for analysis:

Before permanently dropping the corrupted SSTables, consider copying some or all of them aside,
somewhere outside of the ScyllaDB data directory, so they are preserved for later investigation by the ScyllaDB R&D team,
to determine the root cause of the corruption.

.. code-block:: shell

   # Copy quarantined SSTables to a backup location for analysis
   > cp -r /path/to/data/keyspace_name/table_dir/quarantine /path/to/backup/location/

**Step 3**: Drop the quarantined SSTables using :doc:`dropquarantinedsstables </operating-scylla/nodetool-commands/dropquarantinedsstables>`:

.. code-block:: shell

   > nodetool dropquarantinedsstables keyspace_name table_name

This permanently removes the quarantined SSTables from the specified table.

Method 2: Segregate with Drop Unfixable Flag
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach attempts to fix what can be fixed and automatically drops SSTables that cannot be fixed.

.. note::
   This method should be used for the subset of corruption issues where SEGREGATE mode can actually help: where corruption manifests at least partly in reordered partitions or rows.

**Step 1**: Run scrub in SEGREGATE mode with the ``--drop-unfixable-sstables`` flag:

.. code-block:: shell

   > nodetool scrub -m SEGREGATE --drop-unfixable-sstables keyspace_name table_name

This will:

- Attempt to segregate and fix out-of-order data where possible
- Remove faulty data
- Automatically drop SSTables that cannot be fixed
- Create new properly ordered SSTables from the recoverable data

.. include:: /rst_include/apache-copyrights.rst
