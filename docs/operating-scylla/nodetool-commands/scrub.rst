Nodetool scrub
==============

NAME
....

**scrub** - Scrub the SSTable files in the specified keyspace or table(s)

SYNOPSIS
........

.. code-block:: shell

              nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                   [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                   [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                   [(-ns | --no-snapshot)]
                   [(-s | --skip-corrupted)]
                   [(-m <scrub_mode> | --mode <scrub_mode>)]
                   [--] <keyspace> [<table...>]

   Supported scrub modes: ABORT, SKIP, SEGREGATE, VALIDATE

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
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
SEGREGATE                                                             Sort out-of-order rows or partitions by segregating them into additional SSTables.
--------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------------------
VALIDATE                                                              Read-only mode: report any corruptions found while scrubbing but do not fix them.
                                                                      By default, corrupt SSTables are moved into a "quarantine" subdirectory so they will not be subject to compaction.
====================================================================  ==================================================================================================================

SCRUB PROCEDURE
...............

Other compaction operations can interefere with scrub, resulting in some sstables not being scrubbed and the problem that scrub was intended to solve will presist.
To avoid this, it is recommended to disable autocompaction and stop any ongoing compaction before running scrub:

.. code-block:: shell

   # Disable auto compaction for the table
   > nodetool disableautocompaction <keyspace> <table>

   # Either wait for any running compactions to finish, or stop all compaction (see below), note this will affect *all* tables.
   > nodetool stop COMPACTION

   # Run the scrub
   > nodetool scrub --mode=<scrub_mode> <keyspace> <table>

   # Re-enable auto compaction for the table
   > nodetool enableautocompaction <keyspace> <table>


Also be mindful of the fact that scrub creates a snapshot by default, just before starting. This might be desirable, to allow restoring the data in case scrub goes wrong, just be mindful of the space requirements.

The default scrub mode is ABORT, which is not really useful for solving problems. Consider using SEGREGATE or VALIDATE. VALIDATE can be used as a non-intrusive way to check for the presence of problems, or gaguge the impact of a known problem. Note that while this does not modify sstables, it still has a significant CPU cost and might affect production workload. SEGREGATE mode is the one that is able to fix sstables with out-of-order data or bad indexes.


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

.. include:: /rst_include/apache-copyrights.rst
