# Nodetool scrub

## NAME

**scrub** - Help identify and fix corrupted SSTable.
Remove faulty data,  eliminate tombstoned rows that have surpassed the table’s gc_grace period, and fix out-of-order rows and partitions.

## SYNOPSIS

```shell
           nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                [(-ns | --no-snapshot)]
                [(-s | --skip-corrupted)]
                [(-m <scrub_mode> | --mode <scrub_mode>)]
                [--] <keyspace> [<table...>]

Supported scrub modes: ABORT, SKIP, SEGREGATE, VALIDATE
```

## OPTIONS

| Parameter                            | Description                                                                                                                |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| -ns / –no-snapshot                   | Do not take a snapshot of all scrubbed tables before starting scrub (default false).                                       |
| -s / –skip-corrupted                 | Skip corrupted rows or partitions even when scrubbing counter tables.<br/>(Deprecated, use ‘–mode’ instead. default false) |
| -m <scrub_mode> / –mode <scrub_mode> | How to handle corrupt data (one of: ABORT|SKIP|SEGREGATE|VALIDATE, default ABORT; overrides ‘–skip-corrupted’)             |

`--` This option can be used to separate command-line options from the list of argument, (useful when arguments might be mistaken for command-line options.

`<keyspace>` The keyspace to scrub.

`[<table...>]` Optional. One or more tables to scrub.  By default, all tables in the keyspace are scrubbed.

## SCRUB MODES

| Scrub mode   | Description                                                                                                                                                                                              |
|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ABORT        | Abort scrubbing when the first validation error occurs. (default).                                                                                                                                       |
| SKIP         | Skip corrupted rows or partitions. (equivalent to the legacy –skip-corrupted option).                                                                                                                    |
| SEGREGATE    | Sort out-of-order rows or partitions by segregating them into additional SSTables.                                                                                                                       |
| VALIDATE     | Read-only mode: report any corruptions found while scrubbing but do not fix them.<br/>By default, corrupt SSTables are moved into a “quarantine” subdirectory so they will not be subject to compaction. |

## Examples

Scrub **all** tables in a keyspace (mykeyspace)

```shell
> nodetool scrub mykeyspace
```

Scrub **a** specific table (mytable) in a keyspace (mykeyspace)

```shell
> nodetool scrub mykeyspace mytable
```

Scrub **a** specific table (mytable) in a keyspace (mykeyspace) in SEGREGATE mode

```shell
> nodetool scrub -m SEGREGATE mykeyspace mytable
```

Scrub **a** specific table (mytable) in a keyspace (mykeyspace) in VALIDATE mode without taking a preliminary snapshot

```shell
> nodetool scrub -m VALIDATE --no-snapshot mykeyspace mytable
```

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
