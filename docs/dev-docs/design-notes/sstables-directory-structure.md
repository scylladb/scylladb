
# sstables directory structure

## Introduction

SSTables are stored as a set of regular files in the file system
in a common directory per-table (a.k.a. column family).

In addition to SSTable files, sub-directories of the table base directory
are used for additional features such as snapshots, and atomic deletions recovery.

This document summarizes the directory structure and file organization of SSTables.

## Directory Hierarchy

Scylla uses the following directory structure to store all its SSTables, for example:

```
/var/lib/scylla
└── data
    ├── ks
    │   ├── cf-6749a080303111e9b2f8000000000000
    │   │   ├── ...
    │   │   ├── mc-2-big-TOC.txt
    │   │   ├── snapshots
    │   │   │   └── 1550133010687-cf
    │   │   │       ├── manifest.json
    │   │   │       ├── ...
    │   │   │       └── mc-1-big-TOC.txt
    │   │   ├── staging
    │   │   └── upload
    │   └── cf-7ec943202fc611e9a130000000000000
    │       ├── snapshots
    │       │   └── 1550132311207-cf
    │       │       ├── ...
    │       │       ├── ks-cf-ka-3-TOC.txt
    │       │       └── manifest.json
    │       ├── staging
    │       └── upload
    ├── system
    │   ├── schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697
    │   │   ├── staging
    │   │   └── upload
    │   ├── ...
    ├── ...
```

Each keyspace, including system keyspaces has its own sub-directory
under the data directory (See `data_file_directories` in scylla.yaml).
The keyspace directory name is the keyspace name.

In the keyspace directory there is a sub-directory per table
which is named by the table name followed by a dash and a unique identifier
to distinguish between different incarnations of tables that are called with the same name.

In the table directory there are the SSTable files and additional
sub-directories as documented below.

## SSTable Files

SSTables are comprised of multiple component files.
The component file names are self-identifying and denote the component type, as well as per-sstable-format metadata.

Here are the different component types and their naming convention:

* Data (`Data.db`)  
  The SSTable data file, containing a part of the actual data stored in the database.

  See [SSTables-Data-File](https://github.com/scylladb/scylla/wiki/SSTables-Data-File) for more information.

* Primary Index (`Index.db`)  
  Index of the row keys with pointers to their positions in the data file.

  See [SSTables-Index-File](https://github.com/scylladb/scylla/wiki/SSTables-Index-File) for more information.


* Bloom filter (`Filter.db`)  
  A structure stored in memory that checks if row data exists in the memtable before accessing SSTables on disk.


* Compression Information (`CompressionInfo.db`)  
  A file holding information about uncompressed data length, chunk offsets and other compression information.


* Statistics (`Statistics.db`)  
  Statistical metadata about the content of the SSTable and encoding statistics for the data file, starting with the mc format.


* Digest (`Digest.crc32`, `Digest.adler32`, `Digest.sha1`)  
  A file holding checksum of the data file.  
  The method used for checksum is specific to the SSTable format version (See below).


* CRC (`CRC.db`)  
  A file holding the CRC32 for chunks in an uncompressed file.


* SSTable Index Summary (`Summary.db`)  
  A sample of the partition index stored in memory.


* SSTable Table of Contents (`TOC.txt`)  
  A file that stores the list of all components for the SSTable TOC.  
  See details below regarding the use of a temporary TOC name during creation and deletion of SSTables.


* Scylla (`Scylla.db`)  
  A file holding scylla-specific metadata about the SSTable, such as sharding information, extended features support, and sstabe-run identifier.

### SSTable Format Version

SSTable's on-disk format has changed over time.
Three versions are currently supported by Scylla: `ka`, `la`, and `mc`.
Cassandra's convention is that the first letter determines
the major format version, in ascending order, and the second letter -
the minor version, starting from `a` onward.

The SSTable file names identify the SSTable format version.
In addition, they provide the SSTable generation number and other metadata.

The "encoding" of the above metadata into the file name changed over time
and it is version specific, as follows:

```
    mc-<generation>-<big>-<component>
    la-<generation>-<big>-<component>
    <keyspace>-<column_family>-ka-<generation>-<component>
```

where:
* `<generation>` is the SSTable generation - a unique positive number identifying the SSTable.
* `<big>` is an archaic attribute that identifies the SSTable sub-format.  
  (Only `big` sub-format is supported by Scylla (and Cassandra) at this time.)
* `<component>` is the file's component type, as described above.

### Table Sub-directories

The per-table directory may contain several sub-directories, as listed below:

* Staging directory (`staging`)  
  A sub-directory holding materialized views SSTables during their update process.


* Snapshots directory (`snapshots`)  
  A sub-directory holding snapshots of SSTables, using hard links to the actual SSTable component files in the table base directory.


* Upload directory (`upload`)  
  Used for ingesting external SSTables into Scylla on startup.


* Temporary SSTable directory (`<generation>.sstable`)  
  A directory created when writing new SSTables.

  Some file systems (e.g. linux XFS) base their locality-of-use heuristics based on the directory in which files were created.
  In this case, if all files are created in one (or a few) directories, block allocation can become very slow.
  To overcome this issue, when a SSTable is created, the database creates a new sub-directory using the newly-created SSTable generation, named `<generation>.sstable`
  and all SSTable component files are then created in this sub-directory and moved to the table base directory.  
  
* Pending-delete directory (`pending_delete`)  
  A directory that may hold log files for replaying atomic deletion operations of SSTables.

### Temporary TOC Files

SSTables are immutable. I.e., once written and sealed, they are never re-written.  
For data consistency reasons, it is important for the database to determine that a SSTable is complete and valid,
in contrast to a SSTable that might be in a transitional state while being created or while being deleted.

When created and initially written, the table of contents is stored in a TemporaryTOC file - `TOC.txt.tmp`.  
It is renamed to `TOC.txt` when the SSTable is sealed and all components are flushed to stable storage and ready to be used.

When a SSTable is removed, `TOC.txt` is first renamed to `TOC.txt.tmp`, and that atomically marks the SSTable as deleted.

## Recovering from crashes

On startup, the database scans all table directories and cleans up all SSTables that are in a transitional state: either partially written or partially deleted.
These SSTables are identified by their TemporaryTOC component, and the loader simply removes them.

In addition, any existing temporary SSTable sub-directories are automatically removed.

### Atomic deletion of SSTables

In certain cases, the database is required to delete a number of SSTable in an atomic manner.
For example, one of the SSTables may hold a tombstone that deletes data that was inserted to a different SSTable, and both are to be deleted as part of compaction.

When such operation is initiated, `delete_atomically` creates a unique, temporary log file in the `pending_delete` sub-directory named:
`sstables-<min_generation>-<max_generation>.log.tmp`, based on the SSTables to-be-deleted minimum and maximum generation numbers.

The log file contains the list of SSTables' TOC filenames (basename only, with no leading path), one TOC per line.  
After the temporary log file if written, flushed, and closed; it is renamed to its final name: `sstables-<min_generation>-<max_generation>.log`.

Finally, after the SSTables are removed, the log file is removed from the `pending_delete` sub-directory.

On startup, sealed `pending_delete` log files are replayed and after all requires SSTables are deleted successfully, the log file is deleted.

Any temporary `pending_delete` log files that are found during startup are simply removed, as this is an indication that:
- The atomic delete operation had not started to delete any SSTable, and
- The log file may be partially written.
