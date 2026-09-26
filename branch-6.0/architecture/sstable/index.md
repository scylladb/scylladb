# ScyllaDB SSTable Format

[Sorted Strings Table (SSTable)](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-SSTable) is the persistent file format used by ScyllaDB and Apache Cassandra. SSTable is saved as a persistent, ordered, immutable set of files on disk.
Immutable means SSTables are never modified; they are created by a MemTable flush and are deleted by a compaction.
The location of ScyllaDB SSTables is specified in scylla.yaml `data_file_directories` parameter (default location: `/var/lib/scylla/data`).

SSTable 3.x is more efficient and requires less disk space than the SSTable 2.x.

## SSTable Version Support

| SSTable Version   | ScyllaDB Enterprise Version   | ScyllaDB Open Source Version   |
|-------------------|-------------------------------|--------------------------------|
| 3.x (‘me’)        | 2022.2                        | 5.1 and above                  |
| 3.x (‘md’)        | 2021.1                        | 4.3, 4.4, 4.5, 4.6, 5.0        |
| 3.0 (‘mc’)        | 2019.1, 2020.1                | 3.x, 4.1, 4.2                  |
| 2.2 (‘la’)        | N/A                           | 2.3                            |
| 2.1.8 (‘ka’)      | 2018.1                        | 2.2                            |
* In Scylla 6.0 and above, *me* format is enabled by default.
* In Scylla Enterprise 2021.1, Scylla 4.3 and above, *md* format is enabled by default.
* In Scylla 3.1 and above, *mc* format is enabled by default.

For more information on each of the SSTable formats, see below:

* [SSTable 2.x](https://opensource.docs.scylladb.com/branch-6.0/architecture/sstable/sstable2/index.md)
* [SSTable 3.x](https://opensource.docs.scylladb.com/branch-6.0/architecture/sstable/sstable3/index.md)
