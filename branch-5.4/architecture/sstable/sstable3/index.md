# ScyllaDB SSTable - 3.x

[Sorted Strings Table (SSTable)](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-SSTable) is the persistent file format used by ScyllaDB and Apache Cassandra. SSTable is saved as a persistent, ordered, immutable set of files on disk.
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
* In ScyllaDB 5.1 and above, the `me` format is enabled by default.
* In ScyllaDB 4.3 to 5.0, the `md` format is enabled by default.
* In ScyllaDB 3.1 to 4.2, the `mc` format is enabled by default.
* In ScyllaDB 3.0, the `mc` format is disabled by default. You can enable it by adding the `enable_sstables_mc_format` parameter set to `true` in the `scylla.yaml` file. For example:
  > ```shell
  > enable_sstables_mc_format: true
  > ```

<!-- REMOVE IN FUTURE VERSIONS - Remove the note above in version 5.2. -->

## Additional Information

For more information on ScyllaDB 3.x SSTable formats, see below:

* [SSTable 3.0 Data File Format](https://opensource.docs.scylladb.com/branch-5.4/architecture/sstable/sstable3/sstables-3-data-file-format.md)
* [SSTable 3.0 Statistics](https://opensource.docs.scylladb.com/branch-5.4/architecture/sstable/sstable3/sstables-3-statistics.md)
* [SSTable 3.0 Summary](https://opensource.docs.scylladb.com/branch-5.4/architecture/sstable/sstable3/sstables-3-summary.md)
* [SSTable 3.0 Index](https://opensource.docs.scylladb.com/branch-5.4/architecture/sstable/sstable3/sstables-3-index.md)
* [SSTable 3.0 Format in ScyllaDB](https://opensource.docs.scylladb.com/branch-5.4/architecture/sstable/sstable3/sstable-format.md)
