# ScyllaDB SSTable - 2.x

[Sorted Strings Table (SSTable)](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-SSTable) is the persistent file format used by ScyllaDB and Apache Cassandra. SSTable is saved as a persistent, ordered, immutable set of files on disk.
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

For more information about ScyllaDB 2.x SSTable formats, see below:

* [SSTable Compression](https://opensource.docs.scylladb.com/branch-6.1/architecture/sstable/sstable2/sstable-compression.md) - Deep dive into ScyllaDB/Apache Cassandra SSTable Compression
* [SSTable Data File](https://opensource.docs.scylladb.com/branch-6.1/architecture/sstable/sstable2/sstable-data-file.md) - Deep dive into ScyllaDB/Apache Cassandra SSTable format
* [SSTable format in ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/architecture/sstable/sstable2/sstable-format.md) - ScyllaDB SSTables are compatible to those in Apache Cassandra 2.1.8, but why there are more of them?
* [SSTable Interpretation](https://opensource.docs.scylladb.com/branch-6.1/architecture/sstable/sstable2/sstable-interpretation.md) - Deep dive into ScyllaDB/Apache Cassandra SSTable Interpretation in ScyllaDB
* [SSTable Summary File](https://opensource.docs.scylladb.com/branch-6.1/architecture/sstable/sstable2/sstable-summary-file.md) - Deep dive into ScyllaDB/Apache Cassandra SSTable Summary file format

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
