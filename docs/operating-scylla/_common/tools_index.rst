* :doc:`Nodetool Reference</operating-scylla/nodetool>` - Scylla commands for managing Scylla node or cluster using the command-line nodetool utility.
* :doc:`CQLSh - the CQL shell</cql/cqlsh>`.
* :doc:`REST - Scylla REST/HTTP Admin API</operating-scylla/rest>`.
* :doc:`Tracing </using-scylla/tracing>` - a ScyllaDB tool for debugging and analyzing internal flows in the server. 
* :doc:`SSTableloader </operating-scylla/admin-tools/sstableloader>` - Bulk load the sstables found in the directory to a Scylla cluster
* :doc:`scylla-sstable </operating-scylla/admin-tools/scylla-sstable>` - Validates and dumps the content of SStables, generates a histogram, dumps the content of the SStable index.
* :doc:`scylla-types </operating-scylla/admin-tools/scylla-types/>` - Examines raw values obtained from SStables, logs, coredumps, etc.
* :doc:`cassandra-stress </operating-scylla/admin-tools/cassandra-stress/>` A tool for benchmarking and load testing a Scylla and Cassandra clusters.
* :doc:`SSTabledump - Scylla 3.0, Scylla Enterprise 2019.1 and newer versions </operating-scylla/admin-tools/sstabledump>`
* :doc:`SSTable2JSON - Scylla 2.3 and older </operating-scylla/admin-tools/sstable2json>`
* sstablelevelreset - Reset level to 0 on a selected set of SSTables that use LeveledCompactionStrategy (LCS).
* :doc:`SSTable-Index </operating-scylla/admin-tools/sstable-index>` - A tool which lists all partitions contained in an SSTable index.
* sstablemetadata - Prints metadata about a specified SSTable.
* sstablerepairedset - Mark specific SSTables as repaired or unrepaired.
* configuration_encryptor - :doc:`encrypt at rest </operating-scylla/security/encryption-at-rest>` sensitive scylla configuration entries using system key.
* local_file_key_generator - Generate a local file (system) key for :doc:`encryption at rest </operating-scylla/security/encryption-at-rest>`, with the provided length, Key algorithm, Algorithm block mode and Algorithm padding method.
* `scyllatop <https://www.scylladb.com/2016/03/22/scyllatop/>`_ - A terminal base top-like tool for scylladb collectd/prometheus metrics.
* :doc:`scylla_dev_mode_setup</getting-started/install-scylla/dev-mod>` - run Scylla in Developer Mode.
* :doc:`perftune</operating-scylla/admin-tools/perftune>` - performance configuration.

Run each tool with ``-h``, ``--help`` for full options description.
