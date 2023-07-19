* :doc:`Nodetool Reference</operating-scylla/nodetool>` - Scylla commands for managing Scylla node or cluster using the command-line nodetool utility.
* :doc:`CQLSh - the CQL shell</cql/cqlsh>`.
* :doc:`REST - Scylla REST/HTTP Admin API</operating-scylla/rest>`.
* :doc:`Tracing </using-scylla/tracing>` - a ScyllaDB tool for debugging and analyzing internal flows in the server. 
* :doc:`SSTableloader </operating-scylla/admin-tools/sstableloader>` - Bulk load the sstables found in the directory to a Scylla cluster
* :doc:`Scylla SStable </operating-scylla/admin-tools/scylla-sstable>` - Validates and dumps the content of SStables, generates a histogram, dumps the content of the SStable index.
* :doc:`Scylla Types </operating-scylla/admin-tools/scylla-types/>` - Examines raw values obtained from SStables, logs, coredumps, etc.
* :doc:`cassandra-stress </operating-scylla/admin-tools/cassandra-stress/>` A tool for benchmarking and load testing a Scylla and Cassandra clusters.
* :doc:`SSTabledump - Scylla 3.0, Scylla Enterprise 2019.1 and newer versions </operating-scylla/admin-tools/sstabledump>`
* :doc:`SSTable2JSON - Scylla 2.3 and older </operating-scylla/admin-tools/sstable2json>`
* sstablelevelreset - Reset level to 0 on a selected set of SSTables that use LeveledCompactionStrategy (LCS).
* sstablemetadata - Prints metadata about a specified SSTable.
* sstablerepairedset - Mark specific SSTables as repaired or unrepaired.
* `scyllatop <https://www.scylladb.com/2016/03/22/scyllatop/>`_ - A terminal base top-like tool for scylladb collectd/prometheus metrics.
* :doc:`scylla_dev_mode_setup</getting-started/installation-common/dev-mod>` - run Scylla in Developer Mode.
* :doc:`perftune</operating-scylla/admin-tools/perftune>` - performance configuration.
* :doc:`SELECT * FROM MUTATION_FRAGMENTS() Statement </operating-scylla/admin-tools/select-from-mutation-fragments/>` - dump the underlying mutation data from tables.


Run each tool with ``-h``, ``--help`` for full options description.
