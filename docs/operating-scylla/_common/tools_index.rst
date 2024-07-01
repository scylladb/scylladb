* :doc:`Nodetool Reference</operating-scylla/nodetool>` - ScyllaDB commands for managing ScyllaDB node or cluster using the command-line nodetool utility.
* :doc:`CQLSh - the CQL shell</cql/cqlsh>`.
* :doc:`Admin REST API - ScyllaDB Node Admin API</operating-scylla/rest>`.
* :doc:`Tracing </using-scylla/tracing>` - a ScyllaDB tool for debugging and analyzing internal flows in the server. 
* :doc:`SSTableloader </operating-scylla/admin-tools/sstableloader>` - Bulk load the sstables found in the directory to a ScyllaDB cluster
* :doc:`ScyllaDB SStable </operating-scylla/admin-tools/scylla-sstable>` - Validates and dumps the content of SStables, generates a histogram, dumps the content of the SStable index.
* :doc:`ScyllaDB Types </operating-scylla/admin-tools/scylla-types/>` - Examines raw values obtained from SStables, logs, coredumps, etc.
* :doc:`cassandra-stress </operating-scylla/admin-tools/cassandra-stress/>` A tool for benchmarking and load testing a ScyllaDB and Cassandra clusters.
* :doc:`SSTabledump </operating-scylla/admin-tools/sstabledump>`
* :doc:`SSTableMetadata </operating-scylla/admin-tools/sstablemetadata>`
* sstablelevelreset - Reset level to 0 on a selected set of SSTables that use LeveledCompactionStrategy (LCS).
* sstablerepairedset - Mark specific SSTables as repaired or unrepaired.
* `scyllatop <https://www.scylladb.com/2016/03/22/scyllatop/>`_ - A terminal base top-like tool for scylladb collectd/prometheus metrics.
* :doc:`scylla_dev_mode_setup</getting-started/installation-common/dev-mod>` - run ScyllaDB in Developer Mode.
* :doc:`perftune</operating-scylla/admin-tools/perftune>` - performance configuration.
* :doc:`Reading mutation fragments</operating-scylla/admin-tools/select-from-mutation-fragments/>` - dump the underlying mutation data from tables.
* :doc:`Maintenance socket </operating-scylla/admin-tools/maintenance-socket/>` - a Unix domain socket for full-permission CQL connection.
* :doc:`Maintenance mode </operating-scylla/admin-tools/maintenance-mode/>` - a mode for performing maintenance tasks on an offline ScyllaDB node.


Run each tool with ``-h``, ``--help`` for full options description.
