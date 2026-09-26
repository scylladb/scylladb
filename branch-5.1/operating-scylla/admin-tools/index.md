# Admin Tools


            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Admin Tools</h5>
            * [Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool.md) - Scylla commands for managing Scylla node or cluster using the command-line nodetool utility.
* [CQLSh - the CQL shell](https://opensource.docs.scylladb.com/branch-5.1/cql/cqlsh.md).
* [REST - Scylla REST/HTTP Admin API](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/rest.md).
* [Tracing](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/tracing.md) - a ScyllaDB tool for debugging and analyzing internal flows in the server.
* [SSTableloader](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/sstableloader.md) - Bulk load the sstables found in the directory to a Scylla cluster
* [Scylla SStable](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/scylla-sstable.md) - Validates and dumps the content of SStables, generates a histogram, dumps the content of the SStable index.
* [Scylla Types](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/scylla-types.md) - Examines raw values obtained from SStables, logs, coredumps, etc.
* [cassandra-stress](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/cassandra-stress.md) A tool for benchmarking and load testing a Scylla and Cassandra clusters.
* [SSTabledump - Scylla 3.0, Scylla Enterprise 2019.1 and newer versions](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/sstabledump.md)
* [SSTable2JSON - Scylla 2.3 and older](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/sstable2json.md)
* sstablelevelreset - Reset level to 0 on a selected set of SSTables that use LeveledCompactionStrategy (LCS).
* [SSTable-Index](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/sstable-index.md) - A tool which lists all partitions contained in an SSTable index.
* sstablemetadata - Prints metadata about a specified SSTable.
* sstablerepairedset - Mark specific SSTables as repaired or unrepaired.
* configuration_encryptor - [encrypt at rest](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/encryption-at-rest.md) sensitive scylla configuration entries using system key.
* local_file_key_generator - Generate a local file (system) key for [encryption at rest](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/encryption-at-rest.md), with the provided length, Key algorithm, Algorithm block mode and Algorithm padding method.
* [scyllatop](https://www.scylladb.com/2016/03/22/scyllatop/) - A terminal base top-like tool for scylladb collectd/prometheus metrics.
* [scylla_dev_mode_setup](https://opensource.docs.scylladb.com/branch-5.1/getting-started/install-scylla/dev-mod.md) - run Scylla in Developer Mode.
* [perftune](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/perftune.md) - performance configuration.

Run each tool with `-h`, `--help` for full options description.

</div></div>

The [Admin Procedures and Monitoring lesson](https://university.scylladb.com/courses/scylla-operations/lessons/admin-procedures-and-basic-monitoring/topic/admin-procedures-and-monitoring/) on Scylla University provides more training and examples material on this subject.
