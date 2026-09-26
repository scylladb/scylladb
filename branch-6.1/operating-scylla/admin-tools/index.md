# Admin Tools


            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Admin Tools</h5>
            * [Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md) - ScyllaDB commands for managing ScyllaDB node or cluster using the command-line nodetool utility.
* [CQLSh - the CQL shell](https://opensource.docs.scylladb.com/branch-6.1/cql/cqlsh.md).
* [Admin REST API - ScyllaDB Node Admin API](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/rest.md).
* [Tracing](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/tracing.md) - a ScyllaDB tool for debugging and analyzing internal flows in the server.
* [SSTableloader](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/sstableloader.md) - Bulk load the sstables found in the directory to a ScyllaDB cluster
* [ScyllaDB SStable](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/scylla-sstable.md) - Validates and dumps the content of SStables, generates a histogram, dumps the content of the SStable index.
* [ScyllaDB Types](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/scylla-types.md) - Examines raw values obtained from SStables, logs, coredumps, etc.
* [cassandra-stress](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/cassandra-stress.md) A tool for benchmarking and load testing a ScyllaDB and Cassandra clusters.
* [SSTabledump](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/sstabledump.md)
* [SSTableMetadata](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/sstablemetadata.md)
* sstablelevelreset - Reset level to 0 on a selected set of SSTables that use LeveledCompactionStrategy (LCS).
* sstablerepairedset - Mark specific SSTables as repaired or unrepaired.
* [scyllatop](https://www.scylladb.com/2016/03/22/scyllatop/) - A terminal base top-like tool for scylladb collectd/prometheus metrics.
* [scylla_dev_mode_setup](https://opensource.docs.scylladb.com/branch-6.1/getting-started/installation-common/dev-mod.md) - run ScyllaDB in Developer Mode.
* [perftune](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/perftune.md) - performance configuration.
* [Reading mutation fragments](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/select-from-mutation-fragments.md) - dump the underlying mutation data from tables.
* [Maintenance socket](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/maintenance-socket.md) - a Unix domain socket for full-permission CQL connection.
* [Maintenance mode](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin-tools/maintenance-mode.md) - a mode for performing maintenance tasks on an offline ScyllaDB node.

Run each tool with `-h`, `--help` for full options description.

</div></div>

The [Admin Procedures and Monitoring lesson](https://university.scylladb.com/courses/scylla-operations/lessons/admin-procedures-and-basic-monitoring/topic/admin-procedures-and-monitoring/) on ScyllaDB University provides more training and examples material on this subject.
