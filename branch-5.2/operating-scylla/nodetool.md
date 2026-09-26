# Nodetool

The `nodetool` utility provides a simple command-line interface to the following exposed operations and attributes. Scylla’s nodetool is a fork of [the Apache Cassandra nodetool](https://cassandra.apache.org/doc/latest/tools/nodetool/nodetool.html) with the same syntax and a subset of the operations.

<a id="nodetool-generic-options"></a>

## Nodetool generic options

All options are supported:

```shell
( -h | --host ) <host name> | <ip address>
( -p | --port ) <port number>
( -pw | --password ) <password >
( -u | --username ) <user name>
( -pwf <passwordFilePath | --password-file <passwordFilePath> )
```

## Supported Nodetool operations

Operations that are not listed below are currently not available.

* [cfhistograms](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/cfhistograms.md) - Provides statistics about a table, including number of SSTables, read/write latency, partition size and column count.
* [cfstats](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/cfstats.md) - Provides in-depth diagnostics regard table.
* [cleanup](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/cleanup.md) - Triggers the immediate cleanup of keys no longer belonging to a node.
* [clearsnapshot](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/clearsnapshot.md) - This command removes snapshots.
* [compactionhistory](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/compactionhistory.md) - Provides the history of compactions.
* [compactionstats](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/compactionstats.md)- Print statistics on compactions.
* [compact](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/compact.md)- Force a (major) compaction on one or more column families.
* [decommission](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/decommission.md) - Decommission the node.
* [describecluster](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/describecluster.md) - Print the name, snitch, partitioner and schema version of a cluster.
* [describering](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/describering.md) - `<keyspace>`- Shows the partition ranges of a given keyspace.
* [disableautocompaction](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/disableautocompaction.md) - Disable automatic compaction of a keyspace or table.
* [disablebackup](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/disablebackup.md) - Disable incremental backup.
* [disablebinary](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/disablebinary.md) - Disable native transport (binary protocol).
* [disablegossip](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/disablegossip.md) - Disable gossip (effectively marking the node down).
* [drain](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/drain.md) - Drain the node (stop accepting writes and flush all column families).
* [enableautocompaction](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/enbleautocompaction.md) - Enable automatic compaction of a keyspace or table.
* [enablebackup](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/enablebackup.md) - Enable incremental backup.
* [enablebinary](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/enablebinary.md) - Reenable native transport (binary protocol).
* [enablegossip](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/enablegossip.md) - Reenable gossip.
* [flush](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/flush.md) - Flush one or more column families.
* [getendpoints](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/getendpoints.md) `<keyspace>` `<table>` `<key>`- Print the end points that owns the key.
* **getlogginglevels** - Get the runtime logging levels.
* [gettraceprobability](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/gettraceprobability.md) - Displays the current trace probability value. 0 is disabled 1 is enabled.
* [gossipinfo](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/gossipinfo.md) - Shows the gossip information for the cluster.
* [help](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/help.md) - Display list of avilable nodetool commands.
* [info](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/info.md) - Print node information
* [listsnapshots](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/listsnapshots.md) - Lists all the snapshots along with the size on disk and true size.
* **move** `<new token>`- Move node on the token ring to a new token
* **netstats** - Print network information on provided host (connecting node by default)
* [proxyhistograms](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/proxyhistograms.md) - Print statistic histograms for network operations
* [rebuild](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/rebuild.md) `[<src-dc-name>]`- Rebuild data by streaming from other nodes
* [refresh](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/refresh.md)- Load newly placed SSTables to the system without restart
* [removenode](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/removenode.md)- Remove node with the provided ID
* [repair](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/repair.md)  `<keyspace>` `<table>` - Repair one or more tables
* [ring](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/ring.md) - The nodetool ring command display the token ring information.
* [scrub](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/scrub.md) `[-m mode] [--no-snapshot] <keyspace> [<table>...]` - Scrub the SSTable files in the specified keyspace or table(s)
* [setlogginglevel](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/setlogginglevel.md) - sets the logging level threshold for Scylla classes
* [settraceprobability](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/settraceprobability.md) `<value>` - Sets the probability for tracing a request. race probability value
* [snapshot](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/snapshot.md) `[-t tag] [-cf column_family] <keyspace>`  - Take a snapshot of specified keyspaces or a snapshot of the specified table.
* [statusbackup](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/statusbackup.md) - Status of incremental backup.
* [statusbinary](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/statusbinary.md) - Status of native transport (binary protocol).
* [statusgossip](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/statusgossip.md) - Status of gossip.
* [status](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/status.md) - Print cluster information.
* [stop compaction](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/stop.md) - Stop compaction operation.
* **tablehistograms** see [cfhistograms](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/cfhistograms.md)
* [tablestats](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/tablestats.md) - Provides in-depth diagnostics regard table.
* [toppartitions](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/toppartitions.md) - Samples cluster writes and reads and reports the most active partitions in a specified table and time frame.
* [upgradesstables](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/upgradesstables.md) - Upgrades each table that is not running the latest Scylla version, by rewriting SSTables.
* [viewbuildstatus](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/viewbuildstatus.md) - Shows the progress of a materialized view build.
* [version](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/version.md) - Print the DB version.

Copyright

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
