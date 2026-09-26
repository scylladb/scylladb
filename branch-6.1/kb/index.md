# Knowledge Base


            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Planning and Setup</h5>
            * [ScyllaDB Seed Nodes](https://opensource.docs.scylladb.com/branch-6.1/kb/seed-nodes.md) - Introduction on the purpose and role of Seed Nodes in ScyllaDB as well as configuration tips.
* [Compaction](https://opensource.docs.scylladb.com/branch-6.1/kb/compaction.md) - To free up disk space and speed up reads, ScyllaDB must do compaction operations.
* [DPDK mode](https://opensource.docs.scylladb.com/branch-6.1/kb/dpdk-hardware.md) - Learn to select and configure networking for DPDK mode
* [POSIX networking for ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/kb/posix.md) - ScyllaDB’s POSIX mode works on all physical and virtual network devices and is useful for development work.
* [System Limits](https://opensource.docs.scylladb.com/branch-6.1/kb/system-limits.md) - Outlines the system limits which should be set or removed
* [Run ScyllaDB as a custom user:group](https://opensource.docs.scylladb.com/branch-6.1/kb/custom-user.md) - Configure the ScyllaDB and supporting services to run as a custom user:group.
* [How to Set up a Swap Space Using a File](https://opensource.docs.scylladb.com/branch-6.1/kb/set-up-swap.md) - Outlines the steps you need to take to set up a swap space.

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">ScyllaDB under the hood</h5>
            * [Gossip in ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/kb/gossip.md) - ScyllaDB, like Cassandra, uses a type of protocol called “gossip” to exchange metadata about the identities of nodes in a cluster. Here’s how it works behind the scenes.
* [ScyllaDB consistency quiz for administrators](https://opensource.docs.scylladb.com/branch-6.1/kb/quiz-administrators.md) - How much do you know about NoSQL, from the administrator point of view?
* [ScyllaDB Memory Usage](https://opensource.docs.scylladb.com/branch-6.1/kb/memory-usage.md) - Short explanation how ScyllaDB manages memory
* [ScyllaDB Nodes are Unresponsive](https://opensource.docs.scylladb.com/branch-6.1/kb/unresponsive-nodes.md) - How to handle swap in ScyllaDB
* [CQL Query Does Not Display Entire Result Set](https://opensource.docs.scylladb.com/branch-6.1/kb/cqlsh-more.md) - What to do when a CQL query doesn’t display the entire result set.
* [Snapshots and Disk Utilization](https://opensource.docs.scylladb.com/branch-6.1/kb/disk-utilization.md) - How snapshots affect disk utilization
* [ScyllaDB Snapshots](https://opensource.docs.scylladb.com/branch-6.1/kb/snapshots.md) - What ScyllaDB snapshots are, what they are used for, and how they get created and removed.
* [How does ScyllaDB LWT Differ from Apache Cassandra ?](https://opensource.docs.scylladb.com/branch-6.1/kb/lwt-differences.md) - How does ScyllaDB’s implementation of lightweight transactions differ from Apache Cassandra?
* [If a query does not reveal enough results](https://opensource.docs.scylladb.com/branch-6.1/kb/cqlsh-results.md)
* [How to Change gc_grace_seconds for a Table](https://opensource.docs.scylladb.com/branch-6.1/kb/gc-grace-seconds.md) - How to change the `gc_grace_seconds` parameter and prevent data resurrection.
* [How to flush old tombstones from a table](https://opensource.docs.scylladb.com/branch-6.1/kb/tombstones-flush.md) - How to remove old tombstones from SSTables.
* [Increase Cache to Avoid Non-paged Queries](https://opensource.docs.scylladb.com/branch-6.1/kb/increase-permission-cache.md) - How to increase the `permissions_cache_max_entries` setting.
* [How to Safely Increase the Replication Factor](https://opensource.docs.scylladb.com/branch-6.1/kb/rf-increase.md)
* [Facts about TTL, Compaction, and gc_grace_seconds](https://opensource.docs.scylladb.com/branch-6.1/kb/ttl-facts.md)

**Note**: The KB article for social readers has been *removed*. Instead, please look at lessons on [ScyllaDB University](https://university.scylladb.com/) or the [Care Pet example](https://care-pet.docs.scylladb.com/master/)

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Configuring and Integrating ScyllaDB</h5>
            * [NTP configuration for ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/kb/ntp.md) - ScyllaDB depends on an accurate system clock. Learn to configure NTP for your data store and applications.
* [ScyllaDB and Spark integration](https://opensource.docs.scylladb.com/branch-6.1/kb/scylla-and-spark-integration.md) - How to run an example Spark application that uses ScyllaDB to store data?
* [Map CPUs to ScyllaDB Shards](https://opensource.docs.scylladb.com/branch-6.1/kb/map-cpu.md) - Mapping between CPUs and ScyllaDB shards
* [Customizing CPUSET](https://opensource.docs.scylladb.com/branch-6.1/kb/customizing-cpuset.md)
* [Recreate RAID devices](https://opensource.docs.scylladb.com/branch-6.1/kb/raid-device.md) - How to recreate your RAID devices without running scylla-setup
* [Configure ScyllaDB Networking with Multiple NIC/IP Combinations](https://opensource.docs.scylladb.com/branch-6.1/kb/yaml-address.md) - examples for setting the different IP addresses in scylla.yaml
* [Updating the Mode in perftune.yaml After a ScyllaDB Upgrade](https://opensource.docs.scylladb.com/branch-6.1/kb/perftune-modes-sync.md)
* [Kafka Sink Connector Quickstart](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/integrations/kafka-connector.md)
* [Kafka Sink Connector Configuration](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/integrations/sink-config.md)

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Analyzing ScyllaDB</h5>
            * [Using the perf utility with ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/kb/use-perf.md) - Using the perf utility to analyze ScyllaDB
* [Debug your database with Flame Graphs](https://opensource.docs.scylladb.com/branch-6.1/kb/flamegraph.md) - How to setup and run a Flame Graph
* [Decoding Stack Traces](https://opensource.docs.scylladb.com/branch-6.1/kb/decode-stack-trace.md) - How to decode stack traces in ScyllaDB Logs
* [Counting all rows in a table](https://opensource.docs.scylladb.com/branch-6.1/kb/count-all-rows.md) - Why counting all rows in a table often leads to a timeout

</div></div>
