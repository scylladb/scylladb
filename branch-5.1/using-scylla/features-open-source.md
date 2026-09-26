# Scylla Open Source Features


            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Open Source 5.x Features</h5>
            
The following are **new features** for Scylla Open Source 5.x:

* [Strongly consistent schema management based on the Raft consensus algorithm](https://opensource.docs.scylladb.com/branch-5.1/architecture/raft.md). With
  the implementation of Raft, schema changes in ScyllaDB are safe, including concurrent schema updates.
  This feature is experimental in version 5.0 and needs to be explicitly enabled.
* [Scylla SStable tool](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/scylla-sstable.md) - An admin tool that allows you to examine the content of SStables by performing operations such as dumping the content of SStables, generating a histogram, validating the content of SStables, and more.
* [Scylla Types tool](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/scylla-types.md) - An admin tool that allows you to examine raw values obtained from SStables, logs, coredumps, etc., by printing, validating or comparing the values.
* [Virtual Tables](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/virtual-tables.md) - Tables that retrieve system-level information by generating their contents on-the-fly when queried.
  > * Virtual table for configuration - `system.config`, allows you to query and update configuration over CQL.
  > * Virtual tables for exposing system-level information, such as cluster status, version-related information, etc.
* [Automatic management of tombstone garbage collection](https://opensource.docs.scylladb.com/branch-5.1/cql/ddl.md#ddl-tombstones-gc) -
  An automatic mechanism replaces the `gc_grace_seconds` option to ensure better database consistency
  by removing a tombstone only after the range that covers the tombstone is repaired. This feature is
  experimental in version 5.0 and needs to be explicitly enabled by configuring the `tombstone_gc` option.
* [Expiration service in Scylla Alternator](https://opensource.docs.scylladb.com/branch-5.1/alternator/compatibility.md) - Support
  for DynamoDB’s TTL feature for detecting and deleting expired items in the table. This feature is experimental in version 5.0

See the [Release Notes](https://www.scylladb.com/product/release-notes/) for more information.

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Open Source 4.x Features</h5>
            
The following are **new features** for Scylla Open Source 4.x

* [Lightweight Transactions (LWT)](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/lwt.md) - Allows you to create and manipulate data according to a specified condition. [Lightweight Transactions  CQL](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#if) Reference.
* [Scylla Alternator: an Amazon DynamoDB™-compatible API](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/alternator/index.md)
* [Change Data Capture (CDC)](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/index.md) query the history of all changes made to the table. **GA** in Scylla 4.3. In versions prior to Scylla Open Source 4.3, CDC is an experimental feature and you need to enable it in order to use it.

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Open Source 3.x Features</h5>
            
The following are **new features** for Scylla Open Source 3.3

* [Clients Table](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/clients-table.md) real-time information on CQL clients connected to the Scylla cluster.

The following are **new features** for Scylla Open Source 3.2.x

* [IPv6](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin.md#ipv6-addresses) support for client-to-node, node-to-node, Manager to node, and Monitor to node communication - Scylla now supports IPv6 Global Scope Addresses for all IPs: seeds, listen_address, broadcast_address etc. This functionality is available for Scylla Manager in Scylla Manager 2.0 and in Scylla Monitor in 3.0.
* [Like Operator](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#like-operator) - when used on `SELECT` statements informs Scylla that you are looking for a pattern match. The expression ‘column LIKE pattern’ yields true only if the entire column value matches the pattern.
* [Group Results](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#group-by-clause) - using the CQL `GROUP BY` option you can condense into a single row all selected rows that share the same values for a set of columns.
* [Non-Frozen UDTs](https://opensource.docs.scylladb.com/branch-5.1/cql/types.md#udts) - User Defined Types that are not in a collection do not have to be frozen. UDTs in a collection must be frozen.
* [Auto-expanding Replication Factor](https://opensource.docs.scylladb.com/branch-5.1/cql/ddl.md#replication-strategy) -  allows you to set a single replication factor for all Data Centers, including all existing Data Centers.
* [Open range deletions](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#open-range-deletions) -  deletes rows based on an open-ended request (>, <, >=, =<, etc.)

The following are **new features** for Scylla Open Source 3.1.x

* [CQL Per Partition Limit](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#limit-clause) - This new per partition limit further allows you to set the number of partitions returned as a result. You can mix both row limits and per partition limits in the same CQL statement.
* [Local Secondary Indexes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/local-secondary-indexes.md) - More efficient Secondary Index searches when the base table and index share the same partition key.
* [BYPASS CACHE](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#bypass-cache) - This CQL command introduced in Scylla Enterprise 2019.1.1, now available in open source, informs the database that the data being read is unlikely to be read again in the near future, and also was unlikely to have been read in the near past; therefore no attempt should be made to read it from the cache or to populate the cache with the data.
* [Large Cell / Collection Detector](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/large-rows-large-cells-tables.md) - Makes finding large partitions easier.
* [Nodetool toppartitions](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/toppartitions.md) - Samples cluster writes and reads and reports the most active partitions in a specified table and time frame.
* [Row-level Repair](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/maintenance/repair.md#row-level-repair) - [nodetool repair](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/repair.md) operation now uses row-level repair, adding additional granularity with repairs.
* [ALLOW FILTERING CQL Command](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#allow-filtering) that allows for server side data filtering that is not based on the primary key. Introduced in 3.0, this feature was further enhanced in version 3.1.

The following are **new features** for Scylla Open Source 3.0.x

* [Scylla Materialized Views](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/materialized-views.md) - An alternate view table for finding a partition by the value of another column. Experimental in prior versions, this feature was made production ready in Scylla Open Source 3.0.
* [Global Secondary Indexes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/secondary-indexes.md) - A mechanism for allowing efficient searches on non-partition keys using Materialized Views. Experimental in prior versions, this feature was made production ready in Scylla Open Source 3.0.
* [Hinted Handoff](https://opensource.docs.scylladb.com/branch-5.1/architecture/anti-entropy/hinted-handoff.md) - ensures availability and consistency
* [SSTable 3.0](https://opensource.docs.scylladb.com/branch-5.1/architecture/sstable/sstable3/index.md) - new SSTable format
* [ALLOW FILTERING CQL Command](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#allow-filtering) that allows for server side data filtering that is not based on the primary key.

More information in the [Release Notes](https://www.scylladb.com/product/release-notes/).

</div></div>
