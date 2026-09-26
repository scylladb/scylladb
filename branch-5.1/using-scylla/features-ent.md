# Scylla Enterprise Features


            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Coming Soon to Scylla Enterprise</h5>
            
The following features are scheduled for an **upcoming release** of Scylla Enterprise. To see which release, read the [Release Notes](https://www.scylladb.com/product/release-notes/).

* [LDAP Role Management](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/ldap-authorization.md)
* [LDAP Authentication](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/ldap-authentication.md)

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Enterprise 2022.1 Features</h5>
            
The following are **new features** in ScyllaDB Enterprise 2022.1.x:

* [Virtual Tables](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin-tools/virtual-tables.md) - Tables that retrieve system-level information by generating their contents on-the-fly when queried.
  > * Virtual table for configuration - `system.config`, allows you to query and update configuration over CQL.
  > * Virtual tables for exposing system-level information, such as cluster status, version-related information, etc.

See the [Release Notes](https://www.scylladb.com/product/release-notes/) for more information.

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Enterprise 2021.1 Features</h5>
            
The following are **new features** for Scylla Scylla Enterprise 2021.1.x:

* [Space Amplification Goal (SAG)](https://opensource.docs.scylladb.com/branch-5.1/cql/compaction.md#sag) for ICS -  new CQL option to set a Space Amplification Goal (SAG) in Incremental Compaction Strategy (ICS).
* Scylla Unified Installer - Scylla is now available as an all-in-one binary tar file. You can download the tar file from the [Scylla Download Center](https://www.scylladb.com/download/?platform=tar).
* [Change Data Capture (CDC)](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/cdc/index.md) query the history of all changes made to the table. From *Scylla Enterprise 2021.1.1*

Read the [Release Notes](https://www.scylladb.com/product/release-notes/) for more information.

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Enterprise 2020.1 Features</h5>
            
The following are **new features** for Scylla Scylla Enterprise 2020.1.x:

* [Lightweight Transactions (LWT)](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/lwt.md) - Allows you to create and manipulate data according to a specified condition. [Lightweight Transactions  CQL](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#if) Reference.
* [Scylla Alternator: an Amazon DynamoDB™-compatible API](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/alternator/index.md)
* [Group Results](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#group-by-clause) - using the CQL `GROUP BY` option you can condense into a single row all selected rows that share the same values for a set of columns.
* [Like Operator](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#like-operator) - when used on `SELECT` statements informs Scylla that you are looking for a pattern match. The expression ‘column LIKE pattern’ yields true only if the entire column value matches the pattern.
* [Open range deletions](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#open-range-deletions) -  deletes rows based on an open-ended request (>, <, >=, =<, etc.)
* [Auto-expanding Replication Factor](https://opensource.docs.scylladb.com/branch-5.1/cql/ddl.md#replication-strategy) -  allows you to set a single replication factor for all Data Centers, including all existing Data Centers.
* [Non-Frozen UDTs](https://opensource.docs.scylladb.com/branch-5.1/cql/types.md#udts) - User Defined Types that are not in a collection do not have to be frozen. UDTs in a collection must be frozen.
* [CQL Per Partition Limit](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#limit-clause) - This new per partition limit further allows you to set the number of partitions returned as a result. You can mix both row limits and per partition limits in the same CQL statement.
* [BYPASS CACHE](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#bypass-cache) - This CQL command introduced in Scylla Enterprise 2019.1.1, now available in open source, informs the database that the data being read is unlikely to be read again in the near future, and also was unlikely to have been read in the near past; therefore no attempt should be made to read it from the cache or to populate the cache with the data.

Read the [Release Notes](https://www.scylladb.com/product/release-notes/) for more information.

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Enterprise 2019.1 Features</h5>
            
The following are **new features** for Scylla Scylla Enterprise 2019.1.x:

* [Incremental Compaction Strategy](https://opensource.docs.scylladb.com/branch-5.1/kb/compaction.md#incremental-compaction-strategy-ics) - (version 2019.1.4) - significantly lowers SA (size amplification) for workloads which run STCS and should be used instead of STCS.
* [IPv6](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/admin.md#ipv6-addresses) (version 2019.1.4) support for client-to-node, node-to-node, Manager to node, and Monitoring to node communication - Scylla now supports IPv6 Global Scope Addresses for all IPs: seeds, listen_address, broadcast_address etc. This functionality is available for Scylla Manager in Scylla Manager 2.0.
* [Workload Prioritization](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/workload-prioritization.md) - Grant a level of service to roles in your organization.
* [Scylla Materialized Views](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/materialized-views.md) - An alternate view table for finding a partition by the value of another column.
* [Global Secondary Indexes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/secondary-indexes.md) - A mechanism for allowing efficient searches on non-partition keys using Materialized Views.
* [Local Secondary Indexes](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/local-secondary-indexes.md) - More efficient Secondary Index searches when the base table and index share the same partition key.
* [ALLOW FILTERING CQL Command](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#allow-filtering) that allows for server side data filtering that is not based on the primary key.
* [Hinted Handoff](https://opensource.docs.scylladb.com/branch-5.1/architecture/anti-entropy/hinted-handoff.md) - ensures availability and consistency
* [SSTable 3.0](https://opensource.docs.scylladb.com/branch-5.1/architecture/sstable/sstable3/index.md) - new SSTable format
* [Full (multi-partition)](https://www.scylladb.com/2018/11/01/more-efficient-range-scan-paging-with-scylla-3-0/) blog describing improvements for fulll scans.
* [Role Based Access Control (RBAC)](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/rbac-usecase.md) - compatible with Apache Cassandra 3.x using CQL commands to [grant roles](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md#cql-security) to users in an organization.
* [GoogleCloudSnitch](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/system-configuration/snitch.md#googlecloudsnitch)- optimized for use with GCE instances
* [Large Partitions Support](https://www.scylladb.com/2018/09/11/large-partitions-support-scylla-2-3/) - Scylla supports large partitions.
* [Encryption at Rest](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/encryption-at-rest.md) - protects your data persisted in storage or backup.
* [BYPASS CACHE](https://opensource.docs.scylladb.com/branch-5.1/cql/dml.md#bypass-cache) - This CQL command informs the database that the data being read is unlikely to be read again in the near future, and also was unlikely to have been read in the near past; therefore no attempt should be made to read it from the cache or to populate the cache with the data.

Read the [Release Notes](https://www.scylladb.com/product/release-notes/) for more information.

</div></div>
            <div class="cell my-panel">
                <div class="panel">
                    <h5 class="panel_\_title">Scylla Enterprise 2018.1 Features</h5>
            
The following are **new features** for Scylla Scylla Enterprise 2018.1.x

* [Scylla Auditing Guide](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/auditing.md) -  allows administrators to know which users performed what action at what time.
* [Scylla in-memory tables](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/in-memory.md) - an alternative table type for storing data in RAM.
* [Scylla Counters](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/counters.md) - A data type for counting
* [Time Window Compaction Strategy](https://opensource.docs.scylladb.com/branch-5.1/kb/compaction.md#time-window-compactionstrategy-twcs) - a replacement compaction strategy for Date-Tiered Compaction Strategy, refined for time-series data.
* [Heat Weighted Load Balancing](https://www.scylladb.com/2017/09/21/scylla-heat-weighted-load-balancing/) a blog entry which investigates what happens if one of the nodes loses its cache and the solution Heat Weighted Load Balancing.

Read the [Release Notes](https://www.scylladb.com/product/release-notes/) for more information.

</div></div>
