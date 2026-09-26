# ScyllaDB and Apache Cassandra Compatibility

Latest update: ScyllaDB 5.0

ScyllaDB is a drop-in replacement for Apache Cassandra 3.11, with additional features from Apache Cassandra 4.0.
This page contains information about ScyllaDB compatibility with Apache Cassandra.

The tables on this page include information about ScyllaDB Open Source support for Apache Cassandra features.
They do not include the ScyllaDB Enterprise-only features or ScyllaDB-specific features with no match in
Apache Cassandra.  See [ScyllaDB Features](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/features.md) for more information about ScyllaDB features.

## How to Read the Tables on This Page

* <i class="inline-icon icon-check" aria-hidden="true"></i> - Available in ScyllaDB and compatible with Apache Cassandra.
* <i class="inline-icon icon-cancel" aria-hidden="true"></i> - Not available in ScyllaDB.
* **NC** - Available in ScyllaDB, but not compatible with Apache Cassandra.

## Interfaces

| Apache Cassandra Interface     | Version Supported by ScyllaDB                                                                                                                                                                                                                                                              | Comments                                                                                                                                                                                                    |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CQL                            | Fully compatible with version 3.3.1, with additional features from later CQL versions (for example, [Duration type](https://opensource.docs.scylladb.com/branch-5.2/cql/types.md#durations)).<br/><br/><br/>Fully compatible with protocol v4, with additional features from v5.<br/><br/> | More below                                                                                                                                                                                                  |
| Thrift                         | Compatible with Cassandra 2.1                                                                                                                                                                                                                                                              |                                                                                                                                                                                                             |
| SSTable format (all versions)  | 3.11(mc / md / me), 2.2(la), 2.1.8 (ka)                                                                                                                                                                                                                                                    | `me` - supported in ScyllaDB Open Source 5.1 and ScyllaDB Enterprise 2022.2.0 (and later)<br/><br/><br/>`md` - supported in ScyllaDB Open Source 4.3 and ScyllaDB Enterprise 2021.1.0 (and later)<br/><br/> |
| JMX                            | 3.11                                                                                                                                                                                                                                                                                       | More below                                                                                                                                                                                                  |
| Configuration (cassandra.yaml) | 3.11                                                                                                                                                                                                                                                                                       |                                                                                                                                                                                                             |
| Log                            | NC                                                                                                                                                                                                                                                                                         |                                                                                                                                                                                                             |
| Gossip and internal streaming  | NC                                                                                                                                                                                                                                                                                         |                                                                                                                                                                                                             |
| SSL                            | NC                                                                                                                                                                                                                                                                                         |                                                                                                                                                                                                             |

## Supported Tools

The tools are based on Apache Cassandra 3.11.

* [Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool.md) - Scylla commands for managing Scylla node or cluster using the command-line nodetool utility.
* [CQLSh - the CQL shell](https://opensource.docs.scylladb.com/branch-5.2/cql/cqlsh.md).
* [REST - Scylla REST/HTTP Admin API](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/rest.md).
* [Tracing](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/tracing.md) - a ScyllaDB tool for debugging and analyzing internal flows in the server.
* [SSTableloader](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin-tools/sstableloader.md) - Bulk load the sstables found in the directory to a Scylla cluster
* [Scylla SStable](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin-tools/scylla-sstable.md) - Validates and dumps the content of SStables, generates a histogram, dumps the content of the SStable index.
* [Scylla Types](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin-tools/scylla-types.md) - Examines raw values obtained from SStables, logs, coredumps, etc.
* [cassandra-stress](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin-tools/cassandra-stress.md) A tool for benchmarking and load testing a Scylla and Cassandra clusters.
* [SSTabledump - Scylla 3.0, Scylla Enterprise 2019.1 and newer versions](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin-tools/sstabledump.md)
* [SSTable2JSON - Scylla 2.3 and older](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin-tools/sstable2json.md)
* sstablelevelreset - Reset level to 0 on a selected set of SSTables that use LeveledCompactionStrategy (LCS).
* sstablemetadata - Prints metadata about a specified SSTable.
* sstablerepairedset - Mark specific SSTables as repaired or unrepaired.
* configuration_encryptor - [encrypt at rest](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/security/encryption-at-rest.md) sensitive scylla configuration entries using system key.
* local_file_key_generator - Generate a local file (system) key for [encryption at rest](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/security/encryption-at-rest.md), with the provided length, Key algorithm, Algorithm block mode and Algorithm padding method.
* [scyllatop](https://www.scylladb.com/2016/03/22/scyllatop/) - A terminal base top-like tool for scylladb collectd/prometheus metrics.
* [scylla_dev_mode_setup](https://opensource.docs.scylladb.com/branch-5.2/getting-started/install-scylla/dev-mod.md) - run Scylla in Developer Mode.
* [perftune](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin-tools/perftune.md) - performance configuration.

Run each tool with `-h`, `--help` for full options description.

## Features

<a id="consistency-level-read-and-write"></a>

### Consistency Level (read and write)

| Options                  | Support                                                                 |
|--------------------------|-------------------------------------------------------------------------|
| Any (Write Only)         | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| One                      | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| Two                      | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| Three                    | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| Quorum                   | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| All                      | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| Local One                | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| Local Quorum             | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| Each Quorum (Write Only) | <i class="inline-icon icon-check" aria-hidden="true"></i>               |
| SERIAL                   | <i class="inline-icon icon-check" aria-hidden="true"></i> <sub>\*</sub> |
| LOCAL_SERIAL             | <i class="inline-icon icon-check" aria-hidden="true"></i><sub>\*</sub>  |

<sub>\*</sub> From ScyllaDB 4.0. See [Scylla LWT](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/lwt.md).

### Snitches

| Options                                                                                                                                                              | Support                                                    |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| [SimpleSnitch](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/system-configuration/snitch.md#snitch-simple-snitch)                                 | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| [RackInferringSnitch](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/system-configuration/snitch.md#snitch-rack-inferring-snitch)                  | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| PropertyFileSnitch                                                                                                                                                   | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
| [GossipingPropertyFileSnitch](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/system-configuration/snitch.md#snitch-gossiping-property-file-snitch) | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| Dynamic snitching                                                                                                                                                    | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
| [EC2Snitch](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/system-configuration/snitch.md#snitch-ec2-snitch)                                       | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| [EC2MultiRegionSnitch](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/system-configuration/snitch.md#snitch-ec2-multi-region-snitch)               | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| [GoogleCloudSnitch](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/system-configuration/snitch.md#googlecloudsnitch)                               | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| CloudstackSnitch                                                                                                                                                     | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |

### Partitioners

| Options                             | Support                                                                  |
|-------------------------------------|--------------------------------------------------------------------------|
| Murmur3Partitioner (default)        | <i class="inline-icon icon-check" aria-hidden="true"></i>                |
| RandomPartitioner                   | <i class="inline-icon icon-cancel" aria-hidden="true"></i> <sub>\*</sub> |
| OrderPreservingPartitioner          | <i class="inline-icon icon-cancel" aria-hidden="true"></i>               |
| ByteOrderedPartitioner              | <i class="inline-icon icon-cancel" aria-hidden="true"></i> <sub>\*</sub> |
| CollatingOrderPreservingPartitioner | <i class="inline-icon icon-cancel" aria-hidden="true"></i>               |

<sub>\*</sub> Removed in ScyllaDB 4.0

### Protocol Options

| Options                                                                                                                 | Support                                                   |
|-------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| [Encryption](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/security/client-node-encryption.md)       | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| [Authentication](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/security/authentication.md)           | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| [Compression](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin.md#admin-compression)  (see below) | <i class="inline-icon icon-check" aria-hidden="true"></i> |

### Compression

| Options                                                                                                                               | Support                                                   |
|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| CQL Compression                                                                                                                       | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| LZ4                                                                                                                                   | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| Snappy                                                                                                                                | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| [Node to Node Compression](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin.md#internode-compression)           | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| [Client to Node Compression](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin.md#admin-client-node-compression) | <i class="inline-icon icon-check" aria-hidden="true"></i> |

### Backup and Restore

| Options                                                                                                                                              | Support                                                   |
|------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| [Snapshot](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/backup-restore/backup.md#backup-full-backup-snapshots)        | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| [Incremental backup](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/backup-restore/backup.md#backup-incremental-backup) | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| [Restore](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/backup-restore/restore.md)                                     | <i class="inline-icon icon-check" aria-hidden="true"></i> |

### Repair and Consistency

| Options                                                                                                         | Support                                                                |
|-----------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [Nodetool Repair](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/repair.md) | <i class="inline-icon icon-check" aria-hidden="true"></i>              |
| Incremental Repair                                                                                              | <i class="inline-icon icon-cancel" aria-hidden="true"></i>             |
| [Hinted Handoff](https://opensource.docs.scylladb.com/branch-5.2/architecture/anti-entropy/hinted-handoff.md)   | <i class="inline-icon icon-check" aria-hidden="true"></i>              |
| [Lightweight Transactions](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/lwt.md)                 | <i class="inline-icon icon-check" aria-hidden="true"></i><sub>\*</sub> |

<sub>\*</sub> From ScyllaDB 4.0. See [Scylla LWT](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/lwt.md).

### Replica Replacement Strategy

| Options                 | Support                                                   |
|-------------------------|-----------------------------------------------------------|
| SimpleStrategy          | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| NetworkTopologyStrategy | <i class="inline-icon icon-check" aria-hidden="true"></i> |

### Security

| Options                          | Support                                                   |
|----------------------------------|-----------------------------------------------------------|
| Role Based Access Control (RBAC) | <i class="inline-icon icon-check" aria-hidden="true"></i> |

### Indexing and Caching

| Options                                                                                                  | Support                                                                                                                                    |
|----------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| row / key cache                                                                                          | <i class="inline-icon icon-cancel" aria-hidden="true"></i> (More on [Scylla memory and cache](http://www.scylladb.com/technology/memory/)) |
| [Secondary Index](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/secondary-indexes.md)     | <i class="inline-icon icon-check" aria-hidden="true"></i> <sub>\*</sub>                                                                    |
| [Materialized Views](https://opensource.docs.scylladb.com/branch-5.2/using-scylla/materialized-views.md) | <i class="inline-icon icon-check" aria-hidden="true"></i><sub>\*</sub>                                                                     |

<sub>\*</sub> In ScyllaDB Open Source and ScyllaDB Enterprise from 2019.1

### Additional Features

| Feature                | Support                                                                                |
|------------------------|----------------------------------------------------------------------------------------|
| Counters               | <i class="inline-icon icon-check" aria-hidden="true"></i>                              |
| User Defined Types     | <i class="inline-icon icon-check" aria-hidden="true"></i>                              |
| User Defined Functions | <i class="inline-icon icon-cancel" aria-hidden="true"></i> <sub>\*</sub>               |
| Time to live (TTL)     | <i class="inline-icon icon-check" aria-hidden="true"></i>                              |
| Super Column           | <i class="inline-icon icon-cancel" aria-hidden="true"></i>                             |
| vNode Enable           | <i class="inline-icon icon-check" aria-hidden="true"></i> Default                      |
| vNode Disable          | <i class="inline-icon icon-cancel" aria-hidden="true"></i>                             |
| Triggers               | <i class="inline-icon icon-cancel" aria-hidden="true"></i>                             |
| Batch Requests         | <i class="inline-icon icon-check" aria-hidden="true"></i> Includes conditional updates |

<sub>\*</sub>  Experimental

## CQL Command Compatibility

### Create Keyspace

| Feature          | Support                                                                  |
|------------------|--------------------------------------------------------------------------|
| DURABLE_WRITES   | <i class="inline-icon icon-check" aria-hidden="true"></i>                |
| IF NOT EXISTS    | <i class="inline-icon icon-check" aria-hidden="true"></i>                |
| WITH REPLICATION | <i class="inline-icon icon-check" aria-hidden="true"></i>    (see below) |

### Create Keyspace with Replication

| Feature                    | Support                                                    |
|----------------------------|------------------------------------------------------------|
| SimpleStrategy             | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| NetworkTopologyStrategy    | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| OldNetworkTopologyStrategy | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |

### Create Table

| Feature                 | Support                                                   |
|-------------------------|-----------------------------------------------------------|
| Primary key column      | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| Compound primary key    | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| Composite partition key | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| Clustering order        | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| Static column           | <i class="inline-icon icon-check" aria-hidden="true"></i> |

#### Create Table Att

| Feature                     | Support                                                              |
|-----------------------------|----------------------------------------------------------------------|
| bloom_filter_fp_chance      | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| caching                     | <i class="inline-icon icon-cancel" aria-hidden="true"></i> (ignored) |
| comment                     | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| compaction                  | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| compression                 | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| dclocal_read_repair_chance  | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| default_time_to_live        | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| gc_grace_seconds            | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| index_interval              | <i class="inline-icon icon-cancel" aria-hidden="true"></i>           |
| max_index_interval          | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| memtable_flush_period_in_ms | <i class="inline-icon icon-cancel" aria-hidden="true"></i> (ignored) |
| min_index_interval          | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| populate_io_cache_on_flush  | <i class="inline-icon icon-cancel" aria-hidden="true"></i>           |
| read_repair_chance          | <i class="inline-icon icon-check" aria-hidden="true"></i>            |
| replicate_on_write          | <i class="inline-icon icon-cancel" aria-hidden="true"></i>           |
| speculative_retry           | `ALWAYS`, `NONE`                                                     |

#### Create Table Compaction

| Feature                                                                                                       | Support                                                                  |
|---------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [SizeTieredCompactionStrategy](https://opensource.docs.scylladb.com/branch-5.2/cql/compaction.md#stcs) (STCS) | <i class="inline-icon icon-check" aria-hidden="true"></i>                |
| [LeveledCompactionStrategy](https://opensource.docs.scylladb.com/branch-5.2/cql/compaction.md#lcs) (LCS)      | <i class="inline-icon icon-check" aria-hidden="true"></i>                |
| DateTieredCompactionStrategy (DTCS)                                                                           | <i class="inline-icon icon-check" aria-hidden="true"></i>  <sub>\*</sub> |
| [TimeWindowCompactionStrategy](https://opensource.docs.scylladb.com/branch-5.2/cql/compaction.md#twcs) (TWCS) | <i class="inline-icon icon-check" aria-hidden="true"></i>                |

<sub>\*</sub>  Deprecated in ScyllaDB 4.0, use TWCS instead

#### Create Table Compression

| Feature                               | Support                                                    |
|---------------------------------------|------------------------------------------------------------|
| sstable_compression LZ4Compressor     | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| sstable_compression SnappyCompressor  | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| sstable_compression DeflateCompressor | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| chunk_length_kb                       | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| crc_check_chance                      | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |

#### Alter Commands

| Feature        | Support                                                   |
|----------------|-----------------------------------------------------------|
| ALTER KEYSPACE | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| ALTER TABLE    | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| ALTER TYPE     | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| ALTER USER     | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| ALTER ROLE     | <i class="inline-icon icon-check" aria-hidden="true"></i> |

#### Data Manipulation

| Feature             | Support                                                   |
|---------------------|-----------------------------------------------------------|
| BATCH               | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| INSERT              | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| Prepared Statements | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| SELECT              | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| TRUNCATE            | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| UPDATE              | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| USE                 | <i class="inline-icon icon-check" aria-hidden="true"></i> |

#### Create Commands

| Feature        | Support                                                    |
|----------------|------------------------------------------------------------|
| CREATE TRIGGER | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
| CREATE USER    | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| CREATE ROLE    | <i class="inline-icon icon-check" aria-hidden="true"></i>  |

#### Drop Commands

| Feature       | Support                                                    |
|---------------|------------------------------------------------------------|
| DROP KEYSPACE | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| DROP TABLE    | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| DROP TRIGGER  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
| DROP TYPE     | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| DROP USER     | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| DROP ROLE     | <i class="inline-icon icon-check" aria-hidden="true"></i>  |

#### Roles and Permissions

| Feature            | Support                                                   |
|--------------------|-----------------------------------------------------------|
| GRANT PERMISSIONS  | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| GRANT ROLE         | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| LIST PERMISSIONS   | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| LIST USERS         | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| LIST ROLES         | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| REVOKE PERMISSIONS | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| REVOKE ROLE        | <i class="inline-icon icon-check" aria-hidden="true"></i> |

#### Materialized Views

| Feature                  | Support                                                   |
|--------------------------|-----------------------------------------------------------|
| MATERIALIZED VIEW        | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| ALTER MATERIALIZED VIEW  | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| CREATE MATERIALIZED VIEW | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| DROP MATERIALIZED VIEW   | <i class="inline-icon icon-check" aria-hidden="true"></i> |

#### Index commands

| Feature      | Support                                                   |
|--------------|-----------------------------------------------------------|
| INDEX        | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| CREATE INDEX | <i class="inline-icon icon-check" aria-hidden="true"></i> |
| DROP INDEX   | <i class="inline-icon icon-check" aria-hidden="true"></i> |

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
