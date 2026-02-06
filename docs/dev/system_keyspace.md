# System Keyspace Layout

This section describes the layouts and usage of `system.*` tables.

## Table of Contents

- [System Table Categories](#system-table-categories)
- [system."IndexInfo"](#systemindexinfo)
- [system.batchlog](#systembatchlog)
- [system.batchlog_v2](#systembatchlog_v2)
- [system.built_views](#systembuilt_views)
- [system.cdc_generations_v3](#systemcdc_generations_v3)
- [system.cdc_local](#systemcdc_local)
- [system.cdc_streams](#systemcdc_streams)
- [system.cdc_streams_history](#systemcdc_streams_history)
- [system.cdc_streams_state](#systemcdc_streams_state)
- [system.client_routes](#systemclient_routes)
- [system.clients](#systemclients)
- [system.cluster_status](#systemcluster_status)
- [system.commitlog_cleanups](#systemcommitlog_cleanups)
- [system.compaction_history](#systemcompaction_history)
- [system.config](#systemconfig)
- [system.corrupt_data](#systemcorrupt_data)
- [system.dicts](#systemdicts)
- [system.discovery](#systemdiscovery)
- [system.group0_history](#systemgroup0_history)
- [The system.large_* tables](#the-systemlarge_-tables)
- [system.large_cells](#systemlarge_cells)
- [system.large_partitions](#systemlarge_partitions)
- [system.large_rows](#systemlarge_rows)
- [system.load_per_node](#systemload_per_node)
- [system.local](#systemlocal)
- [system.paxos](#systempaxos)
- [system.peers](#systempeers)
- [system.protocol_servers](#systemprotocol_servers)
- [system.raft](#systemraft)
- [system.raft_snapshot_config](#systemraft_snapshot_config)
- [system.raft_snapshots](#systemraft_snapshots)
- [system.raft_state](#systemraft_state)
- [system.repair_history](#systemrepair_history)
- [system.repair_tasks](#systemrepair_tasks)
- [system.role_attributes](#systemrole_attributes)
- [system.role_members](#systemrole_members)
- [system.role_permissions](#systemrole_permissions)
- [system.roles](#systemroles)
- [system.runtime_info](#systemruntime_info)
- [system.scylla_local](#systemscylla_local)
- [system.scylla_table_schema_history](#systemscylla_table_schema_history)
- [system.scylla_views_builds_in_progress](#systemscylla_views_builds_in_progress)
- [system.service_levels_v2](#systemservice_levels_v2)
- [system.size_estimates](#systemsize_estimates)
- [system.snapshots](#systemsnapshots)
- [system.sstables](#systemsstables)
- [system.sstables_registry](#systemsstables_registry)
- [system.tablet_sizes](#systemtablet_sizes)
- [system.tablets](#systemtablets)
- [system.token_ring](#systemtoken_ring)
- [system.topology](#systemtopology)
- [system.topology_requests](#systemtopology_requests)
- [system.truncated](#systemtruncated)
- [system.versions](#systemversions)
- [system.view_build_status_v2](#systemview_build_status_v2)
- [system.view_building_tasks](#systemview_building_tasks)
- [system.views_builds_in_progress](#systemviews_builds_in_progress)

---

## System Table Categories

| Category | System Tables |
|----------|---------------|
| **Core Cluster Information** | [local](#systemlocal), [peers](#systempeers) |
| **Topology and Cluster Management** | [topology](#systemtopology), [topology_requests](#systemtopology_requests), [tablets](#systemtablets), [tablet_sizes](#systemtablet_sizes), [cluster_status](#systemcluster_status), [token_ring](#systemtoken_ring), [load_per_node](#systemload_per_node), [discovery](#systemdiscovery) |
| **Raft Consensus** | [raft](#systemraft), [raft_state](#systemraft_state), [raft_snapshots](#systemraft_snapshots), [raft_snapshot_config](#systemraft_snapshot_config), [group0_history](#systemgroup0_history) |
| **CDC (Change Data Capture)** | [cdc_local](#systemcdc_local), [cdc_streams](#systemcdc_streams), [cdc_streams_history](#systemcdc_streams_history), [cdc_streams_state](#systemcdc_streams_state), [cdc_generations_v3](#systemcdc_generations_v3) |
| **Schema and Metadata** | [scylla_local](#systemscylla_local), [scylla_table_schema_history](#systemscylla_table_schema_history) |
| **Materialized Views** | [views_builds_in_progress](#systemviews_builds_in_progress), [scylla_views_builds_in_progress](#systemscylla_views_builds_in_progress), [built_views](#systembuilt_views), [view_build_status_v2](#systemview_build_status_v2), [view_building_tasks](#systemview_building_tasks) |
| **Lightweight Transactions (Paxos)** | [paxos](#systempaxos) |
| **Compaction** | [compaction_history](#systemcompaction_history) |
| **Repair** | [repair_history](#systemrepair_history), [repair_tasks](#systemrepair_tasks) |
| **Diagnostic and Monitoring** | [large_partitions](#systemlarge_partitions), [large_rows](#systemlarge_rows), [large_cells](#systemlarge_cells), [clients](#systemclients), [client_routes](#systemclient_routes), [protocol_servers](#systemprotocol_servers), [runtime_info](#systemruntime_info) |
| **Data Corruption** | [corrupt_data](#systemcorrupt_data) |
| **Configuration** | [config](#systemconfig), [versions](#systemversions) |
| **Batchlog** | [batchlog](#systembatchlog), [batchlog_v2](#systembatchlog_v2) |
| **Truncation** | [truncated](#systemtruncated) |
| **Commitlog** | [commitlog_cleanups](#systemcommitlog_cleanups) |
| **Service Levels (QoS)** | [service_levels_v2](#systemservice_levels_v2) |
| **Role-Based Access Control (RBAC)** | [roles](#systemroles), [role_members](#systemrole_members), [role_attributes](#systemrole_attributes), [role_permissions](#systemrole_permissions) |
| **Storage** | [snapshots](#systemsnapshots), [size_estimates](#systemsize_estimates), [sstables](#systemsstables), [sstables_registry](#systemsstables_registry), [dicts](#systemdicts) |
| **Legacy/Compatibility** | ["IndexInfo"](#systemindexinfo) |

---

## system."IndexInfo"

Legacy table used for secondary index metadata. This table exists for Cassandra compatibility but is largely unused in modern ScyllaDB versions.

Schema:
```cql
CREATE TABLE system."IndexInfo" (
    table_name text,
    index_name text,
    PRIMARY KEY (table_name, index_name)
) WITH CLUSTERING ORDER BY (index_name ASC);
```

**Note:** This table name requires quoting due to the capital letters.

---

## system.batchlog

Stores batches that have been written locally but not yet replicated to other nodes. Used to ensure batch atomicity across replicas.

Schema:
```cql
CREATE TABLE system.batchlog (
    id uuid PRIMARY KEY,
    data blob,
    version int,
    written_at timestamp
);
```

**Columns:**
- `id`: Unique identifier for the batch
- `data`: Serialized batch mutations
- `version`: Protocol version used to serialize the batch
- `written_at`: Timestamp when the batch was written locally

---

## system.batchlog_v2

Stores batches that have been written locally but not yet replicated to other nodes. This is the newer version of batchlog with improved clustering and sharding. Used to ensure batch atomicity across replicas.

Schema:
```cql
CREATE TABLE system.batchlog_v2 (
    version int,
    stage tinyint,
    shard int,
    written_at timestamp,
    id uuid,
    data blob,
    PRIMARY KEY ((version, stage, shard), written_at, id)
) WITH CLUSTERING ORDER BY (written_at ASC, id ASC);
```

**Columns:**
- `version`: Protocol version (part of composite partition key)
- `stage`: Batch processing stage (part of composite partition key)
- `shard`: Shard number (part of composite partition key)
- `written_at`: Timestamp when the batch was written locally (part of composite clustering key)
- `id`: Unique identifier for the batch (part of composite clustering key)
- `data`: Serialized batch mutations

**Related tables:** [batchlog](#systembatchlog)

---

## system.built_views

Tracks which materialized views have completed their initial build process.

Schema:
```cql
CREATE TABLE system.built_views (
    keyspace_name text,
    view_name text,
    PRIMARY KEY (keyspace_name, view_name)
) WITH CLUSTERING ORDER BY (view_name ASC);
```

**Columns:**
- `keyspace_name`: Name of the keyspace containing the view
- `view_name`: Name of the materialized view

**Related tables:** [views_builds_in_progress](#systemviews_builds_in_progress), [scylla_views_builds_in_progress](#systemscylla_views_builds_in_progress), [view_build_status_v2](#systemview_build_status_v2)

---

## system.cdc_generations_v3

Stores CDC generation data. Each CDC generation defines which streams are active during a particular time period. This is the current version of CDC generation storage, replacing earlier versions.

Schema:
```cql
CREATE TABLE system.cdc_generations_v3 (
    key text,
    id timeuuid,
    range_end bigint,
    streams frozen<set<blob>>,
    ignore_msb tinyint,
    PRIMARY KEY (key, id, range_end)
) WITH CLUSTERING ORDER BY (id ASC, range_end ASC);
```

**Columns:**
- `key`: Partition key (typically `"cdc_generations"`)
- `id`: Unique identifier for this CDC generation
- `range_end`: End of the token range described by this row
- `streams`: Set of stream IDs mapped to this token range
- `ignore_msb`: Number of most significant bits to ignore in token-to-shard mapping (sharding parameter of the node that owned this range when the generation was created)

**How it works:**
This is a single-partition table. Each generation describes a mapping from all tokens in the token ring to stream IDs. The mapping is built from smaller range-local mappings, where each row describes how tokens in a subrange map to stream IDs. The `range_end` is the end of the range for this row; the start is the `range_end` of the previous row (or wraps around for the first row with a given `id`).

**Related tables:** [cdc_local](#systemcdc_local), [cdc_streams](#systemcdc_streams), system_distributed.cdc_timestamps

---

## system.cdc_local

Stores local CDC state information for this node.

Schema:
```cql
CREATE TABLE system.cdc_local (
    key text PRIMARY KEY,
    streams_timestamp timestamp,
    uuid uuid
);
```

**Columns:**
- `key`: Always set to `"cdc_local"`
- `streams_timestamp`: Timestamp of the currently active streams generation on this node
- `uuid`: UUID of the CDC generation (may be null for older generations that only have timestamp)

**Related tables:** [cdc_generations_v3](#systemcdc_generations_v3), [cdc_streams](#systemcdc_streams)

---

## system.cdc_streams

Virtual table containing CDC stream mappings for tablet-based tables. Each stream corresponds to a tablet and is used for distributing CDC log entries.

Schema:
```cql
CREATE TABLE system.cdc_streams (
    keyspace_name text,
    table_name text,
    timestamp timestamp,
    stream_state tinyint,
    stream_id blob,
    PRIMARY KEY ((keyspace_name, table_name), timestamp, stream_state, stream_id)
) WITH CLUSTERING ORDER BY (timestamp ASC, stream_state ASC, stream_id ASC);
```

**Columns:**
- `keyspace_name`: Name of the keyspace
- `table_name`: Name of the table
- `timestamp`: Timestamp identifying the CDC generation
- `stream_state`: State of the stream (0=current, 1=closed, 2=opened)
- `stream_id`: CDC stream identifier

**Related tables:** [cdc_streams_history](#systemcdc_streams_history), [cdc_streams_state](#systemcdc_streams_state), [cdc_generations_v3](#systemcdc_generations_v3)

---

## system.cdc_streams_history

Historical record of CDC stream configurations for tablet-based tables, described as differences from the previous state.

Schema:
```cql
CREATE TABLE system.cdc_streams_history (
    table_id uuid,
    timestamp timestamp,
    stream_state tinyint,
    last_token bigint,
    stream_id blob,
    PRIMARY KEY (table_id, timestamp, stream_state, last_token)
) WITH CLUSTERING ORDER BY (timestamp ASC, stream_state ASC, last_token ASC);
```

**Columns:**
- `table_id`: UUID of the table
- `timestamp`: Timestamp of this stream configuration change
- `stream_state`: State of the stream change (e.g., added, removed)
- `last_token`: Last token of the tablet
- `stream_id`: CDC stream identifier for this tablet

**Related tables:** [cdc_streams](#systemcdc_streams)

---

## system.cdc_streams_state

Tracks the state of CDC streams for tablet-based tables. This is the oldest CDC stream set for tablets.

Schema:
```cql
CREATE TABLE system.cdc_streams_state (
    table_id uuid,
    last_token bigint,
    stream_id blob,
    timestamp timestamp STATIC,
    PRIMARY KEY (table_id, last_token)
) WITH CLUSTERING ORDER BY (last_token ASC);
```

**Columns:**
- `table_id`: UUID of the table
- `last_token`: Last token of the tablet
- `stream_id`: CDC stream identifier for this tablet
- `timestamp`: (static) Timestamp of the CDC stream set

**Related tables:** [cdc_streams](#systemcdc_streams), [cdc_generations_v3](#systemcdc_generations_v3)

**Note:** CDC timestamp tracking is handled by the `system_distributed.cdc_timestamps` table, not in the `system` keyspace.

---

## system.client_routes

Stores client connection routing information for each connection ID.

Schema:
```cql
CREATE TABLE system.client_routes (
    connection_id text,
    host_id uuid,
    address text,
    port int,
    tls_port int,
    alternator_port int,
    alternator_https_port int,
    PRIMARY KEY (connection_id, host_id)
) WITH CLUSTERING ORDER BY (host_id ASC);
```

**Columns:**
- `connection_id`: Unique identifier for the connection
- `host_id`: UUID of the host node (clustering key)
- `address`: IP address of the host
- `port`: CQL port number
- `tls_port`: TLS-enabled CQL port number
- `alternator_port`: Alternator (DynamoDB-compatible) port number
- `alternator_https_port`: HTTPS port for Alternator

---

## system.clients

Virtual table that provides information about currently connected CQL clients.

Schema:
```cql
CREATE TABLE system.clients (
    address inet,
    port int,
    client_type text,
    connection_stage text,
    driver_name text,
    driver_version text,
    hostname text,
    protocol_version int,
    shard_id int,
    ssl_cipher_suite text,
    ssl_enabled boolean,
    ssl_protocol text,
    username text,
    scheduling_group text,
    client_options map<text, text>,
    PRIMARY KEY (address, port, client_type)
) WITH CLUSTERING ORDER BY (port ASC, client_type ASC);
```

**Columns:**
- `address`: Client IP address
- `port`: Client connection port
- `client_type`: Type of client connection (currently only CQL clients are tracked)
- `connection_stage`: Current stage of the connection
- `driver_name`: Name of the client driver being used
- `driver_version`: Version of the client driver
- `hostname`: Client hostname if available
- `protocol_version`: CQL native protocol version used
- `shard_id`: ScyllaDB shard handling this connection
- `ssl_cipher_suite`: SSL cipher suite if SSL is enabled
- `ssl_enabled`: Whether SSL/TLS is enabled for this connection
- `ssl_protocol`: SSL/TLS protocol version
- `username`: Authenticated username
- `scheduling_group`: Workload prioritization group
- `client_options`: Map of additional client options and configuration parameters

**Note:** This is a virtual table. This table was previously persisted to disk (in the data directory) before and including version 4.5.

---

## system.cluster_status

Virtual table containing information about the status of each endpoint in the cluster. Equivalent to the `nodetool status` command.

Schema:
```cql
CREATE TABLE system.cluster_status (
    peer inet PRIMARY KEY,
    dc text,
    up boolean,
    status text,
    load text,
    tokens int,
    owns float,
    host_id uuid
);
```

**Columns:**
- `peer`: IP address of the node
- `dc`: Data center name
- `up`: Whether the node is currently up
- `status`: Node status (e.g., `UN` for Up/Normal)
- `load`: Amount of data stored on the node
- `tokens`: Number of tokens owned
- `owns`: Percentage of the ring owned by this node
- `host_id`: Unique identifier for the node

Implemented by `cluster_status_table` in `db/system_keyspace.cc`.

---

## system.commitlog_cleanups

Tracks commitlog cleanup operations, used to ensure durability guarantees are met.

Schema:
```cql
CREATE TABLE system.commitlog_cleanups (
    shard int,
    position frozen<tuple<bigint, int>>,
    table_uuid uuid,
    start_token_exclusive bigint,
    end_token_inclusive bigint,
    PRIMARY KEY (shard, position, table_uuid, start_token_exclusive, end_token_inclusive)
) WITH CLUSTERING ORDER BY (position ASC, table_uuid ASC, start_token_exclusive ASC, end_token_inclusive ASC);
```

**Columns:**
- `shard`: The shard (CPU) that performed the cleanup
- `position`: Position in the commitlog (tuple of segment ID and position within segment)
- `table_uuid`: UUID of the table being cleaned up
- `start_token_exclusive`: Start of the token range (exclusive)
- `end_token_inclusive`: End of the token range (inclusive)

---

## system.compaction_history

Records historical information about compaction operations.

Schema:
```cql
CREATE TABLE system.compaction_history (
    id uuid PRIMARY KEY,
    bytes_in bigint,
    bytes_out bigint,
    columnfamily_name text,
    started_at timestamp,
    compacted_at timestamp,
    compaction_type text,
    keyspace_name text,
    rows_merged map<int, bigint>,
    shard_id int,
    sstables_in list<frozen<sstableinfo>>,
    sstables_out list<frozen<sstableinfo>>,
    total_tombstone_purge_attempt bigint,
    total_tombstone_purge_failure_due_to_overlapping_with_memtable bigint,
    total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable bigint
) WITH default_time_to_live = 604800;
```

**User-Defined Type:**
```cql
CREATE TYPE system.sstableinfo (
    generation int,
    level int,
    size bigint,
    version text
);
```

**Columns:**
- `id`: Unique identifier for the compaction
- `bytes_in`: Total bytes read during compaction
- `bytes_out`: Total bytes written during compaction
- `columnfamily_name`: Table that was compacted
- `started_at`: Timestamp when compaction started
- `compacted_at`: Timestamp when compaction completed
- `compaction_type`: Type of compaction (e.g., `COMPACTION`, `MAJOR`, `RESHAPE`)
- `keyspace_name`: Keyspace containing the compacted table
- `rows_merged`: Map showing how many rows were merged at each level
- `shard_id`: Shard that performed the compaction
- `sstables_in`: List of input SSTables
- `sstables_out`: List of output SSTables
- `total_tombstone_purge_attempt`: Number of tombstones attempted to be purged
- `total_tombstone_purge_failure_due_to_overlapping_with_memtable`: Tombstones not purged due to memtable overlap
- `total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable`: Tombstones not purged due to SSTable overlap

**Note:** Entries in this table have a TTL of 7 days (604800 seconds).

## system.config

Virtual table holding all configuration variables currently in use. Values can be modified at runtime using the `UPDATE` statement for configuration options that support live updates.

Schema:
```cql
CREATE TABLE system.config (
    name text PRIMARY KEY,
    source text,
    type text,
    value text
);
```

**Columns:**
- `name`: Name of the configuration parameter
- `source`: Origin of the configuration value, one of:
  - `default`: The default value (not explicitly configured)
  - `config`: Set via configuration file
  - `cli`: Set via command-line argument
  - `cql`: Set via updating this table
  - `internal`: Deliberately configured by ScyllaDB internals
- `type`: Data type of the configuration parameter (e.g., `string`, `bool`, `integer`)
- `value`: Current value in JSON format (as it would appear in the config file)

**Usage:**
```cql
-- View a specific setting
SELECT * FROM system.config WHERE name = 'compaction_throughput_mb_per_sec';

-- Update a live-updateable setting
UPDATE system.config SET value = '64' WHERE name = 'compaction_throughput_mb_per_sec';
```

**Related tables:** [versions](#systemversions)

---

## system.corrupt_data

Stores data found to be corrupt during internal operations. This data cannot be written to SSTables because it would be spread around by repair and compaction and might cause failures in SSTable parsing. The table preserves corrupt data so it can be inspected and possibly restored by the database operator.

Schema:
```cql
CREATE TABLE system.corrupt_data (
    keyspace_name text,
    table_name text,
    id timeuuid,
    partition_key blob,
    clustering_key text,
    mutation_fragment_kind text,
    frozen_mutation_fragment blob,
    origin text,
    sstable_name text,
    PRIMARY KEY ((keyspace_name, table_name), id)
) WITH CLUSTERING ORDER BY (id ASC)
    AND gc_grace_seconds = 0;
```

**Columns:**
- `keyspace_name`: Keyspace name of the source table
- `table_name`: Table name of the source table
- `id`: Unique identifier assigned when the corrupt data entry is created
- `partition_key`: Partition key of the partition in the source table (can be incomplete or null due to corruption)
- `clustering_key`: Clustering key of the mutation fragment (can be null for some mutation fragment kinds, or incomplete due to corruption)
- `mutation_fragment_kind`: Kind of the mutation fragment, one of:
  - `partition start`
  - `partition end`
  - `static row`
  - `clustering row`
  - `range tombstone change` (only `clustering row` and `range tombstone change` can have `clustering_key` set)
- `frozen_mutation_fragment`: The serialized mutation fragment itself
- `origin`: The name of the process that found the corruption (e.g., `sstable-writer`)
- `sstable_name`: The name of the SSTable that contains the corrupt data, if known (SSTable may have been compacted or deleted)

---

## system.dicts

Stores shared compression dictionaries used for inter-node traffic compression when `internode_compression_enable_advanced` is enabled.

Schema:
```cql
CREATE TABLE system.dicts (
    name text PRIMARY KEY,
    timestamp timestamp,
    origin uuid,
    data blob
);
```

**Columns:**
- `name`: Unique name/identifier for the dictionary
- `timestamp`: When the dictionary was created
- `origin`: UUID of the node that created the dictionary
- `data`: Compressed dictionary data

**Note:** Dictionary training can leak unencrypted data to disk. See `internode_compression_train_dictionaries` configuration option.

---

## system.discovery

Used during cluster discovery to find peer nodes. This table is part of the Raft-based topology management.

Schema:
```cql
CREATE TABLE system.discovery (
    key text,
    ip_addr inet,
    raft_server_id uuid,
    PRIMARY KEY (key, ip_addr)
) WITH CLUSTERING ORDER BY (ip_addr ASC);
```

**Columns:**
- `key`: Partition key (typically `"peers"`)
- `ip_addr`: IP address of a discovered peer (clustering column)
- `raft_server_id`: The ID of the group 0 Raft server on that peer (may be UUID 0 if unknown during discovery)

**Related tables:** [topology](#systemtopology)

---

## system.group0_history

Records the history of Raft Group 0 state changes. Group 0 is the central Raft group used for topology and schema coordination.

Schema:
```cql
CREATE TABLE system.group0_history (
    key text,
    state_id timeuuid,
    description text,
    PRIMARY KEY (key, state_id)
) WITH CLUSTERING ORDER BY (state_id DESC);
```

**Columns:**
- `key`: Partition key (typically `"history"`)
- `state_id`: Identifier for this state change (in descending order)
- `description`: Human-readable description of the state change

**Related tables:** [raft](#systemraft), [raft_state](#systemraft_state)

---

## The `system.large_*` tables

ScyllaDB performs better when partitions, rows, and cells are not too large. To help diagnose cases where these grow too large, ScyllaDB keeps three tables that record large partitions (including those with too many rows), rows, and cells, respectively.

The meaning of an entry in each of these tables is similar. It means that there is a particular SSTable with a large partition, row, cell, or a partition with too many rows. In particular, this implies that:

* There is no entry until compaction aggregates enough data in a single SSTable.
* The entry stays around until the SSTable is deleted.

In addition, the entries also have a TTL of 30 days.

---

## system.large_cells

Tracks large cells detected in SSTables. Used to help diagnose performance issues caused by oversized cells.

Schema:
```cql
CREATE TABLE system.large_cells (
    keyspace_name text,
    table_name text,
    sstable_name text,
    cell_size bigint,
    partition_key text,
    clustering_key text,
    column_name text,
    compaction_time timestamp,
    collection_elements bigint,
    PRIMARY KEY ((keyspace_name, table_name), sstable_name, cell_size, partition_key, clustering_key, column_name)
) WITH CLUSTERING ORDER BY (sstable_name ASC, cell_size DESC, partition_key ASC, clustering_key ASC, column_name ASC);
```

**Columns:**
- `keyspace_name`: Keyspace containing the large cell
- `table_name`: Table containing the large cell
- `sstable_name`: SSTable where the cell was detected
- `cell_size`: Size of the cell in bytes
- `partition_key`: Partition key of the row
- `clustering_key`: Clustering key of the row
- `column_name`: Name of the column containing the large cell
- `compaction_time`: When the large cell was detected during compaction
- `collection_elements`: Number of elements in the collection (if the cell contains a collection type)

**Note:** A collection is just one cell. There is no information about the size of each collection element. This table is currently only used with the MC SSTable format (issue #4868). Entries have a TTL of 30 days.

**Related tables:** [large_partitions](#systemlarge_partitions), [large_rows](#systemlarge_rows)

### Example usage

```cql
-- View all large cells
SELECT * FROM system.large_cells;

-- View large cells for a specific table
SELECT * FROM system.large_cells WHERE keyspace_name = 'ks1' AND table_name = 'standard1';
```

---

## system.large_partitions

Tracks large partitions detected in SSTables, including partitions with too many rows. ScyllaDB performs better if partitions are not too large.

Schema:
```cql
CREATE TABLE system.large_partitions (
    keyspace_name text,
    table_name text,
    sstable_name text,
    partition_size bigint,
    partition_key text,
    range_tombstones bigint,
    dead_rows bigint,
    rows bigint,
    compaction_time timestamp,
    PRIMARY KEY ((keyspace_name, table_name), sstable_name, partition_size, partition_key)
) WITH CLUSTERING ORDER BY (sstable_name ASC, partition_size DESC, partition_key ASC);
```

**Columns:**
- `keyspace_name`: Keyspace containing the large partition
- `table_name`: Table containing the large partition
- `sstable_name`: SSTable where the partition was detected
- `partition_size`: Size of the partition in bytes
- `partition_key`: The partition key value
- `range_tombstones`: Number of range tombstones in the partition
- `dead_rows`: Number of tombstoned (deleted) rows
- `rows`: Total number of rows in the partition
- `compaction_time`: When the large partition was detected during compaction

**Important notes:**
- There is no entry until compaction aggregates enough data in a single SSTable
- The entry stays around until the SSTable is deleted
- Entries have a TTL of 30 days

**Related tables:** [large_rows](#systemlarge_rows), [large_cells](#systemlarge_cells)

### Example usage

```cql
-- View all large partitions
SELECT * FROM system.large_partitions;

-- View large partitions for a specific table
SELECT * FROM system.large_partitions WHERE keyspace_name = 'ks1' AND table_name = 'standard1';
```

---

## system.large_rows

Tracks large clustering rows and static rows detected in SSTables.

Schema:
```cql
CREATE TABLE system.large_rows (
    keyspace_name text,
    table_name text,
    sstable_name text,
    row_size bigint,
    partition_key text,
    clustering_key text,
    compaction_time timestamp,
    PRIMARY KEY ((keyspace_name, table_name), sstable_name, row_size, partition_key, clustering_key)
) WITH CLUSTERING ORDER BY (sstable_name ASC, row_size DESC, partition_key ASC, clustering_key ASC);
```

**Columns:**
- `keyspace_name`: Keyspace containing the large row
- `table_name`: Table containing the large row
- `sstable_name`: SSTable where the row was detected
- `row_size`: Size of the row in bytes
- `partition_key`: Partition key of the row
- `clustering_key`: Clustering key of the row (empty for static rows)
- `compaction_time`: When the large row was detected during compaction

**Note:** This table is currently only used with the MC SSTable format (issue #4868). Entries have a TTL of 30 days.

**Related tables:** [large_partitions](#systemlarge_partitions), [large_cells](#systemlarge_cells)

### Example usage

```cql
-- View all large rows
SELECT * FROM system.large_rows;

-- View large rows for a specific table
SELECT * FROM system.large_rows WHERE keyspace_name = 'ks1' AND table_name = 'standard1';
```

---

## system.load_per_node

Virtual table containing information about the current tablet load with node granularity. Can be queried on any node, but the data comes from the group0 leader. Reads wait for group0 leader to be elected and load balancer stats to become available.

Schema:
```cql
CREATE TABLE system.load_per_node (
    node uuid PRIMARY KEY,
    dc text,
    rack text,
    ip inet,
    up boolean,
    tablets_allocated bigint,
    tablets_allocated_per_shard double,
    storage_capacity bigint,
    storage_allocated_load bigint,
    storage_allocated_utilization double,
    storage_load bigint,
    storage_utilization double
);
```

**Columns:**
- `node`: Host ID of the node
- `dc`: Data center name
- `rack`: Rack name
- `ip`: IP address of the node
- `up`: Whether the node is alive according to gossiper
- `tablets_allocated`: Number of tablet replicas on the node (migrating tablets are counted as if migration already finished)
- `tablets_allocated_per_shard`: `tablets_allocated` divided by shard count on the node
- `storage_capacity`: Total disk capacity in bytes (by default equals file system capacity)
- `storage_allocated_load`: Estimated disk space for tablets based on the configured `target_tablet_size` (not actual current size)
- `storage_allocated_utilization`: Fraction of node's disk capacity used for `storage_allocated_load` (1.0 means full utilization)
- `storage_load`: Disk space allocated for tablets, computed with actual tablet sizes. Can be null if some of the tablet sizes are not known
- `storage_utilization`: Fraction of node's disk capacity taken for `storage_load` (with actual tablet sizes), where 1.0 means full utilization. Can be null if some of the tablet sizes are not known

**Related tables:** [tablets](#systemtablets), [tablet_sizes](#systemtablet_sizes)

---

## system.local

Stores information about the local node. This is one of the most important system tables, containing essential cluster and node metadata.

Schema:
```cql
CREATE TABLE system.local (
    key text PRIMARY KEY,
    bootstrapped text,
    broadcast_address inet,
    cluster_name text,
    cql_version text,
    data_center text,
    gossip_generation int,
    host_id uuid,
    listen_address inet,
    native_protocol_version text,
    partitioner text,
    rack text,
    release_version text,
    rpc_address inet,
    schema_version uuid,
    supported_features text,
    tokens set<text>,
    truncated_at map<uuid, blob>
);
```

**Key Columns:**
- `key`: Always set to `"local"`
- `bootstrapped`: Bootstrap status (`COMPLETED`, `IN_PROGRESS`, `NEEDS_BOOTSTRAP`)
- `broadcast_address`: Address this node broadcasts to other nodes
- `cluster_name`: Name of the cluster
- `cql_version`: CQL version supported
- `data_center`: Data center this node belongs to
- `host_id`: Unique identifier for this node
- `native_protocol_version`: Native CQL protocol version
- `partitioner`: Partitioner class being used
- `rack`: Rack this node belongs to
- `release_version`: ScyllaDB release version
- `schema_version`: Current schema version UUID
- `tokens`: Set of tokens owned by this node
- `truncated_at`: Legacy truncation records (see [truncated](#systemtruncated) for current storage)

**Related tables:** [peers](#systempeers), [scylla_local](#systemscylla_local)

---

## system.paxos

Stores state for Paxos-based lightweight transactions (LWT). Each row represents the Paxos state for a particular partition in a user table.

Schema:
```cql
CREATE TABLE system.paxos (
    row_key blob,
    cf_id uuid,
    promise timeuuid,
    most_recent_commit blob,
    most_recent_commit_at timeuuid,
    proposal blob,
    proposal_ballot timeuuid,
    PRIMARY KEY (row_key, cf_id)
) WITH CLUSTERING ORDER BY (cf_id ASC);
```

**Columns:**
- `row_key`: Partition key of the user table row
- `cf_id`: UUID of the user table
- `promise`: Ballot of the most recent promise (previously named `in_progress_ballot` in older versions)
- `most_recent_commit`: The most recently committed value (serialized frozen_mutation)
- `most_recent_commit_at`: Ballot of the most recent commit
- `proposal`: The current proposal value (serialized frozen_mutation)
- `proposal_ballot`: Ballot of the current proposal

---

## system.peers

Stores information about other nodes in the cluster as seen by this node.

Schema:
```cql
CREATE TABLE system.peers (
    peer inet PRIMARY KEY,
    data_center text,
    host_id uuid,
    preferred_ip inet,
    rack text,
    release_version text,
    rpc_address inet,
    schema_version uuid,
    supported_features text,
    tokens set<text>
);
```

**Columns:**
- `peer`: IP address of the peer node (primary key)
- `data_center`: Data center of the peer
- `host_id`: Unique identifier for the peer node
- `preferred_ip`: Preferred IP address for inter-node communication
- `rack`: Rack of the peer
- `release_version`: ScyllaDB version running on the peer
- `rpc_address`: Address for client RPC connections
- `schema_version`: Schema version on the peer
- `supported_features`: Features supported by this peer
- `tokens`: Set of tokens owned by this peer

**Related tables:** [local](#systemlocal)

---

## system.protocol_servers

Virtual table listing all client-facing data-plane protocol servers and their listen addresses. Equivalent of `nodetool statusbinary` plus the "Native Transport active" fields from `nodetool info`.

Schema:
```cql
CREATE TABLE system.protocol_servers (
    name text PRIMARY KEY,
    listen_addresses frozen<list<text>>,
    protocol text,
    protocol_version text
);
```

**Columns:**
- `name`: Name/alias of the server (e.g., `"native"` for the CQL server)
- `listen_addresses`: List of addresses this server listens on
- `protocol`: Name of the protocol this server serves (e.g., `"cql"`)
- `protocol_version`: Version of the protocol this server understands

Implemented by `protocol_servers_table` in `db/system_keyspace.cc`.

---

## system.raft

Core Raft consensus state table. Holds information about Raft groups including the log entries.

Schema:
```cql
CREATE TABLE system.raft (
    group_id timeuuid,
    index bigint,
    term bigint,
    data blob,
    vote_term bigint STATIC,
    vote uuid STATIC,
    snapshot_id uuid STATIC,
    commit_idx bigint STATIC,
    PRIMARY KEY (group_id, index)
) WITH CLUSTERING ORDER BY (index ASC);
```

**Columns:**
- `group_id`: Identifier for the Raft group
- `index`: Log entry index
- `term`: Raft term for this log entry
- `data`: Serialized log entry data
- `vote_term`: (static) Term of the last vote
- `vote`: (static) Server ID that received the vote
- `snapshot_id`: (static) ID of the current snapshot
- `commit_idx`: (static) Index of the last committed entry

**Related tables:** [raft_state](#systemraft_state), [raft_snapshots](#systemraft_snapshots), [raft_snapshot_config](#systemraft_snapshot_config), [group0_history](#systemgroup0_history)

---

## system.raft_snapshot_config

Stores configuration for Raft snapshots.

Schema:
```cql
CREATE TABLE system.raft_snapshot_config (
    group_id timeuuid,
    disposition text,
    server_id uuid,
    can_vote boolean,
    PRIMARY KEY (group_id, disposition, server_id)
) WITH CLUSTERING ORDER BY (disposition ASC, server_id ASC);
```

**Columns:**
- `group_id`: Identifier for the Raft group
- `disposition`: Configuration disposition (can be `CURRENT` or `PREVIOUS`)
- `server_id`: ID of the server in the snapshot configuration
- `can_vote`: Whether this server can vote in the Raft group

**Related tables:** [raft](#systemraft), [raft_snapshots](#systemraft_snapshots)

---

## system.raft_snapshots

Stores Raft snapshot data.

Schema:
```cql
CREATE TABLE system.raft_snapshots (
    group_id timeuuid PRIMARY KEY,
    snapshot_id uuid,
    idx bigint,
    term bigint
);
```

**Columns:**
- `group_id`: Identifier for the Raft group
- `snapshot_id`: Snapshot identifier
- `idx`: Log index of the snapshot
- `term`: Raft term of the snapshot

**Related tables:** [raft](#systemraft), [raft_snapshot_config](#systemraft_snapshot_config)

---

## system.raft_state

Tracks the state of Raft groups.

Schema:
```cql
CREATE TABLE system.raft_state (
    group_id timeuuid,
    disposition text,
    server_id uuid,
    can_vote boolean,
    PRIMARY KEY (group_id, disposition, server_id)
) WITH CLUSTERING ORDER BY (disposition ASC, server_id ASC);
```

**Columns:**
- `group_id`: Identifier for the Raft group
- `disposition`: Current disposition/state of the configuration ("CURRENT" or "PREVIOUS")
- `server_id`: ID of the server in this Raft group
- `can_vote`: Whether this server can vote in this configuration

**Related tables:** [raft](#systemraft), [group0_history](#systemgroup0_history)

---

## system.repair_history

Records the history of repair operations.

Schema:
```cql
CREATE TABLE system.repair_history (
    table_uuid uuid,
    repair_time timestamp,
    repair_uuid uuid,
    range_start bigint,
    range_end bigint,
    keyspace_name text STATIC,
    table_name text STATIC,
    PRIMARY KEY (table_uuid, repair_time, repair_uuid, range_start, range_end)
) WITH CLUSTERING ORDER BY (repair_time DESC, repair_uuid ASC, range_start ASC, range_end ASC);
```

**Columns:**
- `table_uuid`: UUID of the repaired table
- `repair_time`: When the repair was completed
- `repair_uuid`: Unique identifier for the repair operation
- `range_start`: Start of the repaired token range (as bigint)
- `range_end`: End of the repaired token range (as bigint)
- `keyspace_name`: (static) Keyspace containing the repaired table
- `table_name`: (static) Name of the repaired table

---

## system.repair_tasks

Records tablet repair tasks and their progress.

Schema:
```cql
CREATE TABLE system.repair_tasks (
    task_uuid uuid,
    operation text,
    first_token bigint,
    last_token bigint,
    timestamp timestamp,
    table_uuid uuid STATIC,
    PRIMARY KEY (task_uuid, operation, first_token, last_token)
) WITH CLUSTERING ORDER BY (operation ASC, first_token ASC, last_token ASC);
```

**Columns:**
- `task_uuid`: Unique identifier for the repair task
- `operation`: Type of repair operation (clustering key)
- `first_token`: First token of the tablet (clustering key)
- `last_token`: Last token of the tablet (clustering key)
- `timestamp`: When the repair task was recorded
- `table_uuid`: (static) UUID of the table being repaired

---

## system.role_attributes

Stores additional attributes for roles (e.g., service level associations).

Schema:
```cql
CREATE TABLE system.role_attributes (
    role text,
    name text,
    value text,
    PRIMARY KEY (role, name)
) WITH CLUSTERING ORDER BY (name ASC);
```

**Columns:**
- `role`: Name of the role
- `name`: Attribute name
- `value`: Attribute value

**Related tables:** [roles](#systemroles), [role_members](#systemrole_members), [role_permissions](#systemrole_permissions)

---

## system.role_members

Stores role membership information (which roles are members of other roles).

Schema:
```cql
CREATE TABLE system.role_members (
    role text,
    member text,
    PRIMARY KEY (role, member)
) WITH CLUSTERING ORDER BY (member ASC);
```

**Columns:**
- `role`: Name of the parent role
- `member`: Name of the member role

**Related tables:** [roles](#systemroles), [role_attributes](#systemrole_attributes), [role_permissions](#systemrole_permissions)

---

## system.role_permissions

Stores permissions granted to roles.

Schema:
```cql
CREATE TABLE system.role_permissions (
    role text,
    resource text,
    permissions set<text>,
    PRIMARY KEY (role, resource)
) WITH CLUSTERING ORDER BY (resource ASC);
```

**Columns:**
- `role`: Name of the role
- `resource`: Resource the permissions apply to (e.g., `data/keyspace/table`)
- `permissions`: Set of permissions granted (e.g., `SELECT`, `MODIFY`, `CREATE`)

**Related tables:** [roles](#systemroles), [role_members](#systemrole_members), [role_attributes](#systemrole_attributes)

---

## system.roles

Stores role definitions for authentication and authorization.

Schema:
```cql
CREATE TABLE system.roles (
    role text PRIMARY KEY,
    can_login boolean,
    is_superuser boolean,
    member_of set<text>,
    salted_hash text
);
```

**Columns:**
- `role`: Name of the role (primary key)
- `can_login`: Whether this role can be used for authentication
- `is_superuser`: Whether this role has superuser privileges
- `member_of`: Set of roles this role is a member of
- `salted_hash`: Hashed password for authentication

**Related tables:** [role_members](#systemrole_members), [role_permissions](#systemrole_permissions), [role_attributes](#systemrole_attributes)

---

## system.runtime_info

Virtual table containing runtime-specific information such as memory stats, memtable stats, cache stats, and more. Data is grouped so that related items stay together and are easily queried. Roughly equivalent to `nodetool info`, `nodetool gettraceprobability`, and `nodetool statusgossip` commands.

Schema:
```cql
CREATE TABLE system.runtime_info (
    group text,
    item text,
    value text,
    PRIMARY KEY (group, item)
) WITH CLUSTERING ORDER BY (item ASC);
```

**Columns:**
- `group`: Category/group of the runtime information
- `item`: Name of the metric or setting
- `value`: Current value

Implemented by `runtime_info_table` in `db/system_keyspace.cc`.

---

## system.scylla_local

Stores ScyllaDB-specific local node information that extends beyond the Cassandra-compatible `system.local` table.

Schema:
```cql
CREATE TABLE system.scylla_local (
    key text PRIMARY KEY,
    value text
);
```

**Columns:**
- `key`: Setting or property name
- `value`: The value (typically JSON or plain text)

**Common keys:**
- `cluster_supports_upgrading_from_version`: Minimum version the cluster supports upgrade from
- `enabled_features`: Features enabled on this node
- `raft_group0_id`: ID of the Raft Group 0
- `topology_state_machine_version`: Version of the topology state machine

**Related tables:** [local](#systemlocal)

---

## system.scylla_table_schema_history

Stores the history of schema changes for tables. Used for schema management and debugging.

Schema:
```cql
CREATE TABLE system.scylla_table_schema_history (
    cf_id uuid,
    schema_version uuid,
    column_name text,
    clustering_order text,
    column_name_bytes blob,
    kind text,
    position int,
    type text,
    PRIMARY KEY (cf_id, schema_version, column_name)
) WITH CLUSTERING ORDER BY (schema_version ASC, column_name ASC);
```

**Columns:**
- `cf_id`: UUID of the table (column family ID)
- `schema_version`: Schema version when this column definition was active
- `column_name`: Name of the column
- `clustering_order`: Clustering order for clustering columns (`ASC` or `DESC`)
- `column_name_bytes`: Column name as raw bytes
- `kind`: Column kind (`partition_key`, `clustering`, `regular`, `static`)
- `position`: Position of the column within its kind
- `type`: CQL type of the column

---

## system.scylla_views_builds_in_progress

ScyllaDB-specific table tracking materialized view build progress, extending the Cassandra-compatible version.

Schema:
```cql
CREATE TABLE system.scylla_views_builds_in_progress (
    keyspace_name text,
    view_name text,
    cpu_id int,
    next_token text,
    generation_number int,
    first_token text,
    PRIMARY KEY (keyspace_name, view_name, cpu_id)
) WITH CLUSTERING ORDER BY (view_name ASC, cpu_id ASC);
```

**Columns:**
- `keyspace_name`: Keyspace containing the view
- `view_name`: Name of the materialized view
- `cpu_id`: Shard (CPU) that owns this portion of the build
- `next_token`: Next token to process
- `generation_number`: Build generation number
- `first_token`: First token in the range being built

**Related tables:** [views_builds_in_progress](#systemviews_builds_in_progress), [built_views](#systembuilt_views)

---

## system.service_levels_v2

Stores service level definitions for Quality of Service (QoS) workload prioritization.

Schema:
```cql
CREATE TABLE system.service_levels_v2 (
    service_level text PRIMARY KEY,
    timeout duration,
    workload_type text,
    shares int
);
```

**Columns:**
- `service_level`: Name of the service level
- `timeout`: Default timeout for operations in this service level
- `workload_type`: Type of workload (e.g., `batch`, `interactive`)
- `shares`: CPU shares allocated to this service level for scheduling priority

---

## system.size_estimates

Virtual table containing size estimates for individual token ranges of each keyspace/table. Useful for capacity planning and understanding data distribution.

Schema:
```cql
CREATE TABLE system.size_estimates (
    keyspace_name text,
    table_name text,
    range_start text,
    range_end text,
    mean_partition_size bigint,
    partitions_count bigint,
    PRIMARY KEY (keyspace_name, table_name, range_start, range_end)
) WITH CLUSTERING ORDER BY (table_name ASC, range_start ASC, range_end ASC);
```

**Columns:**
- `keyspace_name`: Name of the keyspace
- `table_name`: Name of the table
- `range_start`: Start of the token range
- `range_end`: End of the token range
- `mean_partition_size`: Average partition size in bytes for this range (bigint)
- `partitions_count`: Estimated number of partitions in this range (bigint)

Implemented by `size_estimates_mutation_reader` in `db/size_estimates_virtual_reader.{hh,cc}`.

---

## system.snapshots

Virtual table listing snapshots on the node. Equivalent to the `nodetool listsnapshots` command.

Schema:
```cql
CREATE TABLE system.snapshots (
    keyspace_name text,
    table_name text,
    snapshot_name text,
    live bigint,
    total bigint,
    PRIMARY KEY (keyspace_name, table_name, snapshot_name)
) WITH CLUSTERING ORDER BY (table_name ASC, snapshot_name ASC);
```

**Columns:**
- `keyspace_name`: Keyspace of the snapshot
- `table_name`: Table of the snapshot
- `snapshot_name`: Name of the snapshot
- `live`: Live data size in bytes (data that isn't also in the current data files)
- `total`: Total size of snapshot files in bytes

Implemented by `snapshots_table` in `db/system_keyspace.cc`.

---

## system.sstables

The "ownership" table for non-local SSTables when using object storage (S3).

Schema:
```cql
CREATE TABLE system.sstables (
    owner uuid,
    generation timeuuid,
    status text,
    state text,
    version text,
    format text,
    PRIMARY KEY (owner, generation)
) WITH CLUSTERING ORDER BY (generation ASC);
```

**Columns:**
- `owner`: UUID of the owning table
- `generation`: SSTable generation identifier
- `status`: Current status of the SSTable
- `state`: State of the SSTable
- `version`: SSTable format version
- `format`: SSTable format

**Note:** When a user keyspace is created with S3 storage options, SSTables are put on remote object storage and the information about them is kept in this table.

---

## system.sstables_registry

Tracks SSTable ownership information.

Schema:
```cql
CREATE TABLE system.sstables_registry (
    owner uuid,
    generation timeuuid,
    status text,
    state text,
    version text,
    format text,
    PRIMARY KEY (owner, generation)
) WITH CLUSTERING ORDER BY (generation ASC);
```

**Columns:**
- `owner`: UUID of the SSTable owner
- `generation`: SSTable generation identifier (clustering key)
- `status`: Current status of the SSTable
- `state`: State of the SSTable
- `version`: SSTable format version
- `format`: SSTable format

---

## system.tablet_sizes

Virtual table containing information about current tablet disk sizes. The table can contain incomplete data, in which case `missing_replicas` will contain the host IDs of replicas for which the tablet size is not known. Can be queried on any node, but the data comes from the group0 leader. Reads wait for group0 leader to be elected and load balancer stats to become available.

Schema:
```cql
CREATE TABLE system.tablet_sizes (
    table_id uuid,
    last_token bigint,
    missing_replicas frozen<set<uuid>>,
    replicas frozen<map<uuid, bigint>>,
    PRIMARY KEY (table_id, last_token)
) WITH CLUSTERING ORDER BY (last_token ASC);
```

**Columns:**
- `table_id`: UUID of the table
- `last_token`: Last token owned by the tablet
- `missing_replicas`: Set of host IDs for replicas where tablet size is unknown
- `replicas`: Map of replica host IDs to disk size of the tablet replica in bytes

**Related tables:** [tablets](#systemtablets), [load_per_node](#systemload_per_node)

---

## system.tablets

Holds information about all tablets in the cluster. Only tables that use tablet-based replication strategy have entries here.

Each partition (`table_id`) represents a tablet map of a given table.

Schema:
```cql
CREATE TABLE system.tablets (
    table_id uuid,
    last_token bigint,
    base_table uuid STATIC,
    keyspace_name text STATIC,
    repair_scheduler_config frozen<repair_scheduler_config> STATIC,
    resize_seq_number bigint STATIC,
    resize_task_info frozen<tablet_task_info> STATIC,
    resize_type text STATIC,
    table_name text STATIC,
    tablet_count int STATIC,
    migration_task_info frozen<tablet_task_info>,
    new_replicas frozen<list<frozen<tuple<uuid, int>>>>,
    repair_task_info frozen<tablet_task_info>,
    repair_time timestamp,
    replicas frozen<list<frozen<tuple<uuid, int>>>>,
    session uuid,
    stage text,
    transition text,
    sstables_repaired_at bigint,
    repair_incremental_mode text,
    PRIMARY KEY (table_id, last_token)
) WITH CLUSTERING ORDER BY (last_token ASC);
```

**User-Defined Types:**
```cql
CREATE TYPE system.repair_scheduler_config (
    auto_repair_enabled boolean,
    auto_repair_threshold bigint
);

CREATE TYPE system.tablet_task_info (
    request_type text,
    tablet_task_id uuid,
    request_time timestamp,
    sched_nr bigint,
    sched_time timestamp,
    repair_hosts_filter text,
    repair_dcs_filter text
);
```

**Static Columns (per table):**
- `tablet_count`: Number of tablets in the map
- `table_name`: Name of the table (provided for convenience)
- `keyspace_name`: Name of the keyspace
- `base_table`: Optionally set with the `table_id` of another table that this table is co-located with, meaning they always have the same tablet count and tablet replicas, and are migrated and resized together as a group. When `base_table` is set, the rest of the tablet map is empty, and the tablet map of `base_table` should be read instead. When `base_table` is NULL, this table has its own independent tablet map stored in the remaining columns.
- `resize_type`: Resize decision type that spans all tablets of a given table (`merge`, `split`, or `none`)
- `resize_seq_number`: Sequence number (>= 0) of the resize decision that globally identifies it. It's monotonically increasing, incremented by one for every new decision, so a higher value means it came later in time.
- `repair_scheduler_config`: Configuration for the repair scheduler containing:
  - `auto_repair_enabled`: When set to true, auto repair is enabled. Disabled by default.
  - `auto_repair_threshold`: If the time since last repair is longer than `auto_repair_threshold` seconds, the tablet is eligible for auto repair.

**Per-Tablet Columns:**

`last_token` is the last token owned by the tablet. The i-th tablet, where i = 0, 1, ..., `tablet_count`-1, owns the token range:
```
   (-inf, last_token(0)]            for i = 0
   (last_token(i-1), last_token(i)] for i > 0
```

- `replicas`: List of tuples `(host_id, shard_id)` representing the shard-replicas of the tablet
- `new_replicas`: What will be put in `replicas` after transition is done
- `stage`: Current stage of tablet transition
- `transition`: Type of transition (see below)
- `repair_time`: Last time the tablet was repaired
- `sstables_repaired_at`: Tablet-level reference `repaired_at` value. When an SSTable's on-disk `repaired_at` value is less than or equal to `sstables_repaired_at`, the SSTable is considered repaired.
- `repair_incremental_mode`: Mode for incremental repair (see below)
- `repair_task_info`: Metadata for the task manager containing:
  - `request_type`: Type of the request (`user_repair` or `auto_repair`)
  - `tablet_task_id`: UUID of the task
  - `request_time`: Time the request was created
  - `sched_nr`: Number of times the request has been scheduled by the repair scheduler
  - `sched_time`: Time the request was scheduled by the repair scheduler
  - `repair_hosts_filter`: Repair replicas listed in the comma-separated host_id list
  - `repair_dcs_filter`: Repair replicas listed in the comma-separated DC list

**Transition types:**
- `migration`: One tablet replica is moving from one shard to another
- `rebuild`: New tablet replica is created from the remaining replicas
- `repair`: Tablet replicas are being repaired

**Incremental repair modes (`repair_incremental_mode`):**
- `regular`: The incremental repair logic is enabled. Unrepaired SSTables will be included for repair. Repaired SSTables will be skipped. The incremental repair states will be updated after repair.
- `full`: The incremental repair logic is enabled. Both repaired and unrepaired SSTables will be included for repair. The incremental repair states will be updated after repair.
- `disabled`: The incremental repair logic is disabled completely. The incremental repair states (e.g., `repaired_at` in SSTables and `sstables_repaired_at` in this table) will not be updated after repair.

**Related tables:** [tablet_sizes](#systemtablet_sizes), [load_per_node](#systemload_per_node), [topology](#systemtopology)

---

## system.token_ring

Virtual table containing the ring description for each keyspace. Equivalent of the `nodetool describe_ring $KEYSPACE` command (when filtered for a specific keyspace). Overlaps with the output of `nodetool ring`.

Schema:
```cql
CREATE TABLE system.token_ring (
    keyspace_name text,
    table_name text,
    start_token text,
    endpoint inet,
    end_token text,
    dc text,
    rack text,
    PRIMARY KEY (keyspace_name, table_name, start_token, endpoint)
) WITH CLUSTERING ORDER BY (table_name ASC, start_token ASC, endpoint ASC);
```

**Columns:**
- `keyspace_name`: Name of the keyspace
- `table_name`: Name of the table
- `start_token`: Start of the token range
- `endpoint`: IP address of the node owning this range
- `end_token`: End of the token range
- `dc`: Data center of the endpoint
- `rack`: Rack of the endpoint

Implemented by `token_ring_table` in `db/system_keyspace.cc`.

---

## system.topology

Stores cluster topology state managed by Raft. This is the central table for Raft-based topology management.

Schema:
```cql
CREATE TABLE system.topology (
    key text,
    host_id uuid,
    cleanup_status text,
    datacenter text,
    ignore_msb int,
    node_state text,
    num_tokens int,
    rack text,
    rebuild_option text,
    release_version text,
    replaced_id uuid,
    request_id timeuuid,
    shard_count int,
    supported_features frozen<set<text>>,
    tokens frozen<set<text>>,
    tokens_string text,
    topology_request text,
    committed_cdc_generations frozen<set<tuple<bigint, timeuuid>>> STATIC,
    enabled_features frozen<set<text>> STATIC,
    fence_version bigint STATIC,
    global_requests frozen<set<timeuuid>> STATIC,
    global_topology_request text STATIC,
    global_topology_request_id timeuuid STATIC,
    ignore_nodes frozen<set<uuid>> STATIC,
    new_cdc_generation_data_uuid timeuuid STATIC,
    new_keyspace_rf_change_data frozen<map<text, text>> STATIC,
    new_keyspace_rf_change_ks_name text STATIC,
    paused_rf_change_requests frozen<set<timeuuid>> STATIC,
    session uuid STATIC,
    tablet_balancing_enabled boolean STATIC,
    transition_state text STATIC,
    unpublished_cdc_generations frozen<set<tuple<bigint, timeuuid>>> STATIC,
    upgrade_state text STATIC,
    version bigint STATIC,
    PRIMARY KEY (key, host_id)
) WITH CLUSTERING ORDER BY (host_id ASC);
```

**Partition Key:**
- `key`: Partition key (typically `"topology"`)

**Clustering Column:**
- `host_id`: Host ID of the node (each row represents one node)

**Per-Node Regular Columns:**
- `datacenter`: Data center of the node
- `rack`: Rack of the node
- `tokens`: Set of token strings owned by the node
- `node_state`: Current state of the node in topology
- `release_version`: ScyllaDB version running on the node
- `topology_request`: Current topology request for this node
- `replaced_id`: If this node is replacing another, the replaced node's host ID
- `rebuild_option`: Rebuild configuration for the node
- `num_tokens`: Number of tokens for the node
- `tokens_string`: Alternative representation of tokens
- `shard_count`: Number of shards on the node
- `ignore_msb`: MSB bits to ignore for token calculation
- `cleanup_status`: Status of cleanup operations
- `supported_features`: Features supported by this node
- `request_id`: ID of the current topology request for this node

**Static Columns (cluster-wide state):**
- `ignore_nodes`: Set of node IDs to ignore
- `new_cdc_generation_data_uuid`: UUID for new CDC generation data
- `new_keyspace_rf_change_ks_name`: (deprecated) Keyspace name for RF change
- `new_keyspace_rf_change_data`: (deprecated) RF change data
- `version`: Topology version number
- `fence_version`: Version used for fencing operations
- `transition_state`: Current state of topology transition
- `committed_cdc_generations`: Set of committed CDC generations (generation timestamp, ID)
- `unpublished_cdc_generations`: Set of unpublished CDC generations
- `global_topology_request`: Current global topology request
- `global_topology_request_id`: ID of the global topology request
- `enabled_features`: Features enabled across the cluster
- `session`: Current topology session ID
- `tablet_balancing_enabled`: Whether tablet load balancing is enabled
- `upgrade_state`: State of cluster upgrade process
- `global_requests`: Set of global request IDs
- `paused_rf_change_requests`: Set of paused replication factor change requests

**Related tables:** [topology_requests](#systemtopology_requests), [tablets](#systemtablets), [discovery](#systemdiscovery)

---

## system.topology_requests

Queue of pending topology change requests. The coordinator processes this queue to handle topology operations.

Schema:
```cql
CREATE TABLE system.topology_requests (
    id timeuuid PRIMARY KEY,
    initiating_host uuid,
    request_type text,
    start_time timestamp,
    done boolean,
    error text,
    end_time timestamp,
    truncate_table_id uuid,
    new_keyspace_rf_change_ks_name text,
    new_keyspace_rf_change_data map<text, text>
);
```

**Columns:**
- `id`: Unique identifier for the request
- `initiating_host`: Host ID of the node that initiated the request
- `request_type`: Type of topology request (e.g., `join`, `leave`, `remove`, `replace`, `remove_node`)
- `start_time`: When the request was started
- `done`: Whether the request has been completed
- `error`: Error message if the request failed
- `end_time`: When the request completed
- `truncate_table_id`: Table ID for truncate operations
- `new_keyspace_rf_change_ks_name`: Keyspace name for replication factor changes
- `new_keyspace_rf_change_data`: Data for replication factor changes

**Related tables:** [topology](#systemtopology)

---

## system.truncated

Holds truncation replay positions per table and shard. When a table is truncated, SSTables are removed and the current replay position for each shard is recorded here. During commitlog replay (in case of a crash), this data is used to filter mutations and ensure truncated data is not resurrected.

Schema:
```cql
CREATE TABLE system.truncated (
    table_uuid uuid,
    shard int,
    position int,
    segment_id bigint,
    truncated_at timestamp STATIC,
    PRIMARY KEY (table_uuid, shard)
) WITH CLUSTERING ORDER BY (shard ASC);
```

**Columns:**
- `table_uuid`: ID of the truncated table
- `shard`: Shard number
- `position`: Replay position within the segment
- `segment_id`: Commitlog segment ID
- `truncated_at`: (static) Timestamp when the truncation occurred

**Note:** Prior to this table being added, truncation records were kept in the `truncated_at` map column in `system.local`. When booting up, ScyllaDB merges data from the legacy store with data from this table. Until the whole cluster agrees on the feature `TRUNCATION_TABLE`, truncation will write both new and legacy records. When the feature is agreed upon, the legacy map is removed.

---

## system.versions

Virtual table containing all version-related information. Equivalent of the `nodetool version` command, but contains more detailed version information.

Schema:
```cql
CREATE TABLE system.versions (
    key text PRIMARY KEY,
    version text,
    build_mode text,
    build_id text
);
```

**Columns:**
- `key`: Version category identifier (typically `"local"`)
- `version`: Full version string
- `build_mode`: Build mode (e.g., `release`, `debug`, `dev`)
- `build_id`: Build identifier/hash

Implemented by `versions_table` in `db/system_keyspace.cc`.

**Related tables:** [config](#systemconfig)

---

## system.view_build_status_v2

Tracks the build status of materialized views across the cluster.

Schema:
```cql
CREATE TABLE system.view_build_status_v2 (
    keyspace_name text,
    view_name text,
    host_id uuid,
    status text,
    PRIMARY KEY ((keyspace_name, view_name), host_id)
) WITH CLUSTERING ORDER BY (host_id ASC);
```

**Columns:**
- `keyspace_name`: Keyspace containing the view
- `view_name`: Name of the materialized view
- `host_id`: Host ID of the node reporting status
- `status`: Build status (e.g., `STARTED`, `SUCCESS`)

**Related tables:** [built_views](#systembuilt_views), [views_builds_in_progress](#systemviews_builds_in_progress)

---

## system.view_building_tasks

Tracks tasks related to building materialized views.

Schema:
```cql
CREATE TABLE system.view_building_tasks (
    key text,
    id timeuuid,
    type text,
    aborted boolean,
    base_id uuid,
    view_id uuid,
    last_token bigint,
    host_id uuid,
    shard int,
    PRIMARY KEY (key, id)
) WITH CLUSTERING ORDER BY (id ASC);
```

**Columns:**
- `key`: Partition key for grouping tasks
- `id`: Unique identifier for the build task (timeuuid)
- `type`: Type of the build task
- `aborted`: Whether the task has been aborted
- `base_id`: UUID of the base table
- `view_id`: UUID of the materialized view
- `last_token`: Last token processed
- `host_id`: Host ID of the node handling this task
- `shard`: Shard ID handling this task

**Related tables:** [views_builds_in_progress](#systemviews_builds_in_progress)

---

## system.views_builds_in_progress

Tracks materialized view builds that are currently in progress. This is the Cassandra-compatible version.

Schema:
```cql
CREATE TABLE system.views_builds_in_progress (
    keyspace_name text,
    view_name text,
    generation_number int,
    last_token text,
    PRIMARY KEY (keyspace_name, view_name)
) WITH CLUSTERING ORDER BY (view_name ASC);
```

**Columns:**
- `keyspace_name`: Keyspace containing the view
- `view_name`: Name of the materialized view
- `generation_number`: Build generation number
- `last_token`: Last token that was processed

**Related tables:** [scylla_views_builds_in_progress](#systemscylla_views_builds_in_progress), [built_views](#systembuilt_views), [view_build_status_v2](#systemview_build_status_v2)

---

# Virtual Tables

Virtual tables behave just like regular tables from the user's point of view. The difference between them and regular tables comes down to how they are implemented. While regular tables have memtables/commitlog/SSTables and all you would expect from CQL tables, virtual tables translate some in-memory structure to CQL result format.

The following system tables are virtual tables:
- [cluster_status](#systemcluster_status)
- [clients](#systemclients)
- [config](#systemconfig)
- [load_per_node](#systemload_per_node)
- [protocol_servers](#systemprotocol_servers)
- [runtime_info](#systemruntime_info)
- [size_estimates](#systemsize_estimates)
- [snapshots](#systemsnapshots)
- [tablet_sizes](#systemtablet_sizes)
- [token_ring](#systemtoken_ring)
- [versions](#systemversions)

For more details see [virtual-tables.md](virtual-tables.md).

---

# Notes

1. **Read-Only Tables**: Most system tables should be considered read-only. Direct modification can cause cluster instability.

2. **Internal Use**: Many tables (especially Raft, CDC, and topology tables) are for internal ScyllaDB use. Modifying them directly is not recommended.

3. **Monitoring**: Tables like `large_partitions`, `large_rows`, and `large_cells` are useful for performance monitoring and troubleshooting.

4. **Version Suffixes**: Tables with `_v2` or `_v3` suffixes represent schema evolution across ScyllaDB versions.

5. **TTL on Diagnostic Tables**: The `large_*` tables have entries with a 30-day TTL.

## Verification

To verify table schemas in your cluster:

```cql
-- Describe a specific table
DESCRIBE TABLE system.<table_name>;

-- List all tables in the system keyspace
SELECT * FROM system_schema.tables WHERE keyspace_name = 'system';

-- Get column details for a specific table
SELECT * FROM system_schema.columns 
WHERE keyspace_name = 'system' AND table_name = '<table_name>';
```
