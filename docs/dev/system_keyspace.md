# System keyspace layout

This section describes layouts and usage of system.* tables.

## The system.large\_* tables

Scylla performs better if partitions, rows, or cells are not too
large. To help diagnose cases where these grow too large, scylla keeps
3 tables that record large partitions (including those with too many
rows), rows, and cells, respectively.

The meaning of an entry in each of these tables is similar. It means
that there is a particular sstable with a large partition, row, cell,
or a partition with too many rows. In particular, this implies that:

* There is no entry until compaction aggregates enough data in a
  single sstable.
* The entry stays around until the sstable is deleted.

In addition, the entries also have a TTL of 30 days.

## system.large\_partitions

Large partition table can be used to trace largest partitions in a
cluster.  Partitions with too many rows are also recorded there.

Schema:
~~~
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
~~~

### Example usage

#### Extracting large partitions info
~~~
SELECT * FROM system.large_partitions;
~~~

#### Extracting large partitions info for a single table
~~~
SELECT * FROM system.large_partitions WHERE keyspace_name = 'ks1' and table_name = 'standard1';
~~~

## system.large\_rows

Large row table can be used to trace large clustering and static rows in a cluster.

This table is currently only used with the MC format (issue #4868).

Schema:
~~~
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
~~~

### Example usage

#### Extracting large row info
~~~
SELECT * FROM system.large_rows;
~~~

#### Extracting large rows info for a single table
~~~
SELECT * FROM system.large_rows WHERE keyspace_name = 'ks1' and table_name = 'standard1';
~~~

## system.large\_cells

Large cell table can be used to trace large cells in a cluster.

This table is currently only used with the MC format (issue #4868).

Schema:
~~~
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
) WITH CLUSTERING ORDER BY (sstable_name ASC, cell_size DESC, partition_key ASC, clustering_key ASC, column_name ASC)
~~~

Note that a collection is just one cell. There is no information about
the size of each collection element.

### Example usage

#### Extracting large cells info
~~~
SELECT * FROM system.large_cells;
~~~

#### Extracting large cells info for a single table
~~~
SELECT * FROM system.large_cells WHERE keyspace_name = 'ks1' and table_name = 'standard1';
~~~

## system.raft

Holds information about Raft

Schema:
~~~
CREATE TABLE system.raft (
    group_id timeuuid,
    index bigint,
    term bigint,
    data blob,
    vote_term bigint static,
    vote uuid static,
    snapshot_id uuid static,
    commit_idx bigint static,
    PRIMARY KEY (group_id, index)
) WITH CLUSTERING ORDER BY (index ASC)
~~~

## system.truncated

Holds truncation replay positions per table and shard

Schema:
~~~
CREATE TABLE system.truncated (
    table_uuid uuid,    # id of truncated table
    shard int,          # shard
    position int,       # replay position
    segment_id bigint,  # replay segment
    truncated_at timestamp static,  # truncation time
    PRIMARY KEY (table_uuid, shard)
) WITH CLUSTERING ORDER BY (shard ASC)
~~~

When a table is truncated, sstables are removed and the current replay position for each 
shard (last mutation to be committed to either sstable or memtable) is collected.
These are then inserted into the above table, using shard as clustering.

When doing commitlog replay (in case of a crash), the data is read from the above 
table and mutations are filtered based on the replay positions to ensure 
truncated data is not resurrected.
 
Note that until the above table was added, truncation records where kept in the 
`truncated_at` map column in the `system.local` table. When booting up, scylla will
merge the data in the legacy store with data the `truncated` table. Until the whole
cluster agrees on the feature `TRUNCATION_TABLE` truncation will write both new and 
legacy records. When the feature is agreed upon the legacy map is removed.

## system.sstables

The "ownership" table for non-local sstables

Schema:
~~~
CREATE TABLE system.sstables (
    owner uuid,
    generation timeuuid,
    format text,
    status text,
    uuid uuid,
    version text,
    PRIMARY KEY (owner, generation)
)
~~~

When a user keyspace is created with S3 storage options, sstables are put on the
remote object storage and the information about them is kept in this table. The
"uuid" field is used to point to the "folder" in which all sstables files are.

## system.tablets

Holds information about all tablets in the cluster.

Schema:
~~~
CREATE TABLE system.tablets (
    table_id uuid,
    last_token bigint,
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
    PRIMARY KEY (table_id, last_token)
)

CREATE TYPE system.repair_scheduler_config (
    auto_repair_enabled boolean,
    auto_repair_threshold bigint
)

CREATE TYPE system.tablet_task_info (
    request_type text,
    tablet_task_id uuid,
    request_time timestamp,
    sched_nr bigint,
    sched_time timestamp,
    repair_hosts_filter text,
    repair_dcs_filter text,
)
~~~

Each partition (table_id) represents a tablet map of a given table.

Only tables which use tablet-based replication strategy have an entry here.

`tablet_count` is the number of tablets in the map.
`table_name` is the name of the table, provided for convenience.

`resize_type` is the resize decision type that spans all tablets of a given table, which can be one of: `merge`, `split` or `none`.

`resize_seq_number` is the sequence number (>= 0) of the resize decision that globally identifies it. It's monotonically increasing, incremented by one for every new decision, so a higher value means it came later in time.

`last_token` is the last token owned by the tablet. The i-th tablet, where i = 0, 1, ..., `tablet_count`-1),
 owns the token range:
```
   (-inf, last_token(0)]            for i = 0
   (last_token(i-1), last_token(i)] for i > 0
```

`repair_time` is the last time the tablet has been repaired.

`repair_task_info` contains the metadata for the task manager. It contains the following values:
  * `request_type` - The type of the request. It could be user_repair and auto_repair.
  * `tablet_task_id` - The UUID of the task.
  * `request_time` - The time the request is created.
  * `sched_nr` - Number of times the request has been scheduled by the repair scheduler.
  * `sched_time` - The time the request has been scheduled by the repair scheduler.
  * `repair_hosts_filter` - Repair replicas listed in the comma-separated host_id list.
  * `repair_dcs_filter` - Repair replicas listed in the comma-separated DC list.

`repair_scheduler_config` contains configuration for the repair scheduler. It contains the following values:
  * `auto_repair_enabled` - When set to true, auto repair is enabled. Disabled by default.
  * `auto_repair_threshold` -  If the time since last repair is longer than auto_repair_threshold seconds, the tablet is eligible for auto repair.

Each tablet is represented by a single row. `replicas` holds the set of shard-replicas of the tablet.
It's a list of tuples where the first element is `host_id` of the replica and the second element is the `shard_id` of the replica.

During tablet migration, the columns `new_replicas`, `stage` and `transition` are set to represent the transition. The
`new_replicas` column holds what will be put in `replicas` after transition is done.

During tablet splitting, the load balancer sets `resize_type` column with `split`, and sets `resize_seq_number` with the next sequence number, which is the previous value incremented by one.

The `transition` column can have the following values:
  * `migration` - One tablet replica is moving from one shard to another.
  * `rebuild` - New tablet replica is created from the remaining replicas.
  * `repair` - Tablet replicas are being repaired.

# Virtual tables in the system keyspace

Virtual tables behave just like a regular table from the user's point of view.
The difference between them and regular tables comes down to how they are implemented.
While regular tables have memtables/commitlog/sstables and all you would expect from CQL tables, virtual tables translate some in-memory structure to CQL result format.
For more details see the [virtual-tables.md](virtual-tables.md).

Below you can find a list of virtual tables. Sorted in alphabetical order (please keep it so when modifying!).

## system.cluster_status

Contain information about the status of each endpoint in the cluster.
Equivalent of the `nodetool status` command.

Schema:
```cql
CREATE TABLE system.cluster_status (
    peer inet PRIMARY KEY,
    dc text,
    host_id uuid,
    load text,
    owns float,
    status text,
    tokens int,
    up boolean
)
```

Implemented by `cluster_status_table` in `db/system_keyspace.cc`.

## system.load_per_node

Contains information about the current tablet load with node granularity.
Can be queried on any node, but the data comes from the group0 leader.
Reads wait for group0 leader to be elected and load balancer stats to become available.

Schema:
```cql
CREATE TABLE system.load_per_node (
    node uuid PRIMARY KEY,
    dc text,
    rack text,
    storage_allocated_load bigint,
    storage_allocated_utilization double,
    storage_capacity bigint,
    tablets_allocated bigint,
    tablets_allocated_per_shard double
);
```

Columns:
* `dc` - The name of the data center to which the node belongs.
* `rack` - The name of the rack to which the node belongs.
* `storage_allocated_load` - Disk space allocated for tablets, assuming each tablet has a fixed size (target_tablet_size).
* `storage_allocated_utilization` - Fraction of node's disk capacity taken for `storage_allocated_load`, where 1.0 means full utilization.
* `storage_capacity` - Total disk capacity in bytes. Used to compute `storage_allocated_utilization`. By default equal to file system's capacity.
* `tablets_allocated` - Number of tablet replicas on the node. Migrating tablets are accounted as if migration already finished.
* `tablets_allocated_per_shard` - `tablets_allocated` divided by shard count on the node.

## system.protocol_servers

The list of all the client-facing data-plane protocol servers and listen addresses (if running).
Equivalent of the `nodetool statusbinary` plus the `Native Transport active` fields from `nodetool info`.

TODO: include control-plane diagnostics-plane protocols here too.

Schema:
```cql
CREATE TABLE system.protocol_servers (
    name text PRIMARY KEY,
    is_running boolean,
    listen_addresses frozen<list<text>>,
    protocol text,
    protocol_version text
)
```

Columns:
* `name` - the name/alias of the server, this is sometimes different than the protocol the server serves, e.g.: the CQL server is often called "native";
* `listen_addresses` - the addresses this server listens on, empty if the server is not running;
* `protocol` - the name of the protocol this server serves;
* `protocol_version` - the version of the protocol this server understands;

Implemented by `protocol_servers_table` in `db/system_keyspace.cc`.

## system.size_estimates

Size estimates for individual token-ranges of each keyspace/table.

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
)
```

Implemented by `size_estimates_mutation_reader` in `db/size_estimates_virtual_reader.{hh,cc}`.

## system.snapshots

The list of snapshots on the node.
Equivalent to the `nodetool listsnapshots` command.

Schema:
```cql
CREATE TABLE system.snapshots (
    keyspace_name text,
    table_name text,
    snapshot_name text,
    live bigint,
    total bigint,
    PRIMARY KEY (keyspace_name, table_name, snapshot_name)
)
```

Implemented by `snapshots_table` in `db/system_keyspace.cc`.

## system.runtime_info

Runtime specific information, like memory stats, memtable stats, cache stats and more.
Data is grouped so that related items stay together and are easily queried.
Roughly equivalent of the `nodetool info`, `nodetool gettraceprobability` and `nodetool statusgossup` commands.

Schema:
```cql
CREATE TABLE system.runtime_info (
    group text,
    item text,
    value text,
    PRIMARY KEY (group, item)
)
```

Implemented by `runtime_info_table` in `db/system_keyspace.cc`.

## system.token_ring

The ring description for each keyspace.
Equivalent of the `nodetool describe_ring $KEYSPACE` command (when filtered for `WHERE keyspace=$KEYSPACE`).
Overlaps with the output of `nodetool ring`.

Schema:
```cql
CREATE TABLE system.token_ring (
    keyspace_name text,
    start_token text,
    endpoint inet,
    dc text,
    end_token text,
    rack text,
    PRIMARY KEY (keyspace_name, start_token, endpoint)
)
```

Implemented by `token_ring_table` in `db/system_keyspace.cc`.

## system.versions

All version-related information.
Equivalent of `nodetool version` command, but contains more versions.

Schema:
```cql
CREATE TABLE system.versions (
    key text PRIMARY KEY,
    build_id text,
    build_mode text,
    compatible_version text,
    version text
)
```

Implemented by `versions_table` in `db/system_keyspace.cc`.

## system.config

Holds all configuration variables in use

Schema:
~~~
CREATE TABLE system.config (
    name text PRIMARY KEY,
    source text,
    type text,
    value text
)
~~~

The source of the option is one of 'default', 'config', 'cli', 'cql' or 'internal'
which means the value wasn't changed from its default, was configured via config
file, was set by commandline option or via updating this table, or was deliberately
configured by Scylla internals. Any way the option was updated overrides the
previous one, so shown here is the latest one used.

The type denotes the variable type like 'string', 'bool', 'integer', etc. Including
some scylla-internal configuration types.

The value is shown as it would appear in the json config file.

The table can be updated with the UPDATE statement. The accepted value parameter
must (of course) be a text, it's converted to the target configuration value as
needed.

## system.clients

Holds information about clients connections

Schema:
~~~
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
    PRIMARY KEY (address, port, client_type)
) WITH CLUSTERING ORDER BY (port ASC, client_type ASC)
~~~

Currently only CQL clients are tracked. The table used to be present on disk (in data
directory) before and including version 4.5.

## TODO: the rest
