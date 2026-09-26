# Configuration Parameters

This section contains a list of properties that can be configured in `scylla.yaml` - the main configuration file for ScyllaDB.
In addition, properties that support live updates (liveness) can be updated via the `system.config` virtual table or the [REST API](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/rest.md).

Live update means that parameters can be modified dynamically while the server
is running. If `liveness` of a parameter is set to `true`, sending the `SIGHUP`
signal to the server processes will trigger ScyllaDB to re-read its configuration
and override the current configuration with the new value.

**Configuration Precedence**

As the parameters can be configured in more than one place, ScyllaDB applies them
in the following order with `scylla.yaml` parameters updated via `SIGHUP`
having the highest priority:

1. Live update via `scylla.yaml` (with `SIGHUP`) or REST API
2. `system.config` table
3. command line options
4. `scylla.yaml`

<!-- -*- mode: rst -*- -->

<a id="confgroup-ungrouped-properties"></a>

## Ungrouped properties

<a id="confprop-memtable-flush-static-shares"></a>

### memtable_flush_static_shares

> If set to higher than 0, ignore the controller’s output and set the memtable shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity.

> * **Type:** `float`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-static-shares"></a>

### compaction_static_shares

> If set to higher than 0, ignore the controller’s output and set the compaction shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity.

> * **Type:** `float`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-enforce-min-threshold"></a>

### compaction_enforce_min_threshold

> If set to true, enforce the min_threshold option for compactions strictly. If false (default), Scylla may decide to compact even if below min_threshold.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-flush-all-tables-before-major-seconds"></a>

### compaction_flush_all_tables_before_major_seconds

> Set the minimum interval in seconds between flushing all tables before each major compaction (default is 86400).    This option is useful for maximizing tombstone garbage collection by releasing all active commitlog segments.    Set to 0 to disable automatic flushing all tables before major compaction.

> * **Type:** `uint32_t`
> * **Default value:** `86400`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-initialization-properties"></a>

## Initialization properties

> The minimal properties needed for configuring a cluster.

<a id="confprop-cluster-name"></a>

### cluster_name

> The name of the cluster; used to prevent machines in one logical cluster from joining another. All nodes participating in a cluster must have the same value.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-listen-address"></a>

### listen_address

> The IP address or hostname that Scylla binds to for connecting to other Scylla nodes. You must change the default setting for multiple nodes to communicate. Do not set to 0.0.0.0, unless you have set broadcast_address to an address that other nodes can use to reach this node.

> * **Type:** `sstring`
> * **Default value:** `"localhost"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-listen-interface-prefer-ipv6"></a>

### listen_interface_prefer_ipv6

> If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address    you can specify which should be chosen using listen_interface_prefer_ipv6. If false the first ipv4    address will be used. If true the first ipv6 address will be used. Defaults to false preferring    ipv4. If there is only one address it will be selected regardless of ipv4/ipv6.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-default-directories"></a>

## Default directories

> If you have changed any of the default directories during installation, make sure you have root access and set these properties.

<a id="confprop-workdir-w"></a>

### workdir,W

> The directory in which Scylla will put all its subdirectories. The location of individual subdirs can be overridden by the respective \`\`\*_directory\`\` options.

> * **Default value:** `"/var/lib/scylla"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-directory"></a>

### commitlog_directory

> The directory where the commit log is stored. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-schema-commitlog-directory"></a>

### schema_commitlog_directory

> The directory where the schema commit log is stored. This is a special commitlog instance used for schema and system tables. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-data-file-directories"></a>

### data_file_directories

> The directory location where table data (SSTables) is stored.

> * **Type:** `string_list`
> * **Default value:** `{ }`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-hints-directory"></a>

### hints_directory

> The directory where hints files are stored if hinted handoff is enabled.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-view-hints-directory"></a>

### view_hints_directory

> The directory where materialized-view updates are stored while a view replica is unreachable.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-common-initialization-properties"></a>

## Common initialization properties

> Be sure to set the properties in the Quick start section as well.

<a id="confprop-endpoint-snitch"></a>

### endpoint_snitch

> Set to a class that implements the IEndpointSnitch. Scylla uses snitches for locating nodes and routing requests.
> : * SimpleSnitch: Use for single-data center deployments or single-zone in public clouds. Does not recognize data center or rack information. It treats strategy order as proximity, which can improve cache locality when disabling read repair.
>   * GossipingPropertyFileSnitch: Recommended for production. The rack and data center for the local node are defined in the cassandra-rackdc.properties file and propagated to other nodes via gossip. To allow migration from the PropertyFileSnitch, it uses the cassandra-topology.properties file if it is present.
>   * Ec2Snitch: For EC2 deployments in a single region. Loads region and availability zone information from the EC2 API. The region is treated as the data center and the availability zone as the rack. Uses only private IPs. Subsequently it does not work across multiple regions.
>   * Ec2MultiRegionSnitch: Uses public IPs as the broadcast_address to allow cross-region connectivity. This means you must also set seed addresses to the public IP and open the storage_port or ssl_storage_port on the public IP firewall. For intra-region traffic, Scylla switches to the private IP after establishing a connection.
>   * GoogleCloudSnitch: For deployments on Google Cloud Platform across one or more regions. The region is treated as a datacenter and the availability zone is treated as a rack within the datacenter. The communication should occur over private IPs within the same logical network.
>   * RackInferringSnitch: Proximity is determined by rack and data center, which are assumed to correspond to the 3rd and 2nd octet of each node’s IP address, respectively. This snitch is best used as an example for writing a custom snitch class (unless this happens to match your deployment conventions).
>   <br/>
>   Related information: Snitches

> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.locator.SimpleSnitch"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-rpc-address"></a>

### rpc_address

> The listen address for client connections (Thrift RPC service and native transport).Valid values are:
> : * unset: Resolves the address using the hostname configuration of the node. If left unset, the hostname must resolve to the IP address of this node using /etc/hostname, /etc/hosts, or DNS.
>   * 0.0.0.0: Listens on all configured interfaces, but you must set the broadcast_rpc_address to a value other than 0.0.0.0.
>   * IP address
>   * hostname
>   <br/>
>   Related information: Network

> * **Type:** `sstring`
> * **Default value:** `"localhost"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-rpc-interface-prefer-ipv6"></a>

### rpc_interface_prefer_ipv6

> If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address    you can specify which should be chosen using rpc_interface_prefer_ipv6. If false the first ipv4    address will be used. If true the first ipv6 address will be used. Defaults to false preferring    ipv4. If there is only one address it will be selected regardless of ipv4/ipv6.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-seed-provider"></a>

### seed_provider

> The addresses of hosts deemed contact points. Scylla nodes use the -seeds list to find each other and learn the topology of the ring.

> > > * class_name (Default: org.apache.cassandra.locator.SimpleSeedProvider): The class within Scylla that handles the seed logic. It can be customized, but this is typically not required.
> > > * seeds (Default: 127.0.0.1): A comma-delimited list of IP addresses used by gossip for bootstrapping new nodes joining a cluster. When running multiple nodes, you must change the list from the default value. In multiple data-center clusters, the seed list should include at least one node from each data center (replication group). More than a single seed node per data center is recommended for fault tolerance. Otherwise, gossip has to communicate with another data center when bootstrapping a node. Making every node a seed node is not recommended because of increased maintenance and reduced gossip performance. Gossip optimization is not critical, but it is recommended to use a small seed list (approximately three nodes per data center).

> > Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers).
> * **Type:** `seed_provider_type`
> * **Default value:** `seed_provider_type("org.apache.cassandra.locator.SimpleSeedProvider")`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-common-compaction-settings"></a>

## Common compaction settings

> Be sure to set the properties in the Quick start section as well.

<a id="confprop-compaction-throughput-mb-per-sec"></a>

### compaction_throughput_mb_per_sec

> Throttles compaction to the specified total throughput across the entire system. The faster you insert data, the faster you need to compact in order to keep the SSTable count down. The recommended Value is 16 to 32 times the rate of write throughput (in MBs/second). Setting the value to 0 disables compaction throttling.

> > Related information: Configuring compaction
> * **Type:** `uint32_t`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-large-partition-warning-threshold-mb"></a>

### compaction_large_partition_warning_threshold_mb

> Log a warning when writing partitions larger than this value.

> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-large-row-warning-threshold-mb"></a>

### compaction_large_row_warning_threshold_mb

> Log a warning when writing rows larger than this value.

> * **Type:** `uint32_t`
> * **Default value:** `10`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-large-cell-warning-threshold-mb"></a>

### compaction_large_cell_warning_threshold_mb

> Log a warning when writing cells larger than this value.

> * **Type:** `uint32_t`
> * **Default value:** `1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-rows-count-warning-threshold"></a>

### compaction_rows_count_warning_threshold

> Log a warning when writing a number of rows larger than this value.

> * **Type:** `uint32_t`
> * **Default value:** `100000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-compaction-collection-elements-count-warning-threshold"></a>

### compaction_collection_elements_count_warning_threshold

> Log a warning when writing a collection containing more elements than this value.

> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confgroup-common-automatic-backup-settings"></a>

## Common automatic backup settings

<a id="confprop-incremental-backups"></a>

### incremental_backups

> Backs up data updated since the last snapshot was taken. When enabled, Scylla creates a hard link to each SSTable flushed or streamed locally in a backups/ subdirectory of the keyspace data. Removing these links is the operator’s responsibility.

> > Related information: Enabling incremental backups
> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-common-fault-detection-setting"></a>

## Common fault detection setting

<a id="confprop-phi-convict-threshold"></a>

### phi_convict_threshold

> Adjusts the sensitivity of the failure detector on an exponential scale. Generally this setting never needs adjusting.

> > Related information: Failure detection and recovery
> * **Type:** `uint32_t`
> * **Default value:** `8`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-failure-detector-timeout-in-ms"></a>

### failure_detector_timeout_in_ms

> Maximum time between two successful echo message before gossip mark a node down in milliseconds.

> * **Type:** `uint32_t`
> * **Default value:** `20 * 1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-direct-failure-detector-ping-timeout-in-ms"></a>

### direct_failure_detector_ping_timeout_in_ms

> Duration after which the direct failure detector aborts a ping message, so the next ping can start.
> : Note: this failure detector is used by Raft, and is different from gossiper’s failure detector (configured by \`failure_detector_timeout_in_ms\`).

> * **Type:** `uint32_t`
> * **Default value:** `600`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-commit-log-settings"></a>

## Commit log settings

<a id="confprop-commitlog-sync"></a>

### commitlog_sync

> The method that Scylla uses to acknowledge writes in milliseconds:
> : * periodic: Used with commitlog_sync_period_in_ms (Default: 10000 - 10 seconds ) to control how often the commit log is synchronized to disk. Periodic syncs are acknowledged immediately.
>   * batch: Used with commitlog_sync_batch_window_in_ms (Default: disabled \`\`\*\*\`\`) to control how long Scylla waits for other writes before performing a sync. When using this method, writes are not acknowledged until fsynced to disk.
>   <br/>
>   Related information: Durability

> * **Type:** `sstring`
> * **Default value:** `"periodic"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-segment-size-in-mb"></a>

### commitlog_segment_size_in_mb

> Sets the size of the individual commitlog file segments. A commitlog segment may be archived, deleted, or recycled after all its data has been flushed to SSTables. This amount of data can potentially include commitlog segments from every table in the system. The default size is usually suitable for most commitlog archiving, but if you want a finer granularity, 8 or 16 MB is reasonable. See Commit log archive configuration.

> > Related information: Commit log archive configuration
> * **Type:** `uint32_t`
> * **Default value:** `64`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-schema-commitlog-segment-size-in-mb"></a>

### schema_commitlog_segment_size_in_mb

> Sets the size of the individual schema commitlog file segments. The default size is larger than the default size of the data commitlog because the segment size puts a limit on the mutation size that can be written at once, and some schema mutation writes are much larger than average.

> > Related information: Commit log archive configuration
> * **Type:** `uint32_t`
> * **Default value:** `128`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-sync-period-in-ms"></a>

### commitlog_sync_period_in_ms

> Controls how long the system waits for other writes before performing a sync in \`\`periodic\`\` mode.

> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-sync-batch-window-in-ms"></a>

### commitlog_sync_batch_window_in_ms

> Controls how long the system waits for other writes before performing a sync in \`\`batch\`\` mode.

> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-total-space-in-mb"></a>

### commitlog_total_space_in_mb

> Total space used for commitlogs. If the used space goes above this value, Scylla rounds up to the next nearest segment multiple and flushes memtables to disk for the oldest commitlog segments, removing those log segments. This reduces the amount of data to replay on startup, and prevents infrequently-updated tables from indefinitely keeping commitlog segments. A small total commitlog space tends to cause more flush activity on less-active tables.

> > Related information: Configuring memtable throughput
> * **Type:** `int64_t`
> * **Default value:** `-1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-flush-threshold-in-mb"></a>

### commitlog_flush_threshold_in_mb

> Threshold for commitlog disk usage. When used disk space goes above this value, Scylla initiates flushes of memtables to disk for the oldest commitlog segments, removing those log segments. Adjusting this affects disk usage vs. write latency. Default is (approximately) commitlog_total_space_in_mb - <num shards>\*commitlog_segment_size_in_mb.

> * **Type:** `int64_t`
> * **Default value:** `-1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-use-o-dsync"></a>

### commitlog_use_o_dsync

> Whether or not to use O_DSYNC mode for commitlog segments IO. Can improve commitlog latency on some file systems.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-commitlog-use-hard-size-limit"></a>

### commitlog_use_hard_size_limit

> Whether or not to use a hard size limit for commitlog disk usage. Default is true. Enabling this can cause latency spikes, whereas the default can lead to occasional disk usage peaks.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-compaction-settings"></a>

## Compaction settings

> Related information: Configuring compaction

<a id="confprop-defragment-memory-on-idle"></a>

### defragment_memory_on_idle

> When set to true, will defragment memory when the cpu is idle.  This reduces the amount of work Scylla performs when processing client requests.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-cache-and-index-settings"></a>

## Cache and index settings

<a id="confprop-column-index-size-in-kb"></a>

### column_index_size_in_kb

> Granularity of the index of rows within a partition. For huge rows, decrease this setting to improve seek time. If you use key cache, be careful not to make this setting too large because key cache will be overwhelmed. If you’re unsure of the size of the rows, it’s best to use the default setting.

> * **Type:** `uint32_t`
> * **Default value:** `64`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-column-index-auto-scale-threshold-in-kb"></a>

### column_index_auto_scale_threshold_in_kb

> Auto-reduce the promoted index granularity by half when reaching this threshold, to prevent promoted index bloating due to partitions with too many rows. Set to 0 to disable this feature.

> * **Type:** `uint32_t`
> * **Default value:** `10240`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confgroup-disks-settings"></a>

## Disks settings

<a id="confprop-stream-io-throughput-mb-per-sec"></a>

### stream_io_throughput_mb_per_sec

> Throttles streaming I/O to the specified total throughput (in MiBs/s) across the entire system. Streaming I/O includes the one performed by repair and both RBNO and legacy topology operations such as adding or removing a node. Setting the value to 0 disables stream throttling.

> * **Type:** `uint32_t`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-stream-plan-ranges-fraction"></a>

### stream_plan_ranges_fraction

> Specify the fraction of ranges to stream in a single stream plan. Value is between 0 and 1.

> * **Type:** `double`
> * **Default value:** `0.1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confgroup-advanced-initialization-properties"></a>

## Advanced initialization properties

> Properties for advanced users or properties that are less commonly used.

<a id="confprop-auto-bootstrap"></a>

### auto_bootstrap

> This setting has been removed from default configuration. It makes new (non-seed) nodes automatically migrate the right data to themselves. Do not set this to false unless you really know what you are doing.
> : Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers).

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-batch-size-warn-threshold-in-kb"></a>

### batch_size_warn_threshold_in_kb

> Log WARN on any batch size exceeding this value in kilobytes. Caution should be taken on increasing the size of this threshold as it can lead to node instability.

> * **Type:** `uint32_t`
> * **Default value:** `128`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-batch-size-fail-threshold-in-kb"></a>

### batch_size_fail_threshold_in_kb

> Fail any multiple-partition batch exceeding this value. 1 MiB (8x warn threshold) by default.

> * **Type:** `uint32_t`
> * **Default value:** `1024`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-broadcast-address"></a>

### broadcast_address

> The IP address a node tells other nodes in the cluster to contact it by. It allows public and private address to be different. For example, use the broadcast_address parameter in topologies where not all nodes have access to other nodes by their private IP addresses.
> : If your Scylla cluster is deployed across multiple Amazon EC2 regions and you use the EC2MultiRegionSnitch , set the broadcast_address to public IP address of the node and the listen_address to the private IP.

> * **Type:** `sstring`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-listen-on-broadcast-address"></a>

### listen_on_broadcast_address

> When using multiple physical network interfaces, set this to true to listen on broadcast_address in addition to the listen_address, allowing nodes to communicate in both interfaces.  Ignore this property if the network configuration automatically routes between the public and private networks such as EC2.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-initial-token"></a>

### initial_token

> Used in the single-node-per-token architecture, where a node owns exactly one contiguous range in the ring space. Setting this property overrides num_tokens.
> : If you not using vnodes or have num_tokens set it to 1 or unspecified (#num_tokens), you should always specify this parameter when setting up a production cluster for the first time and when adding capacity. For more information, see this parameter in the Cassandra 1.1 Node and Cluster Configuration documentation.
>   This parameter can be used with num_tokens (vnodes) in special cases such as Restoring from a snapshot.

> * **Type:** `sstring`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-num-tokens"></a>

### num_tokens

> Defines the number of tokens randomly assigned to this node on the ring when using virtual nodes (vnodes). The more tokens, relative to other nodes, the larger the proportion of data that the node stores. Generally all nodes should have the same number of tokens assuming equal hardware capability. The recommended value is 256. If unspecified (#num_tokens), Scylla uses 1 (equivalent to #num_tokens
> : If not using vnodes, comment #num_tokens : 256 or set num_tokens : 1 and use initial_token. If you already have an existing cluster with one token per node and wish to migrate to vnodes, see Enabling virtual nodes on an existing production cluster.
>   <br/>
>   #### NOTE
>   If using DataStax Enterprise, the default setting of this property depends on the type of node and type of install.

> * **Type:** `uint32_t`
> * **Default value:** `1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-partitioner"></a>

### partitioner

> Distributes rows (by partition key) across all nodes in the cluster. At the moment, only Murmur3Partitioner is supported. For new clusters use the default partitioner.

> > Related information: Partitioners
> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.dht.Murmur3Partitioner"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-storage-port"></a>

### storage_port

> The port for inter-node communication.

> * **Type:** `uint16_t`
> * **Default value:** `7000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-advanced-automatic-backup-setting"></a>

## Advanced automatic backup setting

<a id="confprop-auto-snapshot"></a>

### auto_snapshot

> Enable or disable whether a snapshot is taken of the data before keyspace truncation or dropping of tables. To prevent data loss, using the default setting is strongly advised. If you set to false, you will lose data on truncation or drop.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-tombstone-settings"></a>

## Tombstone settings

> When executing a scan, within or across a partition, tombstones must be kept in memory to allow returning them to the coordinator. The coordinator uses them to ensure other replicas know about the deleted rows. Workloads that generate numerous tombstones may cause performance problems and exhaust the server heap. See Cassandra anti-patterns: Queues and queue-like datasets. Adjust these thresholds only if you understand the impact and want to scan more tombstones. Additionally, you can adjust these thresholds at runtime using the StorageServiceMBean.   Related information: Cassandra anti-patterns: Queues and queue-like datasets.

<a id="confprop-tombstone-warn-threshold"></a>

### tombstone_warn_threshold

> The maximum number of tombstones a query can scan before warning.

> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-query-tombstone-page-limit"></a>

### query_tombstone_page_limit

> The number of tombstones after which a query cuts a page, even if not full or even empty.

> * **Type:** `uint64_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-query-page-size-in-bytes"></a>

### query_page_size_in_bytes

> The size of pages in bytes, after a page accumulates this much data, the page is cut and sent to the client.     Setting a too large value increases the risk of OOM.

> * **Type:** `uint64_t`
> * **Default value:** `1 << 20`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confgroup-network-timeout-settings"></a>

## Network timeout settings

<a id="confprop-range-request-timeout-in-ms"></a>

### range_request_timeout_in_ms

> The time in milliseconds that the coordinator waits for sequential or index scans to complete.

> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-read-request-timeout-in-ms"></a>

### read_request_timeout_in_ms

> The time that the coordinator waits for read operations to complete

> * **Type:** `uint32_t`
> * **Default value:** `5000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-counter-write-request-timeout-in-ms"></a>

### counter_write_request_timeout_in_ms

> The time that the coordinator waits for counter writes to complete.

> * **Type:** `uint32_t`
> * **Default value:** `5000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-cas-contention-timeout-in-ms"></a>

### cas_contention_timeout_in_ms

> The time that the coordinator continues to retry a CAS (compare and set) operation that contends with other proposals for the same row.

> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-truncate-request-timeout-in-ms"></a>

### truncate_request_timeout_in_ms

> The time that the coordinator waits for truncates (remove all data from a table) to complete. The long default value allows for a snapshot to be taken before removing the data. If auto_snapshot is disabled (not recommended), you can reduce this time.

> * **Type:** `uint32_t`
> * **Default value:** `60000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-write-request-timeout-in-ms"></a>

### write_request_timeout_in_ms

> The time in milliseconds that the coordinator waits for write operations to complete.

> > Related information: About hinted handoff writes
> * **Type:** `uint32_t`
> * **Default value:** `2000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-request-timeout-in-ms"></a>

### request_timeout_in_ms

> The default timeout for other, miscellaneous operations.

> > Related information: About hinted handoff writes
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confgroup-inter-node-settings"></a>

## Inter-node settings

<a id="confprop-internode-compression"></a>

### internode_compression

> Controls whether traffic between nodes is compressed. The valid values are:
> : * all: All traffic is compressed.
>   * dc: Traffic between data centers is compressed.
>   * none: No compression.

> * **Type:** `sstring`
> * **Default value:** `"none"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-inter-dc-tcp-nodelay"></a>

### inter_dc_tcp_nodelay

> Enable or disable tcp_nodelay for inter-data center communication. When disabled larger, but fewer, network packets are sent. This reduces overhead from the TCP protocol itself. However, if cross data-center responses are blocked, it will increase latency.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-native-transport-cql-binary-protocol"></a>

## Native transport (CQL Binary Protocol)

<a id="confprop-start-native-transport"></a>

### start_native_transport

> Enable or disable the native transport server. Uses the same address as the rpc_address, but the port is different from the rpc_port. See native_transport_port.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-native-transport-port"></a>

### native_transport_port

> Port on which the CQL native transport listens for clients.

> * **Type:** `uint16_t`
> * **Default value:** `9042`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-maintenance-socket"></a>

### maintenance_socket

> The Unix Domain Socket the node uses for maintenance socket.
> : The possible options are:
>   <br/>
>   > ignore         the node will not open the maintenance socket.
>   <br/>
>   > workdir        the node will open the maintenance socket on the path <scylla’s workdir>/cql.m,
>   <br/>
>   > > where <scylla’s workdir> is a path defined by the workdir configuration option
>   <br/>
>   > <socket path>  the node will open the maintenance socket on the path <socket path>

> * **Type:** `sstring`
> * **Default value:** `"ignore"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-maintenance-socket-group"></a>

### maintenance_socket_group

> The group that the maintenance socket will be owned by. If not set, the group will be the same as the user running the scylla node.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-maintenance-mode"></a>

### maintenance_mode

> If set to true, the node will not connect to other nodes. It will only serve requests to its local data.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-native-transport-port-ssl"></a>

### native_transport_port_ssl

> Port on which the CQL TLS native transport listens for clients.    Enabling client encryption and keeping native_transport_port_ssl disabled will use encryption    for native_transport_port. Setting native_transport_port_ssl to a different value    from native_transport_port will use encryption for native_transport_port_ssl while    keeping native_transport_port unencrypted.

> * **Type:** `uint16_t`
> * **Default value:** `9142`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-native-shard-aware-transport-port"></a>

### native_shard_aware_transport_port

> Like native_transport_port, but clients-side port number (modulo smp) is used to route the connection to the specific shard.

> * **Type:** `uint16_t`
> * **Default value:** `19042`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-native-shard-aware-transport-port-ssl"></a>

### native_shard_aware_transport_port_ssl

> Like native_transport_port_ssl, but clients-side port number (modulo smp) is used to route the connection to the specific shard.

> * **Type:** `uint16_t`
> * **Default value:** `19142`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-rpc-remote-procedure-call-settings"></a>

## RPC (remote procedure call) settings

> Settings for configuring and tuning client connections.

<a id="confprop-broadcast-rpc-address"></a>

### broadcast_rpc_address

> RPC address to broadcast to drivers and other Scylla nodes. This cannot be set to 0.0.0.0. If blank, it is set to the value of the rpc_address or rpc_interface. If rpc_address or rpc_interfaceis set to 0.0.0.0, this property must be set.

> * **Type:** `sstring`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-rpc-port"></a>

### rpc_port

> Thrift port for client connections.

> * **Type:** `uint16_t`
> * **Default value:** `9160`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-start-rpc"></a>

### start_rpc

> Starts the Thrift RPC server

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-rpc-keepalive"></a>

### rpc_keepalive

> Enable or disable keepalive on client connections (RPC or native).

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-cache-hit-rate-read-balancing"></a>

### cache_hit_rate_read_balancing

> This boolean controls whether the replicas for read query will be chosen based on cache hit ratio.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-advanced-fault-detection-settings"></a>

## Advanced fault detection settings

> Settings to handle poorly performing or failing nodes.

<a id="confprop-hinted-handoff-enabled"></a>

### hinted_handoff_enabled

> Enable or disable hinted handoff. To enable per data center, add data center list. For example: hinted_handoff_enabled: DC1,DC2. A hint indicates that the write needs to be replayed to an unavailable node.     
> : Related information: About hinted handoff writes

> * **Type:** `hinted_handoff_enabled_type`
> * **Default value:** `db::config::hinted_handoff_enabled_type(db::config::hinted_handoff_enabled_type::enabled_for_all_tag())`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-max-hinted-handoff-concurrency"></a>

### max_hinted_handoff_concurrency

> Maximum concurrency allowed for sending hints. The concurrency is divided across shards and rounded up if not divisible by the number of shards. By default (or when set to 0), concurrency of 8\*shard_count will be used.

> * **Type:** `uint32_t`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-max-hint-window-in-ms"></a>

### max_hint_window_in_ms

> Maximum amount of time that hints are generates hints for an unresponsive node. After this interval, new hints are no longer generated until the node is back up and responsive. If the node goes down again, a new interval begins. This setting can prevent a sudden demand for resources when a node is brought back online and the rest of the cluster attempts to replay a large volume of hinted writes.

> > Related information: Failure detection and recovery
> * **Type:** `uint32_t`
> * **Default value:** `10800000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-thrift-interface-properties"></a>

## Thrift interface properties

> Legacy API for older clients. CQL is a simpler and better API for Scylla.

<a id="confprop-thrift-max-message-length-in-mb"></a>

### thrift_max_message_length_in_mb

> The maximum length of a Thrift message in megabytes, including all fields and internal Thrift overhead (1 byte of overhead for each frame). Message length is usually used in conjunction with batches. A frame length greater than or equal to 24 accommodates a batch with four inserts, each of which is 24 bytes. The required message length is greater than or equal to 24+24+24+24+4 (number of frames).

> * **Type:** `uint32_t`
> * **Default value:** `16`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confgroup-security-properties"></a>

## Security properties

> Server and client security settings.

<a id="confprop-authenticator"></a>

### authenticator

> The authentication backend, used to identify users. The available authenticators are:
> : * org.apache.cassandra.auth.AllowAllAuthenticator: Disables authentication; no checks are performed.
>   * org.apache.cassandra.auth.PasswordAuthenticator: Authenticates users with user names and hashed passwords stored in the system_auth.credentials table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.
>   * com.scylladb.auth.CertificateAuthenticator: Authenticates users based on TLS certificate authentication subject. Roles and permissions still need to be defined as normal. Super user can be set using the ‘auth_superuser_name’ configuration value. Query to extract role name from subject string is set using ‘auth_certificate_role_queries’.
>   * com.scylladb.auth.TransitionalAuthenticator: Wraps around the PasswordAuthenticator, logging them in if username/password pair provided is correct and treating them as anonymous users otherwise.
>   <br/>
>   Related information: Internal authentication

> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.auth.AllowAllAuthenticator"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-authorizer"></a>

### authorizer

> The authorization backend. It implements IAuthenticator, which limits access and provides permissions. The available authorizers are:
> : * AllowAllAuthorizer: Disables authorization; allows any action to any user.
>   * CassandraAuthorizer: Stores permissions in system_auth.permissions table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.
>   * com.scylladb.auth.TransitionalAuthorizer: Wraps around the CassandraAuthorizer, which is used to authorize permission management. Other actions are allowed for all users.
>   <br/>
>   Related information: Object permissions

> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.auth.AllowAllAuthorizer"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-role-manager"></a>

### role_manager

> The role-management backend, used to maintain grants and memberships between roles.    The available role-managers are:
> : * CassandraRoleManager: Stores role data in the system_auth keyspace.

> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.auth.CassandraRoleManager"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-permissions-validity-in-ms"></a>

### permissions_validity_in_ms

> How long permissions in cache remain valid. Depending on the authorizer, such as CassandraAuthorizer, fetching permissions can be resource intensive. Permissions caching is disabled when this property is set to 0 or when AllowAllAuthorizer is used. The cached value is considered valid as long as both its value is not older than the permissions_validity_in_ms     and the cached value has been read at least once during the permissions_validity_in_ms time frame. If any of these two conditions doesn’t hold the cached value is going to be evicted from the cache.

> > Related information: Object permissions
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-permissions-update-interval-in-ms"></a>

### permissions_update_interval_in_ms

> Refresh interval for permissions cache (if enabled). After this interval, cache entries become eligible for refresh. An async reload is scheduled every permissions_update_interval_in_ms time period and the old value is returned until it completes. If permissions_validity_in_ms has a non-zero value, then this property must also have a non-zero value. It’s recommended to set this value to be at least 3 times smaller than the permissions_validity_in_ms.

> * **Type:** `uint32_t`
> * **Default value:** `2000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-permissions-cache-max-entries"></a>

### permissions_cache_max_entries

> Maximum cached permission entries. Must have a non-zero value if permissions caching is enabled (see a permissions_validity_in_ms description).

> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-server-encryption-options"></a>

### server_encryption_options

> Enable or disable inter-node encryption. You must also generate keys and provide the appropriate key and trust store locations and passwords. The available options are:
> : * internode_encryption: (Default: none) Enable or disable encryption of inter-node communication using the TLS_RSA_WITH_AES_128_CBC_SHA cipher suite for authentication, key exchange, and encryption of data transfers. The available inter-node options are:
>     : * all: Encrypt all inter-node communications.
>       * none: No encryption.
>       * dc: Encrypt the traffic between the data centers (server only).
>       * rack: Encrypt the traffic between the racks(server only).
>   * certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the internode communication.
>   * keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.
>   * truststore: (Default: <not set, use system truststore> ) Location of the truststore containing the trusted certificate for authenticating remote servers.
>   * certficate_revocation_list: (Default: <not set>) PEM encoded certificate revocation list.
>   <br/>
>   The advanced settings are:
>   <br/>
>   * priority_string: (Default: not set, use default) GnuTLS priority string controlling TLS algorithms used/allowed.
>   * require_client_auth: (Default: false ) Enables or disables certificate authentication.
>   <br/>
>   Related information: Node-to-node encryption

> * **Type:** `string_map`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-client-encryption-options"></a>

### client_encryption_options

> Enable or disable client-to-node encryption. You must also generate keys and provide the appropriate key and certificate. The available options are:
> : * enabled: (Default: false) To enable, set to true.
>   * certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.
>   * keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.
>   * truststore: (Default: <not set. use system truststore>) Location of the truststore containing the trusted certificate for authenticating remote servers.
>   * certficate_revocation_list: (Default: <not set> ) PEM encoded certificate revocation list.
>   <br/>
>   The advanced settings are:
>   <br/>
>   * priority_string: (Default: not set, use default) GnuTLS priority string controlling TLS algorithms used/allowed.
>   * require_client_auth: (Default: false) Enables or disables certificate authentication.
>   <br/>
>   Related information: Client-to-node encryption

> * **Type:** `string_map`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-encryption-options"></a>

### alternator_encryption_options

> When Alternator via HTTPS is enabled with alternator_https_port, where to take the key and certificate. The available options are:
> : * certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.
>   * keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.
>   <br/>
>   The advanced settings are:
>   <br/>
>   * priority_string: GnuTLS priority string controlling TLS algorithms used/allowed.

> * **Type:** `string_map`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-ssl-storage-port"></a>

### ssl_storage_port

> The SSL port for encrypted communication. Unused unless enabled in encryption_options.

> * **Type:** `uint32_t`
> * **Default value:** `7001`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-in-memory-data-store"></a>

### enable_in_memory_data_store

> Enable in memory mode (system tables are always persisted).

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-cache"></a>

### enable_cache

> Enable cache.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-commitlog"></a>

### enable_commitlog

> Enable commitlog.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-volatile-system-keyspace-for-testing"></a>

### volatile_system_keyspace_for_testing

> Don’t persist system keyspace - testing only!

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-api-port"></a>

### api_port

> Http Rest API port.

> * **Type:** `uint16_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-api-address"></a>

### api_address

> Http Rest API address.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-api-ui-dir"></a>

### api_ui_dir

> The directory location of the API GUI.

> * **Type:** `sstring`
> * **Default value:** `"swagger-ui/dist/"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-api-doc-dir"></a>

### api_doc_dir

> The API definition file directory.

> * **Type:** `sstring`
> * **Default value:** `"api/api-doc/"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-consistent-rangemovement"></a>

### consistent_rangemovement

> When set to true, range movements will be consistent. It means: 1) it will refuse to bootstrap a new node if other bootstrapping/leaving/moving nodes detected. 2) data will be streamed to a new node only from the node which is no longer responsible for the token range. Same as -Dcassandra.consistent.rangemovement in cassandra.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-load-ring-state"></a>

### load_ring_state

> When set to true, load tokens and host_ids previously saved. Same as -Dcassandra.load_ring_state in cassandra.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-replace-node-first-boot"></a>

### replace_node_first_boot

> The Host ID of a dead node to replace. If the replacing node has already been bootstrapped successfully, this option will be ignored.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-replace-address"></a>

### replace_address

> [[deprecated]] The listen_address or broadcast_address of the dead node to replace. Same as -Dcassandra.replace_address.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-replace-address-first-boot"></a>

### replace_address_first_boot

> [[deprecated]] Like replace_address option, but if the node has been bootstrapped successfully it will be ignored. Same as -Dcassandra.replace_address_first_boot.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-ignore-dead-nodes-for-replace"></a>

### ignore_dead_nodes_for_replace

> List dead nodes to ignore for replace operation using a comma-separated list of host IDs. E.g., scylla –ignore-dead-nodes-for-replace 8d5ed9f4-7764-4dbd-bad8-43fddce94b7c,125ed9f4-7777-1dbn-mac8-43fddce9123e

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-repair-based-node-ops"></a>

### enable_repair_based_node_ops

> Set true to use enable repair based node operations instead of streaming based.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-allowed-repair-based-node-ops"></a>

### allowed_repair_based_node_ops

> A comma separated list of node operations which are allowed to enable repair based node operations. The operations can be bootstrap, replace, removenode, decommission and rebuild.

> * **Type:** `sstring`
> * **Default value:** `"replace,removenode,rebuild,bootstrap,decommission"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-enable-compacting-data-for-streaming-and-repair"></a>

### enable_compacting_data_for_streaming_and_repair

> Enable the compacting reader, which compacts the data for streaming and repair (load’n’stream included) before sending it to, or synchronizing it with peers. Can reduce the amount of data to be processed by removing dead data, but adds CPU overhead.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-repair-partition-count-estimation-ratio"></a>

### repair_partition_count_estimation_ratio

> Specify the fraction of partitions written by repair out of the total partitions. The value is currently only used for bloom filter estimation. Value is between 0 and 1.

> * **Type:** `double`
> * **Default value:** `0.1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-ring-delay-ms"></a>

### ring_delay_ms

> Time a node waits to hear from other nodes before joining the ring in milliseconds. Same as -Dcassandra.ring_delay_ms in cassandra.

> * **Type:** `uint32_t`
> * **Default value:** `30 * 1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-shadow-round-ms"></a>

### shadow_round_ms

> The maximum gossip shadow round time. Can be used to reduce the gossip feature check time during node boot up.

> * **Type:** `uint32_t`
> * **Default value:** `300 * 1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-fd-max-interval-ms"></a>

### fd_max_interval_ms

> The maximum failure_detector interval time in milliseconds. Interval larger than the maximum will be ignored. Larger cluster may need to increase the default.

> * **Type:** `uint32_t`
> * **Default value:** `2 * 1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-fd-initial-value-ms"></a>

### fd_initial_value_ms

> The initial failure_detector interval time in milliseconds.

> * **Type:** `uint32_t`
> * **Default value:** `2 * 1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-shutdown-announce-in-ms"></a>

### shutdown_announce_in_ms

> Time a node waits after sending gossip shutdown message in milliseconds. Same as -Dcassandra.shutdown_announce_in_ms in cassandra.

> * **Type:** `uint32_t`
> * **Default value:** `2 * 1000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-developer-mode"></a>

### developer_mode

> Relax environment checks. Setting to true can reduce performance and reliability significantly.

> * **Type:** `bool`
> * **Default value:** `DEVELOPER_MODE_DEFAULT`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-skip-wait-for-gossip-to-settle"></a>

### skip_wait_for_gossip_to_settle

> An integer to configure the wait for gossip to settle. -1: wait normally, 0: do not wait at all, n: wait for at most n polls. Same as -Dcassandra.skip_wait_for_gossip_to_settle in cassandra.

> * **Type:** `int32_t`
> * **Default value:** `-1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-force-gossip-generation"></a>

### force_gossip_generation

> Force gossip to use the generation number provided by user.

> * **Type:** `int32_t`
> * **Default value:** `-1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-lsa-reclamation-step"></a>

### lsa_reclamation_step

> Minimum number of segments to reclaim in a single step.

> * **Type:** `size_t`
> * **Default value:** `1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-prometheus-port"></a>

### prometheus_port

> Prometheus port, set to zero to disable.

> * **Type:** `uint16_t`
> * **Default value:** `9180`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-prometheus-address"></a>

### prometheus_address

> Prometheus listening address, defaulting to listen_address if not explicitly set.

> * **Type:** `sstring`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-prometheus-prefix"></a>

### prometheus_prefix

> Set the prefix of the exported Prometheus metrics. Changing this will break Scylla’s dashboard compatibility, do not change unless you know what you are doing.

> * **Type:** `sstring`
> * **Default value:** `"scylla"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-prometheus-allow-protobuf"></a>

### prometheus_allow_protobuf

> If set allows the experimental Prometheus protobuf with native histogram

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-abort-on-lsa-bad-alloc"></a>

### abort_on_lsa_bad_alloc

> Abort when allocation in LSA region fails.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-murmur3-partitioner-ignore-msb-bits"></a>

### murmur3_partitioner_ignore_msb_bits

> Number of most significant token bits to ignore in murmur3 partitioner; increase for very large clusters.

> * **Type:** `unsigned`
> * **Default value:** `default_murmur3_partitioner_ignore_msb_bits`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-unspooled-dirty-soft-limit"></a>

### unspooled_dirty_soft_limit

> Soft limit of unspooled dirty memory expressed as a portion of the hard limit.

> * **Type:** `double`
> * **Default value:** `0.6`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-sstable-summary-ratio"></a>

### sstable_summary_ratio

> Enforces that 1 byte of summary is written for every N (2000 by default)    bytes written to data file. Value must be between 0 and 1.

> * **Type:** `double`
> * **Default value:** `0.0005`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-components-memory-reclaim-threshold"></a>

### components_memory_reclaim_threshold

> Ratio of available memory for all in-memory components of SSTables in a shard beyond which the memory will be reclaimed from components until it falls back under the threshold. Currently, this limit is only enforced for bloom filters.

> * **Type:** `double`
> * **Default value:** `.2`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-large-memory-allocation-warning-threshold"></a>

### large_memory_allocation_warning_threshold

> Warn about memory allocations above this size; set to zero to disable.

> * **Type:** `size_t`
> * **Default value:** `size_t(1) << 20`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-deprecated-partitioners"></a>

### enable_deprecated_partitioners

> Enable the byteordered and random partitioners. These partitioners are deprecated and will be removed in a future version.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-keyspace-column-family-metrics"></a>

### enable_keyspace_column_family_metrics

> Enable per keyspace and per column family metrics reporting.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-node-aggregated-table-metrics"></a>

### enable_node_aggregated_table_metrics

> Enable aggregated per node, per keyspace and per table metrics reporting, applicable if enable_keyspace_column_family_metrics is false.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-sstable-data-integrity-check"></a>

### enable_sstable_data_integrity_check

> Enable interposer which checks for integrity of every sstable write.     Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-sstable-key-validation"></a>

### enable_sstable_key_validation

> Enable validation of partition and clustering keys monotonicity     Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.

> * **Type:** `bool`
> * **Default value:** `ENABLE_SSTABLE_KEY_VALIDATION`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-cpu-scheduler"></a>

### cpu_scheduler

> Enable cpu scheduling.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-view-building"></a>

### view_building

> Enable view building; should only be set to false when the node is experience issues due to view building.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-sstable-format"></a>

### sstable_format

> Default sstable file format

> * **Type:** `sstring`
> * **Default value:** `"me"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-uuid-sstable-identifiers-enabled"></a>

### uuid_sstable_identifiers_enabled

> If set to true, each newly created sstable will have a UUID     based generation identifier, and such files are not readable by previous Scylla versions.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-table-digest-insensitive-to-expiry"></a>

### table_digest_insensitive_to_expiry

> When enabled, per-table schema digest calculation ignores empty partitions.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-dangerous-direct-import-of-cassandra-counters"></a>

### enable_dangerous_direct_import_of_cassandra_counters

> Only turn this option on if you want to import tables from Cassandra containing counters, and you are SURE that no counters in that table were created in a version earlier than Cassandra 2.1.     It is not enough to have ever since upgraded to newer versions of Cassandra. If you EVER used a version earlier than 2.1 in the cluster where these SSTables come from, DO NOT TURN ON THIS OPTION! You will corrupt your data. You have been warned.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-shard-aware-drivers"></a>

### enable_shard_aware_drivers

> Enable native transport drivers to use connection-per-shard for better performance.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-ipv6-dns-lookup"></a>

### enable_ipv6_dns_lookup

> Use IPv6 address resolution

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-abort-on-internal-error"></a>

### abort_on_internal_error

> Abort the server instead of throwing exception when internal invariants are violated.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-max-partition-key-restrictions-per-query"></a>

### max_partition_key_restrictions_per_query

> Maximum number of distinct partition keys restrictions per query. This limit places a bound on the size of IN tuples,     especially when multiple partition key columns have IN restrictions. Increasing this value can result in server instability.

> * **Type:** `uint32_t`
> * **Default value:** `100`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-max-clustering-key-restrictions-per-query"></a>

### max_clustering_key_restrictions_per_query

> Maximum number of distinct clustering key restrictions per query. This limit places a bound on the size of IN tuples,     especially when multiple clustering key columns have IN restrictions. Increasing this value can result in server instability.

> * **Type:** `uint32_t`
> * **Default value:** `100`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-max-memory-for-unlimited-query-soft-limit"></a>

### max_memory_for_unlimited_query_soft_limit

> Maximum amount of memory a query, whose memory consumption is not naturally limited, is allowed to consume, e.g. non-paged and reverse queries.     This is the soft limit, there will be a warning logged for queries violating this limit.

> * **Type:** `uint64_t`
> * **Default value:** `uint64_t(1) << 20`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-max-memory-for-unlimited-query-hard-limit"></a>

### max_memory_for_unlimited_query_hard_limit

> Maximum amount of memory a query, whose memory consumption is not naturally limited, is allowed to consume, e.g. non-paged and reverse queries.     This is the hard limit, queries violating this limit will be aborted.

> * **Type:** `uint64_t`
> * **Default value:** `(uint64_t(100) << 20)`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-reader-concurrency-semaphore-serialize-limit-multiplier"></a>

### reader_concurrency_semaphore_serialize_limit_multiplier

> Start serializing reads after their collective memory consumption goes above $normal_limit \* $multiplier.

> * **Type:** `uint32_t`
> * **Default value:** `2`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-reader-concurrency-semaphore-kill-limit-multiplier"></a>

### reader_concurrency_semaphore_kill_limit_multiplier

> Start killing reads after their collective memory consumption goes above $normal_limit \* $multiplier.

> * **Type:** `uint32_t`
> * **Default value:** `4`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-reader-concurrency-semaphore-cpu-concurrency"></a>

### reader_concurrency_semaphore_cpu_concurrency

> Admit new reads while there are less than this number of requests that need CPU.

> * **Type:** `uint32_t`
> * **Default value:** `1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-twcs-max-window-count"></a>

### twcs_max_window_count

> The maximum number of compaction windows allowed when making use of TimeWindowCompactionStrategy. A setting of 0 effectively disables the restriction.

> * **Type:** `uint32_t`
> * **Default value:** `50`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-initial-sstable-loading-concurrency"></a>

### initial_sstable_loading_concurrency

> Maximum amount of sstables to load in parallel during initialization. A higher number can lead to more memory consumption. You should not need to touch this.

> * **Type:** `unsigned`
> * **Default value:** `4u`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-3-1-0-compatibility-mode"></a>

### enable_3_1_0_compatibility_mode

> Set to true if the cluster was initially installed from 3.1.0. If it was upgraded from an earlier version,     or installed from a later version, leave this set to false. This adjusts the communication protocol to     work around a bug in Scylla 3.1.0.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-user-defined-functions"></a>

### enable_user_defined_functions

> Enable user defined functions. You must also set \`\`experimental-features=udf\`\`.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-user-defined-function-time-limit-ms"></a>

### user_defined_function_time_limit_ms

> The time limit for each UDF invocation.

> * **Type:** `unsigned`
> * **Default value:** `10`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-user-defined-function-allocation-limit-bytes"></a>

### user_defined_function_allocation_limit_bytes

> How much memory each UDF invocation can allocate.

> * **Type:** `unsigned`
> * **Default value:** `1024*1024`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-user-defined-function-contiguous-allocation-limit-bytes"></a>

### user_defined_function_contiguous_allocation_limit_bytes

> How much memory each UDF invocation can allocate in one chunk.

> * **Type:** `unsigned`
> * **Default value:** `1024*1024`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-schema-registry-grace-period"></a>

### schema_registry_grace_period

> Time period in seconds after which unused schema versions will be evicted from the local schema registry cache. Default is 1 second.

> * **Type:** `uint32_t`
> * **Default value:** `1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-max-concurrent-requests-per-shard"></a>

### max_concurrent_requests_per_shard

> Maximum number of concurrent requests a single shard can handle before it starts shedding extra load. By default, no requests will be shed.

> * **Type:** `uint32_t`
> * **Default value:** `std::numeric_limits<uint32_t>::max()`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-cdc-dont-rewrite-streams"></a>

### cdc_dont_rewrite_streams

> Disable rewriting streams from cdc_streams_descriptions to cdc_streams_descriptions_v2. Should not be necessary, but the procedure is expensive and prone to failures; this config option is left as a backdoor in case some user requires manual intervention.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-strict-allow-filtering"></a>

### strict_allow_filtering

> Match Cassandra in requiring ALLOW FILTERING on slow queries. Can be true, false, or warn. When false, Scylla accepts some slow queries even without ALLOW FILTERING that Cassandra rejects. Warn is same as false, but with warning.

> * **Type:** `tri_mode_restriction`
> * **Default value:** `strict_allow_filtering_default()`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-strict-is-not-null-in-views"></a>

### strict_is_not_null_in_views

> In materialized views, restrictions are allowed only on the view’s primary key columns.
> : In old versions Scylla mistakenly allowed IS NOT NULL restrictions on columns which were not part of the view’s     primary key. These invalid restrictions were ignored.
>   This option controls the behavior when someone tries to create a view with such invalid IS NOT NULL restrictions.
>   <br/>
>   Can be true, false, or warn:
>   : * \`true\`: IS NOT NULL is allowed only on the view’s primary key columns,     trying to use it on other columns will cause an error, as it should.
>     * \`false\`: Scylla accepts IS NOT NULL restrictions on regular columns, but they’re silently ignored.     It’s useful for backwards compatibility.
>     * \`warn\`: The same as false, but there’s a warning about invalid view restrictions.
>   <br/>
>   To preserve backwards compatibility on old clusters, Scylla’s default setting is \`warn\`.     New clusters have this option set to \`true\` by scylla.yaml (which overrides the default \`warn\`),     to make sure that trying to create an invalid view causes an error.

> * **Type:** `tri_mode_restriction`
> * **Default value:** `db::tri_mode_restriction_t::mode::WARN`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-reversed-reads-auto-bypass-cache"></a>

### reversed_reads_auto_bypass_cache

> Bypass in-memory data cache (the row cache) when performing reversed queries.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-enable-optimized-reversed-reads"></a>

### enable_optimized_reversed_reads

> Use a new optimized algorithm for performing reversed reads.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-enable-cql-config-updates"></a>

### enable_cql_config_updates

> Make the system.config table UPDATEable.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-enable-parallelized-aggregation"></a>

### enable_parallelized_aggregation

> Use on a new, parallel algorithm for performing aggregate queries.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-cql-duplicate-bind-variable-names-refer-to-same-variable"></a>

### cql_duplicate_bind_variable_names_refer_to_same_variable

> A bind variable that appears twice in a CQL query refers to a single variable (if false, no name matching is performed).

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-alternator-port"></a>

### alternator_port

> Alternator API port.

> * **Type:** `uint16_t`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-https-port"></a>

### alternator_https_port

> Alternator API HTTPS port.

> * **Type:** `uint16_t`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-address"></a>

### alternator_address

> Alternator API listening address.

> * **Type:** `sstring`
> * **Default value:** `"0.0.0.0"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-enforce-authorization"></a>

### alternator_enforce_authorization

> Enforce checking the authorization header for every request in Alternator.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-write-isolation"></a>

### alternator_write_isolation

> Default write isolation policy for Alternator.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-streams-time-window-s"></a>

### alternator_streams_time_window_s

> CDC query confidence window for alternator streams.

> * **Type:** `uint32_t`
> * **Default value:** `10`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-timeout-in-ms"></a>

### alternator_timeout_in_ms

> The server-side timeout for completing Alternator API requests.

> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-alternator-ttl-period-in-seconds"></a>

### alternator_ttl_period_in_seconds

> The default period for Alternator’s expiration scan. Alternator attempts to scan every table within that period.

> * **Type:** `double`
> * **Default value:** `60*60*24`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-alternator-describe-endpoints"></a>

### alternator_describe_endpoints

> Overrides the behavior of Alternator’s DescribeEndpoints operation.     An empty value (the default) means DescribeEndpoints will return     the same endpoint used in the request. The string ‘disabled’     disables the DescribeEndpoints operation. Any other string is the     fixed value that will be returned by DescribeEndpoints operations.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-abort-on-ebadf"></a>

### abort_on_ebadf

> Abort the server on incorrect file descriptor access. Throws exception when disabled.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-redis-port"></a>

### redis_port

> Port on which the REDIS transport listens for clients.

> * **Type:** `uint16_t`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-redis-ssl-port"></a>

### redis_ssl_port

> Port on which the REDIS TLS native transport listens for clients.

> * **Type:** `uint16_t`
> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-redis-read-consistency-level"></a>

### redis_read_consistency_level

> Consistency level for read operations for redis.

> * **Type:** `sstring`
> * **Default value:** `"LOCAL_QUORUM"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-redis-write-consistency-level"></a>

### redis_write_consistency_level

> Consistency level for write operations for redis.

> * **Type:** `sstring`
> * **Default value:** `"LOCAL_QUORUM"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-redis-database-count"></a>

### redis_database_count

> Database count for the redis. You can use the default settings (16).

> * **Type:** `uint16_t`
> * **Default value:** `16`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-redis-keyspace-replication-strategy"></a>

### redis_keyspace_replication_strategy

> Set the replication strategy for the redis keyspace. The setting is used by the first node in the boot phase when the keyspace is not exists to create keyspace for redis.
> : The replication strategy determines how many copies of the data are kept in a given data center. This setting impacts consistency, availability and request speed.
>   Two strategies are available: SimpleStrategy and NetworkTopologyStrategy.
>   <br/>
>   * class: (Default: SimpleStrategy ). Set the replication strategy for redis keyspace.
>   * ‘replication_factor’: N, (Default: ‘replication_factor’:1) IFF the class is SimpleStrategy, assign the same replication factor to the entire cluster.
>   * ‘datacenter_name’: N [,…], (Default: ‘dc1:1’) IFF the class is NetworkTopologyStrategy, assign replication factors to each data center in a comma separated list.
>   <br/>
>   Related information: About replication strategy.

> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-sanitizer-report-backtrace"></a>

### sanitizer_report_backtrace

> In debug mode, report log-structured allocator sanitizer violations with a backtrace. Slow.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-flush-schema-tables-after-modification"></a>

### flush_schema_tables_after_modification

> Flush tables in the system_schema keyspace after schema modification. This is required for crash recovery, but slows down tests and can be disabled for them

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-restrict-twcs-without-default-ttl"></a>

### restrict_twcs_without_default_ttl

> Controls whether to prevent creating TimeWindowCompactionStrategy tables without a default TTL. Can be true, false, or warn.

> * **Type:** `tri_mode_restriction`
> * **Default value:** `db::tri_mode_restriction_t::mode::WARN`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-restrict-future-timestamp"></a>

### restrict_future_timestamp

> Controls whether to detect and forbid unreasonable USING TIMESTAMP, more than 3 days into the future.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-unsafe-ignore-truncation-record"></a>

### unsafe_ignore_truncation_record

> Ignore truncation record stored in system tables as if tables were never truncated.

> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-task-ttl-in-seconds"></a>

### task_ttl_in_seconds

> Time for which information about finished task stays in memory.

> * **Default value:** `0`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-nodeops-watchdog-timeout-seconds"></a>

### nodeops_watchdog_timeout_seconds

> Time in seconds after which node operations abort when not hearing from the coordinator.

> * **Type:** `uint32_t`
> * **Default value:** `120`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-nodeops-heartbeat-interval-seconds"></a>

### nodeops_heartbeat_interval_seconds

> Period of heartbeat ticks in node operations.

> * **Type:** `uint32_t`
> * **Default value:** `10`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-cache-index-pages"></a>

### cache_index_pages

> Keep SSTable index pages in the global cache after a SSTable read. Expected to improve performance for workloads with big partitions, but may degrade performance for workloads with small partitions. The amount of memory usable by index cache is limited with \`\`index_cache_fraction\`\`.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-index-cache-fraction"></a>

### index_cache_fraction

> The maximum fraction of cache memory permitted for use by index cache. Clamped to the [0.0; 1.0] range. Must be small enough to not deprive the row cache of memory, but should be big enough to fit a large fraction of the index. The default value 0.2 means that at least 80% of cache memory is reserved for the row cache, while at most 20% is usable by the index cache.

> * **Type:** `double`
> * **Default value:** `0.2`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-force-gossip-topology-changes"></a>

### force_gossip_topology_changes

> Force gossip-based topology operations in a fresh cluster. Only the first node in the cluster must use it. The rest will fall back to gossip-based operations anyway. This option should be used only for testing.

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-wasm-cache-memory-fraction"></a>

### wasm_cache_memory_fraction

> Maximum total size of all WASM instances stored in the cache as fraction of total shard memory.

> * **Type:** `double`
> * **Default value:** `0.01`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-wasm-cache-timeout-in-ms"></a>

### wasm_cache_timeout_in_ms

> Time after which an instance is evicted from the cache.

> * **Type:** `uint32_t`
> * **Default value:** `5000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-wasm-cache-instance-size-limit"></a>

### wasm_cache_instance_size_limit

> Instances with size above this limit will not be stored in the cache.

> * **Type:** `size_t`
> * **Default value:** `1024*1024`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-wasm-udf-yield-fuel"></a>

### wasm_udf_yield_fuel

> Wasmtime fuel a WASM UDF can consume before yielding.

> * **Type:** `uint64_t`
> * **Default value:** `100000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-wasm-udf-total-fuel"></a>

### wasm_udf_total_fuel

> Wasmtime fuel a WASM UDF can consume before termination.

> * **Type:** `uint64_t`
> * **Default value:** `100000000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-wasm-udf-memory-limit"></a>

### wasm_udf_memory_limit

> How much memory each WASM UDF can allocate at most.

> * **Type:** `size_t`
> * **Default value:** `2*1024*1024`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-relabel-config-file"></a>

### relabel_config_file

> Optionally, read relabel config from file.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-object-storage-config-file"></a>

### object_storage_config_file

> Optionally, read object-storage endpoints config from file.

> * **Type:** `sstring`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-live-updatable-config-params-changeable-via-cql"></a>

### live_updatable_config_params_changeable_via_cql

> If set to true, configuration parameters defined with LiveUpdate can be updated in runtime via CQL (by updating system.config virtual table), otherwise they can’t.

> * **Type:** `bool`
> * **Default value:** `true`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-auth-superuser-name"></a>

### auth_superuser_name

> Initial authentication super username. Ignored if authentication tables already contain a super user.

> * **Type:** `std::string`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-auth-superuser-salted-password"></a>

### auth_superuser_salted_password

> Initial authentication super user salted password. Create using mkpassword or similar. The hashing algorithm used must be available on the node host.     Ignored if authentication tables already contain a super user password.

> * **Type:** `std::string`
> * **Default value:** `""`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-auth-certificate-role-queries"></a>

### auth_certificate_role_queries

> SUBJECT }, {query

> * **Type:** `std::vector<std::unordered_map<sstring, sstring>>`
> * **Default value:** `{ { { "source"`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-minimum-replication-factor-fail-threshold"></a>

### minimum_replication_factor_fail_threshold

> * **Type:** `int`
> * **Default value:** `-1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-minimum-replication-factor-warn-threshold"></a>

### minimum_replication_factor_warn_threshold

> * **Type:** `int`
> * **Default value:** `3`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-maximum-replication-factor-warn-threshold"></a>

### maximum_replication_factor_warn_threshold

> * **Type:** `int`
> * **Default value:** `-1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-maximum-replication-factor-fail-threshold"></a>

### maximum_replication_factor_fail_threshold

> * **Type:** `int`
> * **Default value:** `-1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-tablets-initial-scale-factor"></a>

### tablets_initial_scale_factor

> Calculated initial tablets are multiplied by this number

> * **Type:** `int`
> * **Default value:** `1`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-target-tablet-size-in-bytes"></a>

### target_tablet_size_in_bytes

> Allows target tablet size to be configured. Defaults to 5G (in bytes). Maintaining tablets at reasonable sizes is important to be able to    redistribute load. A higher value means tablet migration throughput can be reduced. A lower value may cause number of tablets to increase significantly,    potentially resulting in performance drawbacks.

> * **Type:** `uint64_t`
> * **Default value:** `service::default_target_tablet_size`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-replication-strategy-warn-list"></a>

### replication_strategy_warn_list

> Controls which replication strategies to warn about when creating/altering a keyspace. Doesn’t affect the pre-existing keyspaces.

> * **Type:** `std::vector<enum_option<replication_strategy_restriction_t>>`
> * **Default value:** `{locator::replication_strategy_type::simple}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-replication-strategy-fail-list"></a>

### replication_strategy_fail_list

> Controls which replication strategies are disallowed to be used when creating/altering a keyspace. Doesn’t affect the pre-existing keyspaces.

> * **Type:** `std::vector<enum_option<replication_strategy_restriction_t>>`
> * **Default value:** `{}`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-service-levels-interval-ms"></a>

### service_levels_interval_ms

> Controls how often service levels module polls configuration table

> * **Default value:** `10000`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `True`

<a id="confprop-topology-barrier-stall-detector-threshold-seconds"></a>

### topology_barrier_stall_detector_threshold_seconds

> Report sites blocking topology barrier if it takes longer than this.

> * **Type:** `double`
> * **Default value:** `2`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`

<a id="confprop-enable-tablets"></a>

### enable_tablets

> Enable tablets for newly created keyspaces

> * **Type:** `bool`
> * **Default value:** `false`
> * [Liveness](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Liveness): `False`
