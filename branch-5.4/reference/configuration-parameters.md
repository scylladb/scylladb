# Configuration Parameters

This section contains a list of properties that can be configured in `scylla.yaml` - the main configuration file for ScyllaDB.
In addition, properties that support live updates (liveness) can be updated via the `system.config` virtual table or the REST API.

<!-- -*- mode: rst -*- -->

## Ungrouped properties

### memtable_flush_static_shares

> <p>If set to higher than 0, ignore the controller's output and set the memtable shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity</p>
> * **Type:** `float`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_static_shares

> <p>If set to higher than 0, ignore the controller's output and set the compaction shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity</p>
> * **Type:** `float`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_enforce_min_threshold

> <p>If set to true, enforce the min_threshold option for compactions strictly. If false (default), Scylla may decide to compact even if below min_threshold</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_flush_all_tables_before_major_seconds

> <p>Set the minimum interval in seconds between flushing all tables before each major compaction (default is 86400). <br>        This option is useful for maximizing tombstone garbage collection by releasing all active commitlog segments. <br>        Set to 0 to disable automatic flushing all tables before major compaction</p>
> * **Type:** `uint32_t`
> * **Default value:** `86400`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Initialization properties

<p>The minimal properties needed for configuring a cluster.</p>

### cluster_name

> <p>The name of the cluster; used to prevent machines in one logical cluster from joining another. All nodes participating in a cluster must have the same value.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### listen_address

> <p>The IP address or hostname that Scylla binds to for connecting to other Scylla nodes. You must change the default setting for multiple nodes to communicate. Do not set to 0.0.0.0, unless you have set broadcast_address to an address that other nodes can use to reach this node.</p>
> * **Type:** `sstring`
> * **Default value:** `"localhost"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### listen_interface_prefer_ipv6

> <p>If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address<br>        you can specify which should be chosen using listen_interface_prefer_ipv6. If false the first ipv4<br>        address will be used. If true the first ipv6 address will be used. Defaults to false preferring<br>        ipv4. If there is only one address it will be selected regardless of ipv4/ipv6.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Default directories

<p>If you have changed any of the default directories during installation, make sure you have root access and set these properties.</p>

### workdir,W

> <p>The directory in which Scylla will put all its subdirectories. The location of individual subdirs can be overriden by the respective \*_directory options.</p>
> * **Default value:** `"/var/lib/scylla"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_directory

> <p>The directory where the commit log is stored. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### schema_commitlog_directory

> <p>The directory where the schema commit log is stored. This is a special commitlog instance used for schema and system tables. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### data_file_directories

> <p>The directory location where table data (SSTables) is stored</p>
> * **Type:** `string_list`
> * **Default value:** `{ }`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### hints_directory

> <p>The directory where hints files are stored if hinted handoff is enabled.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### view_hints_directory

> <p>The directory where materialized-view updates are stored while a view replica is unreachable.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Common initialization properties

<p>Be sure to set the properties in the Quick start section as well.</p>

### endpoint_snitch

> <p>Set to a class that implements the IEndpointSnitch. Scylla uses snitches for locating nodes and routing requests.<br>        - SimpleSnitch: Use for single-data center deployments or single-zone in public clouds. Does not recognize data center or rack information. It treats strategy order as proximity, which can improve cache locality when disabling read repair.<br>        - GossipingPropertyFileSnitch: Recommended for production. The rack and data center for the local node are defined in the cassandra-rackdc.properties file and propagated to other nodes via gossip. To allow migration from the PropertyFileSnitch, it uses the cassandra-topology.properties file if it is present.<br>        <br>        - Ec2Snitch: For EC2 deployments in a single region. Loads region and availability zone information from the EC2 API. The region is treated as the data center and the availability zone as the rack. Uses only private IPs. Subsequently it does not work across multiple regions.<br>        - Ec2MultiRegionSnitch: Uses public IPs as the broadcast_address to allow cross-region connectivity. This means you must also set seed addresses to the public IP and open the storage_port or ssl_storage_port on the public IP firewall. For intra-region traffic, Scylla switches to the private IP after establishing a connection.<br>        - GoogleCloudSnitch: For deployments on Google Cloud Platform across one or more regions. The region is treated as a datacenter and the availability zone is treated as a rack within the datacenter. The communication should occur over private IPs within the same logical network.<br>        - RackInferringSnitch: Proximity is determined by rack and data center, which are assumed to correspond to the 3rd and 2nd octet of each node's IP address, respectively. This snitch is best used as an example for writing a custom snitch class (unless this happens to match your deployment conventions).<br>        <br>        Related information: Snitches</p>
> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.locator.SimpleSnitch"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### rpc_address

> <p>The listen address for client connections (Thrift RPC service and native transport).Valid values are:<br>        <br>        - unset:   Resolves the address using the hostname configuration of the node. If left unset, the hostname must resolve to the IP address of this node using /etc/hostname, /etc/hosts, or DNS.<br>        - 0.0.0.0 : Listens on all configured interfaces, but you must set the broadcast_rpc_address to a value other than 0.0.0.0.<br>        - IP address<br>        - hostname<br>        Related information: Network</p>
> * **Type:** `sstring`
> * **Default value:** `"localhost"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### rpc_interface_prefer_ipv6

> <p>If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address<br>        you can specify which should be chosen using rpc_interface_prefer_ipv6. If false the first ipv4<br>        address will be used. If true the first ipv6 address will be used. Defaults to false preferring<br>        ipv4. If there is only one address it will be selected regardless of ipv4/ipv6</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### seed_provider

> <p>The addresses of hosts deemed contact points. Scylla nodes use the -seeds list to find each other and learn the topology of the ring.<br>        <br>          class_name (Default: org.apache.cassandra.locator.SimpleSeedProvider)<br>          - The class within Scylla that handles the seed logic. It can be customized, but this is typically not required.<br>          - - seeds (Default: 127.0.0.1)    A comma-delimited list of IP addresses used by gossip for bootstrapping new nodes joining a cluster. When running multiple nodes, you must change the list from the default value. In multiple data-center clusters, the seed list should include at least one node from each data center (replication group). More than a single seed node per data center is recommended for fault tolerance. Otherwise, gossip has to communicate with another data center when bootstrapping a node. Making every node a seed node is not recommended because of increased maintenance and reduced gossip performance. Gossip optimization is not critical, but it is recommended to use a small seed list (approximately three nodes per data center).<br>        <br>        Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers).</p>
> * **Type:** `seed_provider_type`
> * **Default value:** `seed_provider_type("org.apache.cassandra.locator.SimpleSeedProvider")`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Common compaction settings

<p>Be sure to set the properties in the Quick start section as well.</p>

### compaction_throughput_mb_per_sec

> <p>Throttles compaction to the specified total throughput across the entire system. The faster you insert data, the faster you need to compact in order to keep the SSTable count down. The recommended Value is 16 to 32 times the rate of write throughput (in MBs/second). Setting the value to 0 disables compaction throttling.<br>        Related information: Configuring compaction</p>
> * **Type:** `uint32_t`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_large_partition_warning_threshold_mb

> <p>Log a warning when writing partitions larger than this value</p>
> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_large_row_warning_threshold_mb

> <p>Log a warning when writing rows larger than this value</p>
> * **Type:** `uint32_t`
> * **Default value:** `10`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_large_cell_warning_threshold_mb

> <p>Log a warning when writing cells larger than this value</p>
> * **Type:** `uint32_t`
> * **Default value:** `1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_rows_count_warning_threshold

> <p>Log a warning when writing a number of rows larger than this value</p>
> * **Type:** `uint32_t`
> * **Default value:** `100000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### compaction_collection_elements_count_warning_threshold

> <p>Log a warning when writing a collection containing more elements than this value</p>
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

## Common automatic backup settings

### incremental_backups

> <p>Backs up data updated since the last snapshot was taken. When enabled, Scylla creates a hard link to each SSTable flushed or streamed locally in a backups/ subdirectory of the keyspace data. Removing these links is the operator's responsibility.<br>        Related information: Enabling incremental backups</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Common fault detection setting

### phi_convict_threshold

> <p>Adjusts the sensitivity of the failure detector on an exponential scale. Generally this setting never needs adjusting.<br>        Related information: Failure detection and recovery</p>
> * **Type:** `uint32_t`
> * **Default value:** `8`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### failure_detector_timeout_in_ms

> <p>Maximum time between two successful echo message before gossip mark a node down in milliseconds.</p>
> * **Type:** `uint32_t`
> * **Default value:** `20 * 1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### direct_failure_detector_ping_timeout_in_ms

> <p>Duration after which the direct failure detector aborts a ping message, so the next ping can start.<br>        Note: this failure detector is used by Raft, and is different from gossiper's failure detector (configured by \`failure_detector_timeout_in_ms\`).</p>
> * **Type:** `uint32_t`
> * **Default value:** `600`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Commit log settings

### commitlog_sync

> <p>The method that Scylla uses to acknowledge writes in milliseconds:<br>        <br>        - periodic : Used with commitlog_sync_period_in_ms (Default: 10000 - 10 seconds ) to control how often the commit log is synchronized to disk. Periodic syncs are acknowledged immediately.<br>        - batch : Used with commitlog_sync_batch_window_in_ms (Default: disabled \*\*) to control how long Scylla waits for other writes before performing a sync. When using this method, writes are not acknowledged until fsynced to disk.<br>        Related information: Durability</p>
> * **Type:** `sstring`
> * **Default value:** `"periodic"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_segment_size_in_mb

> <p>Sets the size of the individual commitlog file segments. A commitlog segment may be archived, deleted, or recycled after all its data has been flushed to SSTables. This amount of data can potentially include commitlog segments from every table in the system. The default size is usually suitable for most commitlog archiving, but if you want a finer granularity, 8 or 16 MB is reasonable. See Commit log archive configuration.<br>        Related information: Commit log archive configuration</p>
> * **Type:** `uint32_t`
> * **Default value:** `64`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### schema_commitlog_segment_size_in_mb

> <p>Sets the size of the individual schema commitlog file segments. The default size is larger than the default size of the data commitlog because the segment size puts a limit on the mutation size that can be written at once, and some schema mutation writes are much larger than average.<br>        Related information: Commit log archive configuration</p>
> * **Type:** `uint32_t`
> * **Default value:** `128`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_sync_period_in_ms

> <p>Controls how long the system waits for other writes before performing a sync in \\periodic\\ mode.</p>
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_sync_batch_window_in_ms

> <p>Controls how long the system waits for other writes before performing a sync in \\batch\\ mode.</p>
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_total_space_in_mb

> <p>Total space used for commitlogs. If the used space goes above this value, Scylla rounds up to the next nearest segment multiple and flushes memtables to disk for the oldest commitlog segments, removing those log segments. This reduces the amount of data to replay on startup, and prevents infrequently-updated tables from indefinitely keeping commitlog segments. A small total commitlog space tends to cause more flush activity on less-active tables.<br>        Related information: Configuring memtable throughput</p>
> * **Type:** `int64_t`
> * **Default value:** `-1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_flush_threshold_in_mb

> <p>Threshold for commitlog disk usage. When used disk space goes above this value, Scylla initiates flushes of memtables to disk for the oldest commitlog segments, removing those log segments. Adjusting this affects disk usage vs. write latency. Default is (approximately) commitlog_total_space_in_mb - &lt;num shards&gt;\*commitlog_segment_size_in_mb.</p>
> * **Type:** `int64_t`
> * **Default value:** `-1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_use_o_dsync

> <p>Whether or not to use O_DSYNC mode for commitlog segments IO. Can improve commitlog latency on some file systems.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### commitlog_use_hard_size_limit

> <p>Whether or not to use a hard size limit for commitlog disk usage. Default is false. Enabling this can cause latency spikes, whereas the default can lead to occasional disk usage peaks.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Compaction settings

<p>Related information: Configuring compaction</p>

### defragment_memory_on_idle

> <p>When set to true, will defragment memory when the cpu is idle.  This reduces the amount of work Scylla performs when processing client requests.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Cache and index settings

### column_index_size_in_kb

> <p>Granularity of the index of rows within a partition. For huge rows, decrease this setting to improve seek time. If you use key cache, be careful not to make this setting too large because key cache will be overwhelmed. If you're unsure of the size of the rows, it's best to use the default setting.</p>
> * **Type:** `uint32_t`
> * **Default value:** `64`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### column_index_auto_scale_threshold_in_kb

> <p>Auto-reduce the promoted index granularity by half when reaching this threshold, to prevent promoted index bloating due to partitions with too many rows. Set to 0 to disable this feature.</p>
> * **Type:** `uint32_t`
> * **Default value:** `10240`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

## Disks settings

### stream_io_throughput_mb_per_sec

> <p>Throttles streaming I/O to the specified total throughput (in MiBs/s) across the entire system. Streaming I/O includes the one performed by repair and both RBNO and legacy topology operations such as adding or removing a node. Setting the value to 0 disables stream throttling</p>
> * **Type:** `uint32_t`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### stream_plan_ranges_fraction

> <p>Specify the fraction of ranges to stream in a single stream plan. Value is between 0 and 1.</p>
> * **Type:** `double`
> * **Default value:** `0.1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

## Advanced initialization properties

<p>Properties for advanced users or properties that are less commonly used.</p>

### auto_bootstrap

> <p>This setting has been removed from default configuration. It makes new (non-seed) nodes automatically migrate the right data to themselves. Do not set this to false unless you really know what you are doing.<br>        Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers).</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### batch_size_warn_threshold_in_kb

> <p>Log WARN on any batch size exceeding this value in kilobytes. Caution should be taken on increasing the size of this threshold as it can lead to node instability.</p>
> * **Type:** `uint32_t`
> * **Default value:** `128`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### batch_size_fail_threshold_in_kb

> <p>Fail any multiple-partition batch exceeding this value. 1 MiB (8x warn threshold) by default.</p>
> * **Type:** `uint32_t`
> * **Default value:** `1024`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### broadcast_address

> <p>The IP address a node tells other nodes in the cluster to contact it by. It allows public and private address to be different. For example, use the broadcast_address parameter in topologies where not all nodes have access to other nodes by their private IP addresses.<br>        If your Scylla cluster is deployed across multiple Amazon EC2 regions and you use the EC2MultiRegionSnitch , set the broadcast_address to public IP address of the node and the listen_address to the private IP.</p>
> * **Type:** `sstring`
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### listen_on_broadcast_address

> <p>When using multiple physical network interfaces, set this to true to listen on broadcast_address in addition to the listen_address, allowing nodes to communicate in both interfaces.  Ignore this property if the network configuration automatically routes between the public and private networks such as EC2.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### initial_token

> <p>Used in the single-node-per-token architecture, where a node owns exactly one contiguous range in the ring space. Setting this property overrides num_tokens.<br>        If you not using vnodes or have num_tokens set it to 1 or unspecified (#num_tokens), you should always specify this parameter when setting up a production cluster for the first time and when adding capacity. For more information, see this parameter in the Cassandra 1.1 Node and Cluster Configuration documentation.<br>        This parameter can be used with num_tokens (vnodes ) in special cases such as Restoring from a snapshot.</p>
> * **Type:** `sstring`
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### num_tokens

> <p>Defines the number of tokens randomly assigned to this node on the ring when using virtual nodes (vnodes). The more tokens, relative to other nodes, the larger the proportion of data that the node stores. Generally all nodes should have the same number of tokens assuming equal hardware capability. The recommended value is 256. If unspecified (#num_tokens), Scylla uses 1 (equivalent to #num_tokens : 1) for legacy compatibility and uses the initial_token setting.<br>        If not using vnodes, comment #num_tokens : 256 or set num_tokens : 1 and use initial_token. If you already have an existing cluster with one token per node and wish to migrate to vnodes, see Enabling virtual nodes on an existing production cluster.<br>        Note: If using DataStax Enterprise, the default setting of this property depends on the type of node and type of install.</p>
> * **Type:** `uint32_t`
> * **Default value:** `1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### partitioner

> <p>Distributes rows (by partition key) across all nodes in the cluster. At the moment, only Murmur3Partitioner is supported. For new clusters use the default partitioner.<br>        <br>        Related information: Partitioners<br>        , {org.apache.cassandra.dht.Murmur3Partitioner})<br>    , storage_port(this, storage_port</p>
> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.dht.Murmur3Partitioner"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Advanced automatic backup setting

### auto_snapshot

> <p>Enable or disable whether a snapshot is taken of the data before keyspace truncation or dropping of tables. To prevent data loss, using the default setting is strongly advised. If you set to false, you will lose data on truncation or drop.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Tombstone settings

<p>When executing a scan, within or across a partition, tombstones must be kept in memory to allow returning them to the coordinator. The coordinator uses them to ensure other replicas know about the deleted rows. Workloads that generate numerous tombstones may cause performance problems and exhaust the server heap. See Cassandra anti-patterns: Queues and queue-like datasets. Adjust these thresholds only if you understand the impact and want to scan more tombstones. Additionally, you can adjust these thresholds at runtime using the StorageServiceMBean.<br>      Related information: Cassandra anti-patterns: Queues and queue-like datasets.</p>

### tombstone_warn_threshold

> <p>The maximum number of tombstones a query can scan before warning.</p>
> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### query_tombstone_page_limit

> <p>The number of tombstones after which a query cuts a page, even if not full or even empty.</p>
> * **Type:** `uint64_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### query_page_size_in_bytes

> <p>The size of pages in bytes, after a page accumulates this much data, the page is cut and sent to the client.<br>         Setting a too large value increases the risk of OOM.</p>
> * **Type:** `uint64_t`
> * **Default value:** `1 << 20`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

## Network timeout settings

### range_request_timeout_in_ms

> <p>The time in milliseconds that the coordinator waits for sequential or index scans to complete.</p>
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### read_request_timeout_in_ms

> <p>The time that the coordinator waits for read operations to complete</p>
> * **Type:** `uint32_t`
> * **Default value:** `5000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### counter_write_request_timeout_in_ms

> <p>The time that the coordinator waits for counter writes to complete.</p>
> * **Type:** `uint32_t`
> * **Default value:** `5000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### cas_contention_timeout_in_ms

> <p>The time that the coordinator continues to retry a CAS (compare and set) operation that contends with other proposals for the same row.</p>
> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### truncate_request_timeout_in_ms

> <p>The time that the coordinator waits for truncates (remove all data from a table) to complete. The long default value allows for a snapshot to be taken before removing the data. If auto_snapshot is disabled (not recommended), you can reduce this time.</p>
> * **Type:** `uint32_t`
> * **Default value:** `60000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### write_request_timeout_in_ms

> <p>The time in milliseconds that the coordinator waits for write operations to complete.<br>        Related information: About hinted handoff writes</p>
> * **Type:** `uint32_t`
> * **Default value:** `2000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### request_timeout_in_ms

> <p>The default timeout for other, miscellaneous operations.<br>        Related information: About hinted handoff writes</p>
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

## Inter-node settings

### internode_compression

> <p>Controls whether traffic between nodes is compressed. The valid values are:<br>        <br>        - all: All traffic is compressed.<br>        - dc : Traffic between data centers is compressed.<br>        - none : No compression.</p>
> * **Type:** `sstring`
> * **Default value:** `"none"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### inter_dc_tcp_nodelay

> <p>Enable or disable tcp_nodelay for inter-data center communication. When disabled larger, but fewer, network packets are sent. This reduces overhead from the TCP protocol itself. However, if cross data-center responses are blocked, it will increase latency.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Native transport (CQL Binary Protocol)

### start_native_transport

> <p>Enable or disable the native transport server. Uses the same address as the rpc_address, but the port is different from the rpc_port. See native_transport_port.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### native_transport_port

> <p>Port on which the CQL native transport listens for clients.</p>
> * **Type:** `uint16_t`
> * **Default value:** `9042`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### native_transport_port_ssl

> <p>Port on which the CQL TLS native transport listens for clients.<br>        Enabling client encryption and keeping native_transport_port_ssl disabled will use encryption<br>        for native_transport_port. Setting native_transport_port_ssl to a different value<br>        from native_transport_port will use encryption for native_transport_port_ssl while<br>        keeping native_transport_port unencrypted</p>
> * **Type:** `uint16_t`
> * **Default value:** `9142`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### native_shard_aware_transport_port

> <p>Like native_transport_port, but clients-side port number (modulo smp) is used to route the connection to the specific shard.</p>
> * **Type:** `uint16_t`
> * **Default value:** `19042`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### native_shard_aware_transport_port_ssl

> <p>Like native_transport_port_ssl, but clients-side port number (modulo smp) is used to route the connection to the specific shard.</p>
> * **Type:** `uint16_t`
> * **Default value:** `19142`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## RPC (remote procedure call) settings

<p>Settings for configuring and tuning client connections.</p>

### broadcast_rpc_address

> <p>RPC address to broadcast to drivers and other Scylla nodes. This cannot be set to 0.0.0.0. If blank, it is set to the value of the rpc_address or rpc_interface. If rpc_address or rpc_interfaceis set to 0.0.0.0, this property must be set.</p>
> * **Type:** `sstring`
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### rpc_port

> <p>Thrift port for client connections.</p>
> * **Type:** `uint16_t`
> * **Default value:** `9160`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### start_rpc

> <p>Starts the Thrift RPC server</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### rpc_keepalive

> <p>Enable or disable keepalive on client connections (RPC or native).</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### cache_hit_rate_read_balancing

> <p>This boolean controls whether the replicas for read query will be choosen based on cache hit ratio</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Advanced fault detection settings

<p>Settings to handle poorly performing or failing nodes.</p>

### hinted_handoff_enabled

> <p>Enable or disable hinted handoff. To enable per data center, add data center list. For example: hinted_handoff_enabled: DC1,DC2. A hint indicates that the write needs to be replayed to an unavailable node. <br>        Related information: About hinted handoff writes</p>
> * **Type:** `hinted_handoff_enabled_type`
> * **Default value:** `db::config::hinted_handoff_enabled_type(db::config::hinted_handoff_enabled_type::enabled_for_all_tag())`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### max_hinted_handoff_concurrency

> <p>Maximum concurrency allowed for sending hints. The concurrency is divided across shards and rounded up if not divisible by the number of shards. By default (or when set to 0), concurrency of 8\*shard_count will be used.</p>
> * **Type:** `uint32_t`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### max_hint_window_in_ms

> <p>Maximum amount of time that hints are generates hints for an unresponsive node. After this interval, new hints are no longer generated until the node is back up and responsive. If the node goes down again, a new interval begins. This setting can prevent a sudden demand for resources when a node is brought back online and the rest of the cluster attempts to replay a large volume of hinted writes.<br>        Related information: Failure detection and recovery</p>
> * **Type:** `uint32_t`
> * **Default value:** `10800000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Thrift interface properties

<p>Legacy API for older clients. CQL is a simpler and better API for Scylla.</p>

### thrift_max_message_length_in_mb

> <p>The maximum length of a Thrift message in megabytes, including all fields and internal Thrift overhead (1 byte of overhead for each frame). Message length is usually used in conjunction with batches. A frame length greater than or equal to 24 accommodates a batch with four inserts, each of which is 24 bytes. The required message length is greater than or equal to 24+24+24+24+4 (number of frames).</p>
> * **Type:** `uint32_t`
> * **Default value:** `16`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

## Security properties

<p>Server and client security settings.</p>

### authenticator

> <p>The authentication backend, used to identify users. The available authenticators are:<br>        <br>        - org.apache.cassandra.auth.AllowAllAuthenticator : Disables authentication; no checks are performed.<br>        - org.apache.cassandra.auth.PasswordAuthenticator : Authenticates users with user names and hashed passwords stored in the system_auth.credentials table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.<br>        - com.scylladb.auth.CertificateAuthenticator : Authenticates users based on TLS certificate authentication subject. Roles and permissions still need to be defined as normal. Super user can be set using the 'auth_superuser_name' configuration value. Query to extract role name from subject string is set using 'auth_certificate_role_queries'.<br>        - com.scylladb.auth.TransitionalAuthenticator : Wraps around the PasswordAuthenticator, logging them in if username/password pair provided is correct and treating them as anonymous users otherwise.<br>        Related information: Internal authentication<br>        , {AllowAllAuthenticator</p>
> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.auth.AllowAllAuthenticator"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### authorizer

> <p>The authorization backend. It implements IAuthenticator, which limits access and provides permissions. The available authorizers are:<br>        <br>        - AllowAllAuthorizer : Disables authorization; allows any action to any user.<br>        - CassandraAuthorizer : Stores permissions in system_auth.permissions table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.<br>        - com.scylladb.auth.TransitionalAuthorizer : Wraps around the CassandraAuthorizer, which is used to authorize permission management. Other actions are allowed for all users.<br>        Related information: Object permissions<br>        , {AllowAllAuthorizer</p>
> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.auth.AllowAllAuthorizer"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### role_manager

> <p>The role-management backend, used to maintain grantts and memberships between roles.<br>        The available role-managers are:<br>        - CassandraRoleManager : Stores role data in the system_auth keyspace.</p>
> * **Type:** `sstring`
> * **Default value:** `"org.apache.cassandra.auth.CassandraRoleManager"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### permissions_validity_in_ms

> <p>How long permissions in cache remain valid. Depending on the authorizer, such as CassandraAuthorizer, fetching permissions can be resource intensive. Permissions caching is disabled when this property is set to 0 or when AllowAllAuthorizer is used. The cached value is considered valid as long as both its value is not older than the permissions_validity_in_ms <br>        and the cached value has been read at least once during the permissions_validity_in_ms time frame. If any of these two conditions doesn't hold the cached value is going to be evicted from the cache.<br>        Related information: Object permissions</p>
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### permissions_update_interval_in_ms

> <p>Refresh interval for permissions cache (if enabled). After this interval, cache entries become eligible for refresh. An async reload is scheduled every permissions_update_interval_in_ms time period and the old value is returned until it completes. If permissions_validity_in_ms has a non-zero value, then this property must also have a non-zero value. It's recommended to set this value to be at least 3 times smaller than the permissions_validity_in_ms.</p>
> * **Type:** `uint32_t`
> * **Default value:** `2000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### permissions_cache_max_entries

> <p>Maximum cached permission entries. Must have a non-zero value if permissions caching is enabled (see a permissions_validity_in_ms description).</p>
> * **Type:** `uint32_t`
> * **Default value:** `1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### server_encryption_options

> <p>Enable or disable inter-node encryption. You must also generate keys and provide the appropriate key and trust store locations and passwords. The available options are:<br>        <br>        internode_encryption : (Default: none) Enable or disable encryption of inter-node communication using the TLS_RSA_WITH_AES_128_CBC_SHA cipher suite for authentication, key exchange, and encryption of data transfers. The available inter-node options are:<br>        - all : Encrypt all inter-node communications.<br>        - none : No encryption.<br>        - dc : Encrypt the traffic between the data centers (server only).<br>        - rack : Encrypt the traffic between the racks(server only).<br>        certificate : (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the internode communication.<br>        keyfile : (Default: conf/scylla.key) PEM Key file associated with certificate.<br>        truststore : (Default: &lt;not set, use system truststore&gt; ) Location of the truststore containing the trusted certificate for authenticating remote servers.<br>        certficate_revocation_list : (Default: &lt;not set&gt;) PEM encoded certificate revocation list.<br>        <br>        The advanced settings are:<br>        <br>        - priority_string : (Default: not set, use default) GnuTLS priority string controlling TLS algorithms used/allowed.<br>        - require_client_auth : (Default: false ) Enables or disables certificate authentication.<br>        Related information: Node-to-node encryption</p>
> * **Type:** `string_map`
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### client_encryption_options

> <p>Enable or disable client-to-node encryption. You must also generate keys and provide the appropriate key and certificate. The available options are:<br>        <br>        - enabled : (Default: false) To enable, set to true.<br>        - certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.<br>        - keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.<br>        truststore : (Default: &lt;not set. use system truststore&gt;) Location of the truststore containing the trusted certificate for authenticating remote servers.<br>        certficate_revocation_list : (Default: &lt;not set&gt; ) PEM encoded certificate revocation list.<br>        <br>        The advanced settings are:<br>        <br>        - priority_string : (Default: not set, use default) GnuTLS priority string controlling TLS algorithms used/allowed.<br>        - require_client_auth : (Default: false) Enables or disables certificate authentication.<br>        Related information: Client-to-node encryption</p>
> * **Type:** `string_map`
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_encryption_options

> <p>When Alternator via HTTPS is enabled with alternator_https_port, where to take the key and certificate. The available options are:<br>        <br>        - certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.<br>        - keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.<br>        <br>        The advanced settings are:<br>        <br>        - priority_string : GnuTLS priority string controlling TLS algorithms used/allowed.</p>
> * **Type:** `string_map`
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### ssl_storage_port

> <p>The SSL port for encrypted communication. Unused unless enabled in encryption_options.</p>
> * **Type:** `uint32_t`
> * **Default value:** `7001`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_in_memory_data_store

> <p>Enable in memory mode (system tables are always persisted)</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_cache

> <p>Enable cache</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_commitlog

> <p>Enable commitlog</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### volatile_system_keyspace_for_testing

> <p>Don't persist system keyspace - testing only!</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### api_port

> <p>Http Rest API port</p>
> * **Type:** `uint16_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### api_address

> <p>Http Rest API address</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### api_ui_dir

> <p>The directory location of the API GUI</p>
> * **Type:** `sstring`
> * **Default value:** `"swagger-ui/dist/"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### api_doc_dir

> <p>The API definition file directory</p>
> * **Type:** `sstring`
> * **Default value:** `"api/api-doc/"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### consistent_rangemovement

> <p>When set to true, range movements will be consistent. It means: 1) it will refuse to bootstrap a new node if other bootstrapping/leaving/moving nodes detected. 2) data will be streamed to a new node only from the node which is no longer responsible for the token range. Same as -Dcassandra.consistent.rangemovement in cassandra</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### load_ring_state

> <p>When set to true, load tokens and host_ids previously saved. Same as -Dcassandra.load_ring_state in cassandra.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### replace_node_first_boot

> <p>The Host ID of a dead node to replace. If the replacing node has already been bootstrapped successfully, this option will be ignored.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### replace_address

> <p>[[deprecated]] The listen_address or broadcast_address of the dead node to replace. Same as -Dcassandra.replace_address.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### replace_address_first_boot

> <p>[[deprecated]] Like replace_address option, but if the node has been bootstrapped successfully it will be ignored. Same as -Dcassandra.replace_address_first_boot.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### ignore_dead_nodes_for_replace

> <p>List dead nodes to ignore for replace operation using a comma-separated list of host IDs. E.g., scylla --ignore-dead-nodes-for-replace 8d5ed9f4-7764-4dbd-bad8-43fddce94b7c,125ed9f4-7777-1dbn-mac8-43fddce9123e</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### override_decommission

> <p>Set true to force a decommissioned node to join the cluster (cannot be set if consistent-cluster-management is enabled</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_repair_based_node_ops

> <p>Set true to use enable repair based node operations instead of streaming based</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### allowed_repair_based_node_ops

> <p>A comma separated list of node operations which are allowed to enable repair based node operations. The operations can be bootstrap, replace, removenode, decommission and rebuild</p>
> * **Type:** `sstring`
> * **Default value:** `"replace,removenode,rebuild,bootstrap,decommission"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### enable_compacting_data_for_streaming_and_repair

> <p>Enable the compacting reader, which compacts the data for streaming and repair (load'n'stream included) before sending it to, or synchronizing it with peers. Can reduce the amount of data to be processed by removing dead data, but adds CPU overhead.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### repair_partition_count_estimation_ratio

> <p>Specify the fraction of partitions written by repair out of the total partitions. The value is currently only used for bloom filter estimation. Value is between 0 and 1.</p>
> * **Type:** `double`
> * **Default value:** `0.1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### ring_delay_ms

> <p>Time a node waits to hear from other nodes before joining the ring in milliseconds. Same as -Dcassandra.ring_delay_ms in cassandra.</p>
> * **Type:** `uint32_t`
> * **Default value:** `30 * 1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### shadow_round_ms

> <p>The maximum gossip shadow round time. Can be used to reduce the gossip feature check time during node boot up.</p>
> * **Type:** `uint32_t`
> * **Default value:** `300 * 1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### fd_max_interval_ms

> <p>The maximum failure_detector interval time in milliseconds. Interval larger than the maximum will be ignored. Larger cluster may need to increase the default.</p>
> * **Type:** `uint32_t`
> * **Default value:** `2 * 1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### fd_initial_value_ms

> <p>The initial failure_detector interval time in milliseconds.</p>
> * **Type:** `uint32_t`
> * **Default value:** `2 * 1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### shutdown_announce_in_ms

> <p>Time a node waits after sending gossip shutdown message in milliseconds. Same as -Dcassandra.shutdown_announce_in_ms in cassandra.</p>
> * **Type:** `uint32_t`
> * **Default value:** `2 * 1000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### developer_mode

> <p>Relax environment checks. Setting to true can reduce performance and reliability significantly.</p>
> * **Type:** `bool`
> * **Default value:** `DEVELOPER_MODE_DEFAULT`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### skip_wait_for_gossip_to_settle

> <p>An integer to configure the wait for gossip to settle. -1: wait normally, 0: do not wait at all, n: wait for at most n polls. Same as -Dcassandra.skip_wait_for_gossip_to_settle in cassandra.</p>
> * **Type:** `int32_t`
> * **Default value:** `-1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### force_gossip_generation

> <p>Force gossip to use the generation number provided by user</p>
> * **Type:** `int32_t`
> * **Default value:** `-1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### lsa_reclamation_step

> <p>Minimum number of segments to reclaim in a single step</p>
> * **Type:** `size_t`
> * **Default value:** `1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### prometheus_port

> <p>Prometheus port, set to zero to disable</p>
> * **Type:** `uint16_t`
> * **Default value:** `9180`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### prometheus_address

> <p>Prometheus listening address, defaulting to listen_address if not explicitly set</p>
> * **Type:** `sstring`
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### prometheus_prefix

> <p>Set the prefix of the exported Prometheus metrics. Changing this will break Scylla's dashboard compatibility, do not change unless you know what you are doing.</p>
> * **Type:** `sstring`
> * **Default value:** `"scylla"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### abort_on_lsa_bad_alloc

> <p>Abort when allocation in LSA region fails</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### murmur3_partitioner_ignore_msb_bits

> <p>Number of most siginificant token bits to ignore in murmur3 partitioner; increase for very large clusters</p>
> * **Type:** `unsigned`
> * **Default value:** `default_murmur3_partitioner_ignore_msb_bits`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### unspooled_dirty_soft_limit

> <p>Soft limit of unspooled dirty memory expressed as a portion of the hard limit</p>
> * **Type:** `double`
> * **Default value:** `0.6`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### sstable_summary_ratio

> <p>Enforces that 1 byte of summary is written for every N (2000 by default) <br>        bytes written to data file. Value must be between 0 and 1.</p>
> * **Type:** `double`
> * **Default value:** `0.0005`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### components_memory_reclaim_threshold

> <p>Ratio of available memory for all in-memory components of SSTables in a shard beyond which the memory will be reclaimed from components until it falls back under the threshold. Currently, this limit is only enforced for bloom filters.</p>
> * **Type:** `double`
> * **Default value:** `.2`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### large_memory_allocation_warning_threshold

> <p>Warn about memory allocations above this size; set to zero to disable</p>
> * **Type:** `size_t`
> * **Default value:** `size_t(1) << 20`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_deprecated_partitioners

> <p>Enable the byteordered and random partitioners. These partitioners are deprecated and will be removed in a future version.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_keyspace_column_family_metrics

> <p>Enable per keyspace and per column family metrics reporting</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_node_aggregated_table_metrics

> <p>Enable aggregated per node, per keyspace and per table metrics reporting, applicable if enable_keyspace_column_family_metrics is false</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_sstable_data_integrity_check

> <p>Enable interposer which checks for integrity of every sstable write.<br>         Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_sstable_key_validation

> <p>Enable validation of partition and clustering keys monotonicity<br>         Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.</p>
> * **Type:** `bool`
> * **Default value:** `ENABLE_SSTABLE_KEY_VALIDATION`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### cpu_scheduler

> <p>Enable cpu scheduling</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### view_building

> <p>Enable view building; should only be set to false when the node is experience issues due to view building</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### sstable_format

> <p>Default sstable file format</p>
> * **Type:** `sstring`
> * **Default value:** `"me"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### uuid_sstable_identifiers_enabled

> <p>If set to true, each newly created sstable will have a UUID <br>            based generation identifier, and such files are not readable by previous Scylla versions.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### table_digest_insensitive_to_expiry

> <p>When enabled, per-table schema digest calculation ignores empty partitions.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_dangerous_direct_import_of_cassandra_counters

> <p>Only turn this option on if you want to import tables from Cassandra containing counters, and you are SURE that no counters in that table were created in a version earlier than Cassandra 2.1.<br>         It is not enough to have ever since upgraded to newer versions of Cassandra. If you EVER used a version earlier than 2.1 in the cluster where these SSTables come from, DO NOT TURN ON THIS OPTION! You will corrupt your data. You have been warned.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_shard_aware_drivers

> <p>Enable native transport drivers to use connection-per-shard for better performance</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_ipv6_dns_lookup

> <p>Use IPv6 address resolution</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### abort_on_internal_error

> <p>Abort the server instead of throwing exception when internal invariants are violated</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### max_partition_key_restrictions_per_query

> <p>Maximum number of distinct partition keys restrictions per query. This limit places a bound on the size of IN tuples, <br>            especially when multiple partition key columns have IN restrictions. Increasing this value can result in server instability.</p>
> * **Type:** `uint32_t`
> * **Default value:** `100`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### max_clustering_key_restrictions_per_query

> <p>Maximum number of distinct clustering key restrictions per query. This limit places a bound on the size of IN tuples, <br>            especially when multiple clustering key columns have IN restrictions. Increasing this value can result in server instability.</p>
> * **Type:** `uint32_t`
> * **Default value:** `100`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### max_memory_for_unlimited_query_soft_limit

> <p>Maximum amount of memory a query, whose memory consumption is not naturally limited, is allowed to consume, e.g. non-paged and reverse queries. <br>            This is the soft limit, there will be a warning logged for queries violating this limit.</p>
> * **Type:** `uint64_t`
> * **Default value:** `uint64_t(1) << 20`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### max_memory_for_unlimited_query_hard_limit

> <p>Maximum amount of memory a query, whose memory consumption is not naturally limited, is allowed to consume, e.g. non-paged and reverse queries. <br>            This is the hard limit, queries violating this limit will be aborted.</p>
> * **Type:** `uint64_t`
> * **Default value:** `(uint64_t(100) << 20)`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### reader_concurrency_semaphore_serialize_limit_multiplier

> <p>Start serializing reads after their collective memory consumption goes above $normal_limit \* $multiplier.</p>
> * **Type:** `uint32_t`
> * **Default value:** `2`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### reader_concurrency_semaphore_kill_limit_multiplier

> <p>Start killing reads after their collective memory consumption goes above $normal_limit \* $multiplier.</p>
> * **Type:** `uint32_t`
> * **Default value:** `4`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### reader_concurrency_semaphore_cpu_concurrency

> <p>Admit new reads while there are less than this number of requests that need CPU.</p>
> * **Type:** `uint32_t`
> * **Default value:** `1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### twcs_max_window_count

> <p>The maximum number of compaction windows allowed when making use of TimeWindowCompactionStrategy. A setting of 0 effectively disables the restriction.</p>
> * **Type:** `uint32_t`
> * **Default value:** `50`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### initial_sstable_loading_concurrency

> <p>Maximum amount of sstables to load in parallel during initialization. A higher number can lead to more memory consumption. You should not need to touch this</p>
> * **Type:** `unsigned`
> * **Default value:** `4u`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_3_1_0_compatibility_mode

> <p>Set to true if the cluster was initially installed from 3.1.0. If it was upgraded from an earlier version,<br>         or installed from a later version, leave this set to false. This adjusts the communication protocol to<br>         work around a bug in Scylla 3.1.0</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### enable_user_defined_functions

> <p>Enable user defined functions. You must also set experimental-features=udf</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### user_defined_function_time_limit_ms

> <p>The time limit for each UDF invocation</p>
> * **Type:** `unsigned`
> * **Default value:** `10`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### user_defined_function_allocation_limit_bytes

> <p>How much memory each UDF invocation can allocate</p>
> * **Type:** `unsigned`
> * **Default value:** `1024*1024`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### user_defined_function_contiguous_allocation_limit_bytes

> <p>How much memory each UDF invocation can allocate in one chunk</p>
> * **Type:** `unsigned`
> * **Default value:** `1024*1024`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### schema_registry_grace_period

> <p>Time period in seconds after which unused schema versions will be evicted from the local schema registry cache. Default is 1 second.</p>
> * **Type:** `uint32_t`
> * **Default value:** `1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### max_concurrent_requests_per_shard

> <p>Maximum number of concurrent requests a single shard can handle before it starts shedding extra load. By default, no requests will be shed.</p>
> * **Type:** `uint32_t`
> * **Default value:** `std::numeric_limits<uint32_t>::max()`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### cdc_dont_rewrite_streams

> <p>Disable rewriting streams from cdc_streams_descriptions to cdc_streams_descriptions_v2. Should not be necessary, but the procedure is expensive and prone to failures; this config option is left as a backdoor in case some user requires manual intervention.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### strict_allow_filtering

> <p>Match Cassandra in requiring ALLOW FILTERING on slow queries. Can be true, false, or warn. When false, Scylla accepts some slow queries even without ALLOW FILTERING that Cassandra rejects. Warn is same as false, but with warning.</p>
> * **Type:** `tri_mode_restriction`
> * **Default value:** `strict_allow_filtering_default()`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### strict_is_not_null_in_views

> <p>In materialized views, restrictions are allowed only on the view's primary key columns.<br>        In old versions Scylla mistakenly allowed IS NOT NULL restrictions on columns which were not part of the view's<br>         primary key. These invalid restrictions were ignored.<br>        This option controls the behavior when someone tries to create a view with such invalid IS NOT NULL restrictions.<br>        Can be true, false, or warn:<br>         \* \`true\`: IS NOT NULL is allowed only on the view's primary key columns, <br>        trying to use it on other columns will cause an error, as it should.<br>         \* \`false\`: Scylla accepts IS NOT NULL restrictions on regular columns, but they're silently ignored. <br>        It's useful for backwards compatibility.<br>         \* \`warn\`: The same as false, but there's a warning about invalid view restrictions.<br>        To preserve backwards compatibility on old clusters, Scylla's default setting is \`warn\`. <br>        New clusters have this option set to \`true\` by scylla.yaml (which overrides the default \`warn\`), <br>        to make sure that trying to create an invalid view causes an error.</p>
> * **Type:** `tri_mode_restriction`
> * **Default value:** `db::tri_mode_restriction_t::mode::WARN`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### reversed_reads_auto_bypass_cache

> <p>Bypass in-memory data cache (the row cache) when performing reversed queries.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### enable_optimized_reversed_reads

> <p>Use a new optimized algorithm for performing reversed reads.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### enable_cql_config_updates

> <p>Make the system.config table UPDATEable</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### enable_parallelized_aggregation

> <p>Use on a new, parallel algorithm for performing aggregate queries.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### alternator_port

> <p>Alternator API port</p>
> * **Type:** `uint16_t`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_https_port

> <p>Alternator API HTTPS port</p>
> * **Type:** `uint16_t`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_address

> <p>Alternator API listening address</p>
> * **Type:** `sstring`
> * **Default value:** `"0.0.0.0"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_enforce_authorization

> <p>Enforce checking the authorization header for every request in Alternator</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_write_isolation

> <p>Default write isolation policy for Alternator</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_streams_time_window_s

> <p>CDC query confidence window for alternator streams</p>
> * **Type:** `uint32_t`
> * **Default value:** `10`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_timeout_in_ms

> <p>The server-side timeout for completing Alternator API requests.</p>
> * **Type:** `uint32_t`
> * **Default value:** `10000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### alternator_ttl_period_in_seconds

> <p>The default period for Alternator's expiration scan. Alternator attempts to scan every table within that period.</p>
> * **Type:** `double`
> * **Default value:** `60*60*24`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### alternator_describe_endpoints

> <p>Overrides the behavior of Alternator's DescribeEndpoints operation. <br>        An empty value (the default) means DescribeEndpoints will return <br>        the same endpoint used in the request. The string 'disabled' <br>        disables the DescribeEndpoints operation. Any other string is the <br>        fixed value that will be returned by DescribeEndpoints operations.</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### abort_on_ebadf

> <p>Abort the server on incorrect file descriptor access. Throws exception when disabled.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### redis_port

> <p>Port on which the REDIS transport listens for clients.</p>
> * **Type:** `uint16_t`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### redis_ssl_port

> <p>Port on which the REDIS TLS native transport listens for clients.</p>
> * **Type:** `uint16_t`
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### redis_read_consistency_level

> <p>Consistency level for read operations for redis.</p>
> * **Type:** `sstring`
> * **Default value:** `"LOCAL_QUORUM"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### redis_write_consistency_level

> <p>Consistency level for write operations for redis.</p>
> * **Type:** `sstring`
> * **Default value:** `"LOCAL_QUORUM"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### redis_database_count

> <p>Database count for the redis. You can use the default settings (16).</p>
> * **Type:** `uint16_t`
> * **Default value:** `16`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### redis_keyspace_replication_strategy

> <p>Set the replication strategy for the redis keyspace. The setting is used by the first node in the boot phase when the keyspace is not exists to create keyspace for redis.<br>        The replication strategy determines how many copies of the data are kept in a given data center. This setting impacts consistency, availability and request speed.<br>        Two strategies are available: SimpleStrategy and NetworkTopologyStrategy.<br>        - class: (Default: SimpleStrategy ). Set the replication strategy for redis keyspace.<br>        - 'replication_factor':N, (Default: 'replication_factor':1) IFF the class is SimpleStrategy, assign the same replication factor to the entire cluster.<br>        - 'datacenter_name':N [,...], (Default: 'dc1:1') IFF the class is NetworkTopologyStrategy, assign replication factors to each data center in a comma separated list.<br>        <br>        Related information: About replication strategy.</p>
> * **Default value:** `{}`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### sanitizer_report_backtrace

> <p>In debug mode, report log-structured allocator sanitizer violations with a backtrace. Slow.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### flush_schema_tables_after_modification

> <p>Flush tables in the system_schema keyspace after schema modification. This is required for crash recovery, but slows down tests and can be disabled for them</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### restrict_replication_simplestrategy

> <p>Controls whether to disable SimpleStrategy replication. Can be true, false, or warn.</p>
> * **Type:** `tri_mode_restriction`
> * **Default value:** `db::tri_mode_restriction_t::mode::FALSE`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### restrict_twcs_without_default_ttl

> <p>Controls whether to prevent creating TimeWindowCompactionStrategy tables without a default TTL. Can be true, false, or warn.</p>
> * **Type:** `tri_mode_restriction`
> * **Default value:** `db::tri_mode_restriction_t::mode::WARN`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### restrict_future_timestamp

> <p>Controls whether to detect and forbid unreasonable USING TIMESTAMP, more than 3 days into the future.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### unsafe_ignore_truncation_record

> <p>Ignore truncation record stored in system tables as if tables were never truncated.</p>
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### force_schema_commit_log

> <p>Use separate schema commit log unconditionally rater than after restart following discovery of cluster-wide support for it.</p>
> * **Type:** `bool`
> * **Default value:** `false`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### task_ttl_in_seconds

> <p>Time for which information about finished task stays in memory.</p>
> * **Default value:** `0`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### nodeops_watchdog_timeout_seconds

> <p>Time in seconds after which node operations abort when not hearing from the coordinator</p>
> * **Type:** `uint32_t`
> * **Default value:** `120`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### nodeops_heartbeat_interval_seconds

> <p>Period of heartbeat ticks in node operations</p>
> * **Type:** `uint32_t`
> * **Default value:** `10`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### cache_index_pages

> <p>Keep SSTable index pages in the global cache after a SSTable read. Expected to improve performance for workloads with big partitions, but may degrade performance for workloads with small partitions. The amount of memory usable by index cache is limited with \`index_cache_fraction\`.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### index_cache_fraction

> <p>The maximum fraction of cache memory permitted for use by index cache. Clamped to the [0.0; 1.0] range. Must be small enough to not deprive the row cache of memory, but should be big enough to fit a large fraction of the index. The default value 0.2 means that at least 80\\% of cache memory is reserved for the row cache, while at most 20\\% is usable by the index cache.</p>
> * **Type:** `double`
> * **Default value:** `0.2`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### consistent_cluster_management

> <p>Use RAFT for cluster management and DDL</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### wasm_cache_memory_fraction

> <p>Maximum total size of all WASM instances stored in the cache as fraction of total shard memory</p>
> * **Type:** `double`
> * **Default value:** `0.01`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### wasm_cache_timeout_in_ms

> <p>Time after which an instance is evicted from the cache</p>
> * **Type:** `uint32_t`
> * **Default value:** `5000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### wasm_cache_instance_size_limit

> <p>Instances with size above this limit will not be stored in the cache</p>
> * **Type:** `size_t`
> * **Default value:** `1024*1024`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### wasm_udf_yield_fuel

> <p>Wasmtime fuel a WASM UDF can consume before yielding</p>
> * **Type:** `uint64_t`
> * **Default value:** `100000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### wasm_udf_total_fuel

> <p>Wasmtime fuel a WASM UDF can consume before termination</p>
> * **Type:** `uint64_t`
> * **Default value:** `100000000`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### wasm_udf_memory_limit

> <p>How much memory each WASM UDF can allocate at most</p>
> * **Type:** `size_t`
> * **Default value:** `2*1024*1024`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### relabel_config_file

> <p>Optionally, read relabel config from file</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### object_storage_config_file

> <p>Optionally, read object-storage endpoints config from file</p>
> * **Type:** `sstring`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### live_updatable_config_params_changeable_via_cql

> <p>If set to true, configuration parameters defined with LiveUpdate can be updated in runtime via CQL (by updating system.config virtual table), otherwise they can't.</p>
> * **Type:** `bool`
> * **Default value:** `true`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### auth_superuser_name

> <p>Initial authentication super username. Ignored if authentication tables already contain a super user</p>
> * **Type:** `std::string`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### auth_superuser_salted_password

> <p>Initial authentication super user salted password. Create using mkpassword or similar. The hashing algorithm used must be available on the node host. <br>        Ignored if authentication tables already contain a super user password.</p>
> * **Type:** `std::string`
> * **Default value:** `""`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### auth_certificate_role_queries

> <p>SUBJECT }, {query</p>
> * **Type:** `std::vector<std::unordered_map<sstring, sstring>>`
> * **Default value:** `{ { { "source"`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `False`

### minimum_replication_factor_fail_threshold

> <p></p>
> * **Type:** `int`
> * **Default value:** `-1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### minimum_replication_factor_warn_threshold

> <p></p>
> * **Type:** `int`
> * **Default value:** `3`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### maximum_replication_factor_warn_threshold

> <p></p>
> * **Type:** `int`
> * **Default value:** `-1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`

### maximum_replication_factor_fail_threshold

> <p></p>
> * **Type:** `int`
> * **Default value:** `-1`
> * **Liveness** [\*](https://opensource.docs.scylladb.com/branch-5.4/reference/glossary.md#term-Liveness) **:** `True`
