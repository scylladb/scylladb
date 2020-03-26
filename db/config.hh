/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <boost/program_options.hpp>
#include <unordered_map>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/program-options.hh>
#include <seastar/util/log.hh>

#include "seastarx.hh"
#include "utils/config_file.hh"

namespace seastar { class file; struct logging_settings; }

namespace db {

namespace fs = std::filesystem;

class extensions;

/*
 * This type is not use, and probably never will be.
 * So it makes sense to jump through hoops just to ensure
 * it is in fact handled properly...
 */
struct seed_provider_type {
    seed_provider_type() = default;
    seed_provider_type(sstring n,
            std::initializer_list<program_options::string_map::value_type> opts =
                    { })
            : class_name(std::move(n)), parameters(std::move(opts)) {
    }
    sstring class_name;
    std::unordered_map<sstring, sstring> parameters;
};

class config : public utils::config_file {
public:
    config();
    config(std::shared_ptr<db::extensions>);
    ~config();

    // Throws exception if experimental feature is disabled.
    void check_experimental(const sstring& what) const;

    /**
     * Scans the environment variables for configuration files directory
     * definition. It's either $SCYLLA_CONF, $SCYLLA_HOME/conf or "conf" if none
     * of SCYLLA_CONF and SCYLLA_HOME is defined.
     *
     * @return path of the directory where configuration files are located
     *         according the environment variables definitions.
     */
    static fs::path get_conf_dir();

    using string_map = std::unordered_map<sstring, sstring>;
                    //program_options::string_map;
    using string_list = std::vector<sstring>;
    using seed_provider_type = db::seed_provider_type;

    /*
     * All values and documentation taken from
     * http://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html
     *
     * Big fat x-macro expansion of _all_ (at this writing) cassandra opts, with the "documentation"
     * included.
     *
     * X-macro syntax:
     *
     * X(member, type, default, status, desc [, value, value...])
     *
     * Where
     *  member: is the property name -> config member name
     *  type:   is the value type (bool, uint32_t etc)
     *  status: is the current _usage_ of the opt. I.e. if you actually use the value, set it to "Used".
     *          A value marked "UsedFromSeastar" is a configuration value that is assigned based on a Seastar-defined
     *          command-line interface option.
     *          Most values are set to "Unused", as in "will probably have an effect eventually".
     *          Values set to "Invalid" have no meaning/usage in scylla, and should (and will currently)
     *          be signaled to a user providing a config with them, that these settings are pointless.
     *  desc:   documentation.
     *  value...: enumerated valid values if any. Not currently used, but why not...
     *
     *
     * Note:
     * Only values marked as "Used" will be added as command line options.
     * Options marked as "Invalid" will be warned about if read from config
     * Options marked as "Unused" will also warned about.
     *
     */

#define _make_config_values(val)                \
    val(background_writer_scheduling_quota, double, 1.0, Unused, \
            "max cpu usage ratio (between 0 and 1) for compaction process. Not intended for setting in normal operations. Setting it to 1 or higher will disable it, recommended operational setting is 0.5." \
    )   \
    val(auto_adjust_flush_quota, bool, false, Unused, \
            "true: auto-adjust memtable shares for flush processes" \
    )   \
    val(memtable_flush_static_shares, float, 0, Used, \
            "If set to higher than 0, ignore the controller's output and set the memtable shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity" \
    )   \
    val(compaction_static_shares, float, 0, Used, \
            "If set to higher than 0, ignore the controller's output and set the compaction shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity" \
    )   \
    val(compaction_enforce_min_threshold, bool, false, Used, \
            "If set to true, enforce the min_threshold option for compactions strictly. If false (default), Scylla may decide to compact even if below min_threshold" \
    )   \
    /* Initialization properties */             \
    /* The minimal properties needed for configuring a cluster. */  \
    val(cluster_name, sstring, "", Used,   \
            "The name of the cluster; used to prevent machines in one logical cluster from joining another. All nodes participating in a cluster must have the same value."   \
    )                                           \
    val(listen_address, sstring, "localhost", Used,     \
            "The IP address or hostname that Scylla binds to for connecting to other Scylla nodes. Set this parameter or listen_interface, not both. You must change the default setting for multiple nodes to communicate:\n"    \
            "\n"    \
            "Generally set to empty. If the node is properly configured (host name, name resolution, and so on), Scylla uses InetAddress.getLocalHost() to get the local address from the system.\n" \
            "For a single node cluster, you can use the default setting (localhost).\n" \
            "If Scylla can't find the correct address, you must specify the IP address or host name.\n"  \
            "Never specify 0.0.0.0; it is always wrong."  \
    )                                                   \
    val(listen_interface, sstring, "eth0", Unused,  \
            "The interface that Scylla binds to for connecting to other Scylla nodes. Interfaces must correspond to a single address, IP aliasing is not supported. See listen_address."  \
    )   \
    /* Default directories */   \
    /* If you have changed any of the default directories during installation, make sure you have root access and set these properties: */  \
    val(commitlog_directory, sstring, "/var/lib/scylla/commitlog", Used,   \
            "The directory where the commit log is stored. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories."   \
    )                                           \
    val(data_file_directories, string_list, { "/var/lib/scylla/data" }, Used,   \
            "The directory location where table data (SSTables) is stored"   \
    )                                           \
    val(hints_directory, sstring, "/var/lib/scylla/hints", Used,   \
            "The directory where hints files are stored if hinted handoff is enabled."   \
    )                                           \
    val(view_hints_directory, sstring, "/var/lib/scylla/view_hints", Used,   \
            "The directory where materialized-view updates are stored while a view replica is unreachable."   \
    )                                           \
    val(saved_caches_directory, sstring, "/var/lib/scylla/saved_caches", Unused, \
            "The directory location where table key and row caches are stored."  \
    )                                                   \
    /* Commonly used properties */  \
    /* Properties most frequently used when configuring Scylla. */   \
    /* Before starting a node for the first time, you should carefully evaluate your requirements. */   \
    /* Common initialization properties */  \
    /* Note: Be sure to set the properties in the Quick start section as well. */   \
    val(commit_failure_policy, sstring, "stop", Unused, \
            "Policy for commit disk failures:\n"    \
            "\n"    \
            "\tdie          Shut down gossip and Thrift and kill the JVM, so the node can be replaced.\n"   \
            "\tstop         Shut down gossip and Thrift, leaving the node effectively dead, but can be inspected using JMX.\n" \
            "\tstop_commit  Shut down the commit log, letting writes collect but continuing to service reads (as in pre-2.0.5 Cassandra).\n"    \
            "\tignore       Ignore fatal errors and let the batches fail."\
            , "die", "stop", "stop_commit", "ignore"    \
    )   \
    val(disk_failure_policy, sstring, "stop", Unused, \
            "Sets how Scylla responds to disk failure. Recommend settings are stop or best_effort.\n"    \
            "\n"    \
            "\tdie              Shut down gossip and Thrift and kill the JVM for any file system errors or single SSTable errors, so the node can be replaced.\n"   \
            "\tstop_paranoid    Shut down gossip and Thrift even for single SSTable errors.\n"    \
            "\tstop             Shut down gossip and Thrift, leaving the node effectively dead, but available for inspection using JMX.\n"    \
            "\tbest_effort      Stop using the failed disk and respond to requests based on the remaining available SSTables. This means you will see obsolete data at consistency level of ONE.\n"    \
            "\tignore           Ignores fatal errors and lets the requests fail; all file system errors are logged but otherwise ignored. Scylla acts as in versions prior to Cassandra 1.2.\n"    \
            "\n"    \
            "Related information: Handling Disk Failures In Cassandra 1.2 blog and Recovering from a single disk failure using JBOD.\n"    \
            , "die", "stop_paranoid", "stop", "best_effort", "ignore"   \
    )   \
    val(endpoint_snitch, sstring, "org.apache.cassandra.locator.SimpleSnitch", Used,  \
            "Set to a class that implements the IEndpointSnitch. Scylla uses snitches for locating nodes and routing requests.\n\n"    \
            "\tSimpleSnitch: Use for single-data center deployments or single-zone in public clouds. Does not recognize data center or rack information. It treats strategy order as proximity, which can improve cache locality when disabling read repair.\n\n"    \
            "\tGossipingPropertyFileSnitch: Recommended for production. The rack and data center for the local node are defined in the cassandra-rackdc.properties file and propagated to other nodes via gossip. To allow migration from the PropertyFileSnitch, it uses the cassandra-topology.properties file if it is present.\n\n"    \
            /*"\tPropertyFileSnitch: Determines proximity by rack and data center, which are explicitly configured in the cassandra-topology.properties file.\n\n"    */\
            "\tEc2Snitch: For EC2 deployments in a single region. Loads region and availability zone information from the EC2 API. The region is treated as the data center and the availability zone as the rack. Uses only private IPs. Subsequently it does not work across multiple regions.\n\n"    \
            "\tEc2MultiRegionSnitch: Uses public IPs as the broadcast_address to allow cross-region connectivity. This means you must also set seed addresses to the public IP and open the storage_port or ssl_storage_port on the public IP firewall. For intra-region traffic, Scylla switches to the private IP after establishing a connection.\n\n"    \
            "\tGoogleCloudSnitch: For deployments on Google Cloud Platform across one or more regions. The region is treated as a datacenter and the availability zone is treated as a rack within the datacenter. The communication should occur over private IPs within the same logical network.\n\n" \
            "\tRackInferringSnitch: Proximity is determined by rack and data center, which are assumed to correspond to the 3rd and 2nd octet of each node's IP address, respectively. This snitch is best used as an example for writing a custom snitch class (unless this happens to match your deployment conventions).\n" \
            "\n"    \
            "Related information: Snitches\n"    \
    )                                                   \
    val(rpc_address, sstring, "localhost", Used,     \
            "The listen address for client connections (Thrift RPC service and native transport).Valid values are:\n"    \
            "\n"    \
            "\tunset:   Resolves the address using the hostname configuration of the node. If left unset, the hostname must resolve to the IP address of this node using /etc/hostname, /etc/hosts, or DNS.\n"  \
            "\t0.0.0.0 : Listens on all configured interfaces, but you must set the broadcast_rpc_address to a value other than 0.0.0.0.\n"   \
            "\tIP address\n"    \
            "\thostname\n"  \
            "Related information: Network\n"    \
    )                                                   \
    val(rpc_interface, sstring, "eth1", Unused,     \
            "The listen address for client connections. Interfaces must correspond to a single address, IP aliasing is not supported. See rpc_address." \
    )   \
    val(seed_provider, seed_provider_type, seed_provider_type("org.apache.cassandra.locator.SimpleSeedProvider"), Used, \
            "The addresses of hosts deemed contact points. Scylla nodes use the -seeds list to find each other and learn the topology of the ring.\n"    \
            "\n"    \
            "  class_name (Default: org.apache.cassandra.locator.SimpleSeedProvider)\n" \
            "  \tThe class within Scylla that handles the seed logic. It can be customized, but this is typically not required.\n"   \
            "  \t- seeds (Default: 127.0.0.1)    A comma-delimited list of IP addresses used by gossip for bootstrapping new nodes joining a cluster. When running multiple nodes, you must change the list from the default value. In multiple data-center clusters, the seed list should include at least one node from each data center (replication group). More than a single seed node per data center is recommended for fault tolerance. Otherwise, gossip has to communicate with another data center when bootstrapping a node. Making every node a seed node is not recommended because of increased maintenance and reduced gossip performance. Gossip optimization is not critical, but it is recommended to use a small seed list (approximately three nodes per data center).\n"    \
            "\n"    \
            "Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers)."  \
    )                                                   \
    /* Common compaction settings */    \
    val(compaction_throughput_mb_per_sec, uint32_t, 16, Unused,     \
            "Throttles compaction to the specified total throughput across the entire system. The faster you insert data, the faster you need to compact in order to keep the SSTable count down. The recommended Value is 16 to 32 times the rate of write throughput (in MBs/second). Setting the value to 0 disables compaction throttling.\n"  \
            "Related information: Configuring compaction"   \
    )                                                   \
    val(compaction_large_partition_warning_threshold_mb, uint32_t, 1000, Used, \
            "Log a warning when writing partitions larger than this value"   \
    )                                               \
    val(compaction_large_row_warning_threshold_mb, uint32_t, 10, Used, \
            "Log a warning when writing rows larger than this value"   \
    )                                               \
    val(compaction_large_cell_warning_threshold_mb, uint32_t, 1, Used, \
            "Log a warning when writing cells larger than this value"   \
    )                                               \
    /* Common memtable settings */  \
    val(memtable_total_space_in_mb, uint32_t, 0, Invalid,     \
            "Specifies the total memory used for all memtables on a node. This replaces the per-table storage settings memtable_operations_in_millions and memtable_throughput_in_mb."  \
    )                                                   \
    /* Common disk settings */  \
    val(concurrent_reads, uint32_t, 32, Invalid,     \
            "For workloads with more data than can fit in memory, the bottleneck is reads fetching data from disk. Setting to (16 × number_of_drives) allows operations to queue low enough in the stack so that the OS and drives can reorder them."  \
    )                                                   \
    val(concurrent_writes, uint32_t, 32, Invalid,     \
            "Writes in Cassandra are rarely I/O bound, so the ideal number of concurrent writes depends on the number of CPU cores in your system. The recommended value is (8 x number_of_cpu_cores)."  \
    )                                                   \
    val(concurrent_counter_writes, uint32_t, 32, Unused,     \
            "Counter writes read the current values before incrementing and writing them back. The recommended value is (16 × number_of_drives) ."  \
    )                                                   \
    /* Common automatic backup settings */  \
    val(incremental_backups, bool, false, Used,     \
            "Backs up data updated since the last snapshot was taken. When enabled, Scylla creates a hard link to each SSTable flushed or streamed locally in a backups/ subdirectory of the keyspace data. Removing these links is the operator's responsibility.\n"  \
            "Related information: Enabling incremental backups" \
    )                                                   \
    val(snapshot_before_compaction, bool, false, Unused,     \
            "Enable or disable taking a snapshot before each compaction. This option is useful to back up data when there is a data format change. Be careful using this option because Cassandra does not clean up older snapshots automatically.\n"  \
            "Related information: Configuring compaction"   \
    )                                                   \
    /* Common fault detection setting */    \
    val(phi_convict_threshold, uint32_t, 8, Used,     \
            "Adjusts the sensitivity of the failure detector on an exponential scale. Generally this setting never needs adjusting.\n"  \
            "Related information: Failure detection and recovery"  \
    )                                                   \
    /* Performance tuning properties */ \
    /* Tuning performance and system reso   urce utilization, including commit log, compaction, memory, disk I/O, CPU, reads, and writes. */    \
    /* Commit log settings */   \
    val(commitlog_sync, sstring, "periodic", Used,     \
            "The method that Scylla uses to acknowledge writes in milliseconds:\n"   \
            "\n"    \
            "\tperiodic : Used with commitlog_sync_period_in_ms (Default: 10000 - 10 seconds ) to control how often the commit log is synchronized to disk. Periodic syncs are acknowledged immediately.\n"   \
            "\tbatch : Used with commitlog_sync_batch_window_in_ms (Default: disabled **) to control how long Scylla waits for other writes before performing a sync. When using this method, writes are not acknowledged until fsynced to disk.\n"  \
            "Related information: Durability"   \
    )                                                   \
    val(commitlog_segment_size_in_mb, uint32_t, 64, Used,     \
            "Sets the size of the individual commitlog file segments. A commitlog segment may be archived, deleted, or recycled after all its data has been flushed to SSTables. This amount of data can potentially include commitlog segments from every table in the system. The default size is usually suitable for most commitlog archiving, but if you want a finer granularity, 8 or 16 MB is reasonable. See Commit log archive configuration.\n"  \
            "Related information: Commit log archive configuration" \
    )                                                   \
    /* Note: does not exist on the listing page other than in above comment, wtf? */    \
    val(commitlog_sync_period_in_ms, uint32_t, 10000, Used,     \
            "Controls how long the system waits for other writes before performing a sync in \"periodic\" mode."    \
    )   \
    /* Note: does not exist on the listing page other than in above comment, wtf? */    \
    val(commitlog_sync_batch_window_in_ms, uint32_t, 10000, Used,     \
            "Controls how long the system waits for other writes before performing a sync in \"batch\" mode."    \
    )   \
    val(commitlog_total_space_in_mb, int64_t, -1, Used,     \
            "Total space used for commitlogs. If the used space goes above this value, Scylla rounds up to the next nearest segment multiple and flushes memtables to disk for the oldest commitlog segments, removing those log segments. This reduces the amount of data to replay on startup, and prevents infrequently-updated tables from indefinitely keeping commitlog segments. A small total commitlog space tends to cause more flush activity on less-active tables.\n"  \
            "Related information: Configuring memtable throughput"  \
    )                                                   \
    val(commitlog_reuse_segments, bool, true, Used,     \
            "Whether or not to re-use commitlog segments when finished instead of deleting them. Can improve commitlog latency on some file systems.\n"  \
    )                                                   \
    /* Compaction settings */   \
    /* Related information: Configuring compaction */   \
    val(compaction_preheat_key_cache, bool, true, Unused,                \
            "When set to true , cached row keys are tracked during compaction, and re-cached to their new positions in the compacted SSTable. If you have extremely large key caches for tables, set the value to false ; see Global row and key caches properties."  \
    )                                                   \
    val(concurrent_compactors, uint32_t, 0, Invalid,     \
            "Sets the number of concurrent compaction processes allowed to run simultaneously on a node, not including validation compactions for anti-entropy repair. Simultaneous compactions help preserve read performance in a mixed read-write workload by mitigating the tendency of small SSTables to accumulate during a single long-running compaction. If compactions run too slowly or too fast, change compaction_throughput_mb_per_sec first."  \
    )                                                   \
    val(in_memory_compaction_limit_in_mb, uint32_t, 64, Invalid,     \
            "Size limit for rows being compacted in memory. Larger rows spill to disk and use a slower two-pass compaction process. When this occurs, a message is logged specifying the row key. The recommended value is 5 to 10 percent of the available Java heap size."  \
    )                                                   \
    val(preheat_kernel_page_cache, bool, false, Unused, \
            "Enable or disable kernel page cache preheating from contents of the key cache after compaction. When enabled it preheats only first page (4KB) of each row to optimize for sequential access. It can be harmful for fat rows, see CASSANDRA-4937 for more details."    \
    )   \
    val(sstable_preemptive_open_interval_in_mb, uint32_t, 50, Unused,     \
            "When compacting, the replacement opens SSTables before they are completely written and uses in place of the prior SSTables for any range previously written. This setting helps to smoothly transfer reads between the SSTables by reducing page cache churn and keeps hot rows hot."  \
    )                                                   \
    val(defragment_memory_on_idle, bool, false, Used, "When set to true, will defragment memory when the cpu is idle.  This reduces the amount of work Scylla performs when processing client requests.") \
    /* Memtable settings */ \
    val(memtable_allocation_type, sstring, "heap_buffers", Invalid,     \
            "Specify the way Cassandra allocates and manages memtable memory. See Off-heap memtables in Cassandra 2.1. Options are:\n"  \
            "\theap_buffers     On heap NIO (non-blocking I/O) buffers.\n"  \
            "\toffheap_buffers  Off heap (direct) NIO buffers.\n"   \
            "\toffheap_objects  Native memory, eliminating NIO buffer heap overhead."   \
    )                                                   \
    val(memtable_cleanup_threshold, double, .11, Invalid, \
            "Ratio of occupied non-flushing memtable size to total permitted size for triggering a flush of the largest memtable. Larger values mean larger flushes and less compaction, but also less concurrent flush activity, which can make it difficult to keep your disks saturated under heavy write load." \
    )   \
    val(file_cache_size_in_mb, uint32_t, 512, Unused,  \
            "Total memory to use for SSTable-reading buffers."  \
    )   \
    val(memtable_flush_queue_size, uint32_t, 4, Unused,     \
            "The number of full memtables to allow pending flush (memtables waiting for a write thread). At a minimum, set to the maximum number of indexes created on a single table.\n"  \
            "Related information: Flushing data from the memtable"  \
    )   \
    val(memtable_flush_writers, uint32_t, 1, Invalid,     \
            "Sets the number of memtable flush writer threads. These threads are blocked by disk I/O, and each one holds a memtable in memory while blocked. If you have a large Java heap size and many data directories, you can increase the value for better flush performance."  \
    )   \
    val(memtable_heap_space_in_mb, uint32_t, 0, Unused,     \
            "Total permitted memory to use for memtables. Triggers a flush based on memtable_cleanup_threshold. Cassandra stops accepting writes when the limit is exceeded until a flush completes. If unset, sets to default."  \
    )   \
    val(memtable_offheap_space_in_mb, uint32_t, 0, Unused,     \
            "See memtable_heap_space_in_mb"  \
    )   \
    /* Cache and index settings */  \
    val(column_index_size_in_kb, uint32_t, 64, Used,     \
            "Granularity of the index of rows within a partition. For huge rows, decrease this setting to improve seek time. If you use key cache, be careful not to make this setting too large because key cache will be overwhelmed. If you're unsure of the size of the rows, it's best to use the default setting."  \
    )   \
    val(index_summary_capacity_in_mb, uint32_t, 0, Unused,     \
            "Fixed memory pool size in MB for SSTable index summaries. If the memory usage of all index summaries exceeds this limit, any SSTables with low read rates shrink their index summaries to meet this limit. This is a best-effort process. In extreme conditions, Cassandra may need to use more than this amount of memory."  \
    )   \
    val(index_summary_resize_interval_in_minutes, uint32_t, 60, Unused,     \
            "How frequently index summaries should be re-sampled. This is done periodically to redistribute memory from the fixed-size pool to SSTables proportional their recent read rates. To disable, set to -1. This leaves existing index summaries at their current sampling level."  \
    )   \
    val(reduce_cache_capacity_to, double, .6, Invalid,     \
            "Sets the size percentage to which maximum cache capacity is reduced when Java heap usage reaches the threshold defined by reduce_cache_sizes_at. Together with flush_largest_memtables_at, these properties constitute an emergency measure for preventing sudden out-of-memory (OOM) errors."  \
    )   \
    val(reduce_cache_sizes_at, double, .85, Invalid,     \
            "When Java heap usage (after a full concurrent mark sweep (CMS) garbage collection) exceeds this percentage, Cassandra reduces the cache capacity to the fraction of the current size as specified by reduce_cache_capacity_to. To disable, set the value to 1.0."  \
    )   \
    /* Disks settings */    \
    val(stream_throughput_outbound_megabits_per_sec, uint32_t, 400, Unused,     \
            "Throttles all outbound streaming file transfers on a node to the specified throughput. Cassandra does mostly sequential I/O when streaming data during bootstrap or repair, which can lead to saturating the network connection and degrading client (RPC) performance."  \
    )   \
    val(inter_dc_stream_throughput_outbound_megabits_per_sec, uint32_t, 0, Unused,     \
            "Throttles all streaming file transfer between the data centers. This setting allows throttles streaming throughput betweens data centers in addition to throttling all network stream traffic as configured with stream_throughput_outbound_megabits_per_sec."  \
    )   \
    val(trickle_fsync, bool, false, Unused,     \
            "When doing sequential writing, enabling this option tells fsync to force the operating system to flush the dirty buffers at a set interval trickle_fsync_interval_in_kb. Enable this parameter to avoid sudden dirty buffer flushing from impacting read latencies. Recommended to use on SSDs, but not on HDDs."  \
    )   \
    val(trickle_fsync_interval_in_kb, uint32_t, 10240, Unused,     \
            "Sets the size of the fsync in kilobytes."  \
    )   \
    /* Advanced properties */   \
    /* Properties for advanced users or properties that are less commonly used. */  \
    /* Advanced initialization properties */    \
    val(auto_bootstrap, bool, true, Used,     \
            "This setting has been removed from default configuration. It makes new (non-seed) nodes automatically migrate the right data to themselves. Do not set this to false unless you really know what you are doing.\n"  \
            "Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers)."  \
    )   \
    val(batch_size_warn_threshold_in_kb, uint32_t, 5, Used,     \
            "Log WARN on any batch size exceeding this value in kilobytes. Caution should be taken on increasing the size of this threshold as it can lead to node instability."  \
    )   \
    val(batch_size_fail_threshold_in_kb, uint32_t, 50, Used,     \
            "Fail any multiple-partition batch exceeding this value. 50kb (10x warn threshold) by default." \
    )   \
    val(broadcast_address, sstring, /* listen_address */, Used, \
            "The IP address a node tells other nodes in the cluster to contact it by. It allows public and private address to be different. For example, use the broadcast_address parameter in topologies where not all nodes have access to other nodes by their private IP addresses.\n" \
            "If your Scylla cluster is deployed across multiple Amazon EC2 regions and you use the EC2MultiRegionSnitch , set the broadcast_address to public IP address of the node and the listen_address to the private IP."    \
    )   \
    val(listen_on_broadcast_address, bool, false, Used, "When using multiple physical network interfaces, set this to true to listen on broadcast_address in addition to the listen_address, allowing nodes to communicate in both interfaces.  Ignore this property if the network configuration automatically routes between the public and private networks such as EC2." \
        )\
    val(initial_token, sstring, /* N/A */, Used,     \
            "Used in the single-node-per-token architecture, where a node owns exactly one contiguous range in the ring space. Setting this property overrides num_tokens.\n"   \
            "If you not using vnodes or have num_tokens set it to 1 or unspecified (#num_tokens), you should always specify this parameter when setting up a production cluster for the first time and when adding capacity. For more information, see this parameter in the Cassandra 1.1 Node and Cluster Configuration documentation.\n" \
            "This parameter can be used with num_tokens (vnodes ) in special cases such as Restoring from a snapshot." \
    )   \
    val(num_tokens, uint32_t, 1, Used,                \
            "Defines the number of tokens randomly assigned to this node on the ring when using virtual nodes (vnodes). The more tokens, relative to other nodes, the larger the proportion of data that the node stores. Generally all nodes should have the same number of tokens assuming equal hardware capability. The recommended value is 256. If unspecified (#num_tokens), Scylla uses 1 (equivalent to #num_tokens : 1) for legacy compatibility and uses the initial_token setting.\n"    \
            "If not using vnodes, comment #num_tokens : 256 or set num_tokens : 1 and use initial_token. If you already have an existing cluster with one token per node and wish to migrate to vnodes, see Enabling virtual nodes on an existing production cluster.\n"    \
            "Note: If using DataStax Enterprise, the default setting of this property depends on the type of node and type of install."  \
    )   \
    val(partitioner, sstring, "org.apache.cassandra.dht.Murmur3Partitioner", Used,                \
            "Distributes rows (by partition key) across all nodes in the cluster. Any IPartitioner may be used, including your own as long as it is in the class path. For new clusters use the default partitioner.\n" \
            "Scylla provides the following partitioners for backwards compatibility:\n"  \
            "\n"    \
            "\tRandomPartitioner\n" \
            "\tByteOrderedPartitioner\n"    \
            "\tOrderPreservingPartitioner (deprecated)\n"    \
            "\n"    \
            "Related information: Partitioners"  \
            , "org.apache.cassandra.dht.Murmur3Partitioner" \
            , "org.apache.cassandra.dht.RandomPartitioner" \
            , "org.apache.cassandra.dht.ByteOrderedPartitioner" \
            , "org.apache.cassandra.dht.OrderPreservingPartitioner" \
    )                                                   \
    val(storage_port, uint16_t, 7000, Used,                \
            "The port for inter-node communication."  \
    )                                                   \
    /* Advanced automatic backup setting */ \
    val(auto_snapshot, bool, true, Used,     \
            "Enable or disable whether a snapshot is taken of the data before keyspace truncation or dropping of tables. To prevent data loss, using the default setting is strongly advised. If you set to false, you will lose data on truncation or drop."  \
    )   \
    /* Key caches and global row properties */  \
    /* When creating or modifying tables, you enable or disable the key cache (partition key cache) or row cache for that table by setting the caching parameter. Other row and key cache tuning and configuration options are set at the global (node) level. Cassandra uses these settings to automatically distribute memory for each table on the node based on the overall workload and specific table usage. You can also configure the save periods for these caches globally. */    \
    /* Related information: Configuring caches */   \
    val(key_cache_keys_to_save, uint32_t, 0, Unused,                \
            "Number of keys from the key cache to save. (0: all)"  \
    )   \
    val(key_cache_save_period, uint32_t, 14400, Unused,                \
            "Duration in seconds that keys are saved in cache. Caches are saved to saved_caches_directory. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O."  \
    )   \
    val(key_cache_size_in_mb, uint32_t, 100, Unused,                \
            "A global cache setting for tables. It is the maximum size of the key cache in memory. To disable set to 0.\n"  \
            "Related information: nodetool setcachecapacity."   \
    )   \
    val(row_cache_keys_to_save, uint32_t, 0, Unused,                \
            "Number of keys from the row cache to save."  \
    )   \
    val(row_cache_size_in_mb, uint32_t, 0, Unused,                \
            "Maximum size of the row cache in memory. Row cache can save more time than key_cache_size_in_mb, but is space-intensive because it contains the entire row. Use the row cache only for hot rows or static rows. If you reduce the size, you may not get you hottest keys loaded on start up."  \
    )   \
    val(row_cache_save_period, uint32_t, 0, Unused,     \
            "Duration in seconds that rows are saved in cache. Caches are saved to saved_caches_directory."  \
    )   \
    val(memory_allocator, sstring, "NativeAllocator", Invalid,     \
            "The off-heap memory allocator. In addition to caches, this property affects storage engine meta data. Supported values:\n"  \
            "\tNativeAllocator\n"  \
            "\tJEMallocAllocator\n"  \
            "\n"  \
            "Experiments show that jemalloc saves some memory compared to the native allocator because it is more fragmentation resistant. To use, install jemalloc as a library and modify cassandra-env.sh (instructions in file)."  \
    )   \
    /* Counter caches properties */ \
    /* Counter cache helps to reduce counter locks' contention for hot counter cells. In case of RF = 1 a counter cache hit will cause Cassandra to skip the read before write entirely. With RF > 1 a counter cache hit will still help to reduce the duration of the lock hold, helping with hot counter cell updates, but will not allow skipping the read entirely. Only the local (clock, count) tuple of a counter cell is kept in memory, not the whole counter, so it's relatively cheap. */    \
    /* Note: Reducing the size counter cache may result in not getting the hottest keys loaded on start-up. */  \
    val(counter_cache_size_in_mb, uint32_t, 0, Unused,     \
            "When no value is specified a minimum of 2.5% of Heap or 50MB. If you perform counter deletes and rely on low gc_grace_seconds, you should disable the counter cache. To disable, set to 0"  \
    )   \
    val(counter_cache_save_period, uint32_t, 7200, Unused,     \
            "Duration after which Cassandra should save the counter cache (keys only). Caches are saved to saved_caches_directory."  \
    )   \
    val(counter_cache_keys_to_save, uint32_t, 0, Unused,     \
            "Number of keys from the counter cache to save. When disabled all keys are saved."  \
    )   \
    /* Tombstone settings */    \
    /* When executing a scan, within or across a partition, tombstones must be kept in memory to allow returning them to the coordinator. The coordinator uses them to ensure other replicas know about the deleted rows. Workloads that generate numerous tombstones may cause performance problems and exhaust the server heap. See Cassandra anti-patterns: Queues and queue-like datasets. Adjust these thresholds only if you understand the impact and want to scan more tombstones. Additionally, you can adjust these thresholds at runtime using the StorageServiceMBean. */   \
    /* Related information: Cassandra anti-patterns: Queues and queue-like datasets */  \
    val(tombstone_warn_threshold, uint32_t, 1000, Unused,     \
            "The maximum number of tombstones a query can scan before warning."  \
    )   \
    val(tombstone_failure_threshold, uint32_t, 100000, Unused,     \
            "The maximum number of tombstones a query can scan before aborting."  \
    )   \
    /* Network timeout settings */  \
    val(range_request_timeout_in_ms, uint32_t, 10000, Used,     \
            "The time in milliseconds that the coordinator waits for sequential or index scans to complete."  \
    )   \
    val(read_request_timeout_in_ms, uint32_t, 5000, Used,     \
            "The time that the coordinator waits for read operations to complete"  \
    )   \
    val(counter_write_request_timeout_in_ms, uint32_t, 5000, Used,     \
            "The time that the coordinator waits for counter writes to complete."  \
    )   \
    val(cas_contention_timeout_in_ms, uint32_t, 5000, Unused,     \
            "The time that the coordinator continues to retry a CAS (compare and set) operation that contends with other proposals for the same row."  \
    )   \
    val(truncate_request_timeout_in_ms, uint32_t, 10000, Used,     \
            "The time that the coordinator waits for truncates (remove all data from a table) to complete. The long default value allows for a snapshot to be taken before removing the data. If auto_snapshot is disabled (not recommended), you can reduce this time."  \
    )   \
    val(write_request_timeout_in_ms, uint32_t, 2000, Used,     \
            "The time in milliseconds that the coordinator waits for write operations to complete.\n"  \
            "Related information: About hinted handoff writes"  \
    )   \
    val(request_timeout_in_ms, uint32_t, 10000, Used,     \
            "The default timeout for other, miscellaneous operations.\n"  \
            "Related information: About hinted handoff writes"  \
    )   \
    /* Inter-node settings */   \
    val(cross_node_timeout, bool, false, Unused,                \
            "Enable or disable operation timeout information exchange between nodes (to accurately measure request timeouts). If disabled Cassandra assumes the request was forwarded to the replica instantly by the coordinator.\n"   \
            "CAUTION:\n"    \
            "Before enabling this property make sure NTP (network time protocol) is installed and the times are synchronized between the nodes."  \
    )   \
    val(internode_send_buff_size_in_bytes, uint32_t, 0, Unused,     \
            "Sets the sending socket buffer size in bytes for inter-node calls.\n"  \
            "When setting this parameter and internode_recv_buff_size_in_bytes, the buffer size is limited by net.core.wmem_max. When unset, buffer size is defined by net.ipv4.tcp_wmem. See man tcp and:\n"   \
            "\n"    \
            "\t/proc/sys/net/core/wmem_max\n"   \
            "\t/proc/sys/net/core/rmem_max\n"   \
            "\t/proc/sys/net/ipv4/tcp_wmem\n"   \
            "\t/proc/sys/net/ipv4/tcp_wmem\n"   \
    )   \
    val(internode_recv_buff_size_in_bytes, uint32_t, 0, Unused,     \
            "Sets the receiving socket buffer size in bytes for inter-node calls."  \
    )   \
    val(internode_compression, sstring, "none", Used,     \
            "Controls whether traffic between nodes is compressed. The valid values are:\n" \
            "\n"    \
            "\tall: All traffic is compressed.\n"   \
            "\tdc : Traffic between data centers is compressed.\n"  \
            "\tnone : No compression."  \
    )   \
    val(inter_dc_tcp_nodelay, bool, false, Used,     \
            "Enable or disable tcp_nodelay for inter-data center communication. When disabled larger, but fewer, network packets are sent. This reduces overhead from the TCP protocol itself. However, if cross data-center responses are blocked, it will increase latency."  \
    )   \
    val(streaming_socket_timeout_in_ms, uint32_t, 0, Unused,     \
            "Enable or disable socket timeout for streaming operations. When a timeout occurs during streaming, streaming is retried from the start of the current file. Avoid setting this value too low, as it can result in a significant amount of data re-streaming."  \
    )   \
    /* Native transport (CQL Binary Protocol) */    \
    val(start_native_transport, bool, true, Unused,                \
            "Enable or disable the native transport server. Uses the same address as the rpc_address, but the port is different from the rpc_port. See native_transport_port."  \
    )   \
    val(native_transport_port, uint16_t, 9042, Used,                \
            "Port on which the CQL native transport listens for clients."  \
    )   \
    val(native_transport_port_ssl, uint16_t, 9142, Used,                \
            "Port on which the CQL TLS native transport listens for clients."  \
            "Enabling client encryption and keeping native_transport_port_ssl disabled will use encryption" \
            "for native_transport_port. Setting native_transport_port_ssl to a different value" \
            "from native_transport_port will use encryption for native_transport_port_ssl while"    \
            "keeping native_transport_port unencrypted" \
    )   \
    val(native_transport_max_threads, uint32_t, 128, Invalid,                \
            "The maximum number of thread handling requests. The meaning is the same as rpc_max_threads.\n"  \
            "Default is different (128 versus unlimited).\n"  \
            "No corresponding native_transport_min_threads.\n"  \
            "Idle threads are stopped after 30 seconds.\n"  \
    )   \
    val(native_transport_max_frame_size_in_mb, uint32_t, 256, Unused,                \
            "The maximum size of allowed frame. Frame (requests) larger than this are rejected as invalid."  \
    )   \
    /* RPC (remote procedure call) settings */  \
    /* Settings for configuring and tuning client connections. */   \
    val(broadcast_rpc_address, sstring, /* unset */, Used,    \
            "RPC address to broadcast to drivers and other Scylla nodes. This cannot be set to 0.0.0.0. If blank, it is set to the value of the rpc_address or rpc_interface. If rpc_address or rpc_interfaceis set to 0.0.0.0, this property must be set.\n"    \
    )   \
    val(rpc_port, uint16_t, 9160, Used,                \
            "Thrift port for client connections."  \
    )   \
    val(start_rpc, bool, true, Used,                \
            "Starts the Thrift RPC server"  \
    )   \
    val(rpc_keepalive, bool, true, Used,     \
            "Enable or disable keepalive on client connections (RPC or native)."  \
    )   \
    val(rpc_max_threads, uint32_t, 0, Invalid,     \
            "Regardless of your choice of RPC server (rpc_server_type), the number of maximum requests in the RPC thread pool dictates how many concurrent requests are possible. However, if you are using the parameter sync in the rpc_server_type, it also dictates the number of clients that can be connected. For a large number of client connections, this could cause excessive memory usage for the thread stack. Connection pooling on the client side is highly recommended. Setting a maximum thread pool size acts as a safeguard against misbehaved clients. If the maximum is reached, Cassandra blocks additional connections until a client disconnects."  \
    )   \
    val(rpc_min_threads, uint32_t, 16, Invalid,     \
            "Sets the minimum thread pool size for remote procedure calls."  \
    )   \
    val(rpc_recv_buff_size_in_bytes, uint32_t, 0, Unused,     \
            "Sets the receiving socket buffer size for remote procedure calls."  \
    )                                                   \
    val(rpc_send_buff_size_in_bytes, uint32_t, 0, Unused,     \
            "Sets the sending socket buffer size in bytes for remote procedure calls."  \
    )                                                   \
    val(rpc_server_type, sstring, "sync", Unused,     \
            "Cassandra provides three options for the RPC server. On Windows, sync is about 30% slower than hsha. On Linux, sync and hsha performance is about the same, but hsha uses less memory.\n"  \
            "\n"    \
            "\tsync    (Default One thread per Thrift connection.) For a very large number of clients, memory is the limiting factor. On a 64-bit JVM, 180KB is the minimum stack size per thread and corresponds to your use of virtual memory. Physical memory may be limited depending on use of stack space.\n"   \
            "\thsh      Half synchronous, half asynchronous. All Thrift clients are handled asynchronously using a small number of threads that does not vary with the number of clients and thus scales well to many clients. The RPC requests are synchronous (one thread per active request).\n"   \
            "\t         Note: When selecting this option, you must change the default value (unlimited) of rpc_max_threads.\n"   \
            "\tYour own RPC server: You must provide a fully-qualified class name of an o.a.c.t.TServerFactory that can create a server instance."  \
    )   \
    val(cache_hit_rate_read_balancing, bool, true, Used, \
            "This boolean controls whether the replicas for read query will be choosen based on cache hit ratio"\
    ) \
    /* Advanced fault detection settings */ \
    /* Settings to handle poorly performing or failing nodes. */    \
    val(dynamic_snitch_badness_threshold, double, 0, Unused,     \
            "Sets the performance threshold for dynamically routing requests away from a poorly performing node. A value of 0.2 means Cassandra continues to prefer the static snitch values until the node response time is 20% worse than the best performing node. Until the threshold is reached, incoming client requests are statically routed to the closest replica (as determined by the snitch). Having requests consistently routed to a given replica can help keep a working set of data hot when read repair is less than 1."  \
    )   \
    val(dynamic_snitch_reset_interval_in_ms, uint32_t, 60000, Unused,     \
            "Time interval in milliseconds to reset all node scores, which allows a bad node to recover."  \
    )   \
    val(dynamic_snitch_update_interval_in_ms, uint32_t, 100, Unused,     \
            "The time interval for how often the snitch calculates node scores. Because score calculation is CPU intensive, be careful when reducing this interval."  \
    )   \
    val(hinted_handoff_enabled, sstring, "true", Used,     \
            "Enable or disable hinted handoff. To enable per data center, add data center list. For example: hinted_handoff_enabled: DC1,DC2. A hint indicates that the write needs to be replayed to an unavailable node. " \
            "Related information: About hinted handoff writes"  \
    )   \
    val(hinted_handoff_throttle_in_kb, uint32_t, 1024, Unused,     \
            "Maximum throttle per delivery thread in kilobytes per second. This rate reduces proportionally to the number of nodes in the cluster. For example, if there are two nodes in the cluster, each delivery thread will use the maximum rate. If there are three, each node will throttle to half of the maximum, since the two nodes are expected to deliver hints simultaneously."  \
    )   \
    val(max_hint_window_in_ms, uint32_t, 10800000, Used,     \
            "Maximum amount of time that hints are generates hints for an unresponsive node. After this interval, new hints are no longer generated until the node is back up and responsive. If the node goes down again, a new interval begins. This setting can prevent a sudden demand for resources when a node is brought back online and the rest of the cluster attempts to replay a large volume of hinted writes.\n"  \
            "Related information: Failure detection and recovery"  \
    )   \
    val(max_hints_delivery_threads, uint32_t, 2, Invalid,     \
            "Number of threads with which to deliver hints. In multiple data-center deployments, consider increasing this number because cross data-center handoff is generally slower."  \
    )   \
    val(batchlog_replay_throttle_in_kb, uint32_t, 1024, Unused,     \
            "Total maximum throttle. Throttling is reduced proportionally to the number of nodes in the cluster."  \
    )   \
    /* Request scheduler properties */  \
    /* Settings to handle incoming client requests according to a defined policy. If you need to use these properties, your nodes are overloaded and dropping requests. It is recommended that you add more nodes and not try to prioritize requests. */    \
    val(request_scheduler, sstring, "org.apache.cassandra.scheduler.NoScheduler", Unused,     \
            "Defines a scheduler to handle incoming client requests according to a defined policy. This scheduler is useful for throttling client requests in single clusters containing multiple keyspaces. This parameter is specifically for requests from the client and does not affect inter-node communication. Valid values are:\n" \
            "\n"    \
            "\torg.apache.cassandra.scheduler.NoScheduler   No scheduling takes place.\n"   \
            "\torg.apache.cassandra.scheduler.RoundRobinScheduler   Round robin of client requests to a node with a separate queue for each request_scheduler_id property.\n"   \
            "\tA Java class that implements the RequestScheduler interface."  \
            , "org.apache.cassandra.scheduler.NoScheduler"  \
            , "org.apache.cassandra.scheduler.RoundRobinScheduler"  \
    )   \
    val(request_scheduler_id, sstring, /* keyspace */, Unused,     \
            "An identifier on which to perform request scheduling. Currently the only valid value is keyspace. See weights."  \
    )   \
    val(request_scheduler_options, string_map, /* disabled */, Unused,     \
            "Contains a list of properties that define configuration options for request_scheduler:\n"  \
            "\n"    \
            "\tthrottle_limit: The number of in-flight requests per client. Requests beyond this limit are queued up until running requests complete. Recommended value is ((concurrent_reads + concurrent_writes) × 2)\n" \
            "\tdefault_weight: (Default: 1 **)  How many requests are handled during each turn of the RoundRobin.\n" \
            "\tweights: (Default: Keyspace: 1)  Takes a list of keyspaces. It sets how many requests are handled during each turn of the RoundRobin, based on the request_scheduler_id."  \
    )   \
    /* Thrift interface properties */   \
    /* Legacy API for older clients. CQL is a simpler and better API for Scylla. */  \
    val(thrift_framed_transport_size_in_mb, uint32_t, 15, Unused,     \
            "Frame size (maximum field length) for Thrift. The frame is the row or part of the row the application is inserting."  \
    )   \
    val(thrift_max_message_length_in_mb, uint32_t, 16, Used,     \
            "The maximum length of a Thrift message in megabytes, including all fields and internal Thrift overhead (1 byte of overhead for each frame). Message length is usually used in conjunction with batches. A frame length greater than or equal to 24 accommodates a batch with four inserts, each of which is 24 bytes. The required message length is greater than or equal to 24+24+24+24+4 (number of frames)."  \
    )   \
    /* Security properties */   \
    /* Server and client security settings. */  \
    val(authenticator, sstring, "org.apache.cassandra.auth.AllowAllAuthenticator", Used,     \
            "The authentication backend, used to identify users. The available authenticators are:\n"    \
            "\n"    \
            "\torg.apache.cassandra.auth.AllowAllAuthenticator : Disables authentication; no checks are performed.\n"   \
            "\torg.apache.cassandra.auth.PasswordAuthenticator : Authenticates users with user names and hashed passwords stored in the system_auth.credentials table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.\n"  \
            "Related information: Internal authentication"  \
            , "org.apache.cassandra.auth.AllowAllAuthenticator" \
            , "org.apache.cassandra.auth.PasswordAuthenticator" \
    )   \
    val(internode_authenticator, sstring, "enabled", Unused,     \
            "Internode authentication backend. It implements org.apache.cassandra.auth.AllowAllInternodeAuthenticator to allows or disallow connections from peer nodes."  \
    )   \
    val(authorizer, sstring, "org.apache.cassandra.auth.AllowAllAuthorizer", Used,     \
            "The authorization backend. It implements IAuthenticator, which limits access and provides permissions. The available authorizers are:\n"    \
            "\n"    \
            "\tAllowAllAuthorizer : Disables authorization; allows any action to any user.\n"   \
            "\tCassandraAuthorizer : Stores permissions in system_auth.permissions table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.\n"  \
            "Related information: Object permissions"   \
            , "org.apache.cassandra.auth.AllowAllAuthorizer" \
            , "org.apache.cassandra.auth.CassandraAuthorizer" \
    )   \
    val(role_manager, sstring, "org.apache.cassandra.auth.CassandraRoleManager", Used,    \
            "The role-management backend, used to maintain grantts and memberships between roles.\n"    \
            "The available role-managers are:\n"    \
            "\tCassandraRoleManager : Stores role data in the system_auth keyspace."    \
    )   \
    val(permissions_validity_in_ms, uint32_t, 10000, Used,     \
            "How long permissions in cache remain valid. Depending on the authorizer, such as CassandraAuthorizer, fetching permissions can be resource intensive. Permissions caching is disabled when this property is set to 0 or when AllowAllAuthorizer is used. The cached value is considered valid as long as both its value is not older than the permissions_validity_in_ms " \
            "and the cached value has been read at least once during the permissions_validity_in_ms time frame. If any of these two conditions doesn't hold the cached value is going to be evicted from the cache.\n"  \
            "Related information: Object permissions"   \
    )   \
    val(permissions_update_interval_in_ms, uint32_t, 2000, Used,     \
            "Refresh interval for permissions cache (if enabled). After this interval, cache entries become eligible for refresh. An async reload is scheduled every permissions_update_interval_in_ms time period and the old value is returned until it completes. If permissions_validity_in_ms has a non-zero value, then this property must also have a non-zero value. It's recommended to set this value to be at least 3 times smaller than the permissions_validity_in_ms."   \
    )   \
    val(permissions_cache_max_entries, uint32_t, 1000, Used,    \
            "Maximum cached permission entries. Must have a non-zero value if permissions caching is enabled (see a permissions_validity_in_ms description)." \
    )   \
    val(server_encryption_options, string_map, /*none*/, Used,     \
            "Enable or disable inter-node encryption. You must also generate keys and provide the appropriate key and trust store locations and passwords. No custom encryption options are currently enabled. The available options are:\n"    \
            "\n"    \
            "internode_encryption : (Default: none ) Enable or disable encryption of inter-node communication using the TLS_RSA_WITH_AES_128_CBC_SHA cipher suite for authentication, key exchange, and encryption of data transfers. The available inter-node options are:\n"  \
            "\tall : Encrypt all inter-node communications.\n"  \
            "\tnone : No encryption.\n" \
            "\tdc : Encrypt the traffic between the data centers (server only).\n"  \
            "\track : Encrypt the traffic between the racks(server only).\n"    \
            "certificate : (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the internode communication.\n"    \
            "keyfile : (Default: conf/scylla.key) PEM Key file associated with certificate.\n"  \
            "truststore : (Default: <system truststore> ) Location of the truststore containing the trusted certificate for authenticating remote servers.\n"    \
            "\n"    \
            "The advanced settings are:\n"  \
            "\n"    \
            "\tpriority_string : GnuTLS priority string controlling TLS algorithms used/allowed.\n"   \
            "\trequire_client_auth : (Default: false ) Enables or disables certificate authentication.\n" \
            "Related information: Node-to-node encryption"  \
    )   \
    val(client_encryption_options, string_map, /*none*/, Used,     \
            "Enable or disable client-to-node encryption. You must also generate keys and provide the appropriate key and certificate. No custom encryption options are currently enabled. The available options are:\n"    \
            "\n"    \
            "\tenabled : (Default: false ) To enable, set to true.\n"    \
            "\tcertificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.\n"   \
            "\tkeyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.\n"   \
            "truststore : (Default: <system truststore> ) Location of the truststore containing the trusted certificate for authenticating remote servers.\n"    \
            "\n"    \
            "The advanced settings are:\n"  \
            "\n"    \
            "\tpriority_string : GnuTLS priority string controlling TLS algorithms used/allowed.\n"   \
            "\trequire_client_auth : (Default: false ) Enables or disables certificate authentication.\n" \
            "Related information: Client-to-node encryption"    \
    )   \
    val(ssl_storage_port, uint32_t, 7001, Used,     \
            "The SSL port for encrypted communication. Unused unless enabled in encryption_options."  \
    )                                                   \
    val(enable_in_memory_data_store, bool, false, Used, "Enable in memory mode (system tables are always persisted)") \
    val(enable_cache, bool, true, Used, "Enable cache") \
    val(enable_commitlog, bool, true, Used, "Enable commitlog") \
    val(volatile_system_keyspace_for_testing, bool, false, Used, "Don't persist system keyspace - testing only!") \
    val(api_port, uint16_t, 10000, Used, "Http Rest API port") \
    val(api_address, sstring, "", Used, "Http Rest API address") \
    val(api_ui_dir, sstring, "swagger-ui/dist/", Used, "The directory location of the API GUI") \
    val(api_doc_dir, sstring, "api/api-doc/", Used, "The API definition file directory") \
    val(load_balance, sstring, "none", Used, "CQL request load balancing: 'none' or round-robin'") \
    val(consistent_rangemovement, bool, true, Used, "When set to true, range movements will be consistent. It means: 1) it will refuse to bootstrap a new node if other bootstrapping/leaving/moving nodes detected. 2) data will be streamed to a new node only from the node which is no longer responsible for the token range. Same as -Dcassandra.consistent.rangemovement in cassandra") \
    val(join_ring, bool, true, Used, "When set to true, a node will join the token ring. When set to false, a node will not join the token ring. User can use nodetool join to initiate ring joinging later. Same as -Dcassandra.join_ring in cassandra.") \
    val(load_ring_state, bool, true, Used, "When set to true, load tokens and host_ids previously saved. Same as -Dcassandra.load_ring_state in cassandra.") \
    val(replace_node, sstring, "", Used, "The UUID of the node to replace. Same as -Dcassandra.replace_node in cssandra.") \
    val(replace_token, sstring, "", Used, "The tokens of the node to replace. Same as -Dcassandra.replace_token in cassandra.") \
    val(replace_address, sstring, "", Used, "The listen_address or broadcast_address of the dead node to replace. Same as -Dcassandra.replace_address.") \
    val(replace_address_first_boot, sstring, "", Used, "Like replace_address option, but if the node has been bootstrapped successfully it will be ignored. Same as -Dcassandra.replace_address_first_boot.") \
    val(override_decommission, bool, false, Used, "Set true to force a decommissioned node to join the cluster") \
    val(ring_delay_ms, uint32_t, 30 * 1000, Used, "Time a node waits to hear from other nodes before joining the ring in milliseconds. Same as -Dcassandra.ring_delay_ms in cassandra.") \
    val(shadow_round_ms, uint32_t, 300 * 1000, Used, "The maximum gossip shadow round time. Can be used to reduce the gossip feature check time during node boot up.") \
    val(fd_max_interval_ms, uint32_t, 2 * 1000, Used, "The maximum failure_detector interval time in milliseconds. Interval larger than the maximum will be ignored. Larger cluster may need to increase the default.") \
    val(fd_initial_value_ms, uint32_t, 2 * 1000, Used, "The initial failure_detector interval time in milliseconds.") \
    val(shutdown_announce_in_ms, uint32_t, 2 * 1000, Used, "Time a node waits after sending gossip shutdown message in milliseconds. Same as -Dcassandra.shutdown_announce_in_ms in cassandra.") \
    val(developer_mode, bool, false, Used, "Relax environment checks. Setting to true can reduce performance and reliability significantly.") \
    val(skip_wait_for_gossip_to_settle, int32_t, -1, Used, "An integer to configure the wait for gossip to settle. -1: wait normally, 0: do not wait at all, n: wait for at most n polls. Same as -Dcassandra.skip_wait_for_gossip_to_settle in cassandra.") \
    val(force_gossip_generation, int32_t, -1, Used, "Force gossip to use the generation number provided by user") \
    val(experimental, bool, false, Used, "Set to true to unlock experimental features.") \
    val(lsa_reclamation_step, size_t, 1, Used, "Minimum number of segments to reclaim in a single step") \
    val(prometheus_port, uint16_t, 9180, Used, "Prometheus port, set to zero to disable") \
    val(prometheus_address, sstring, "0.0.0.0", Used, "Prometheus listening address") \
    val(prometheus_prefix, sstring, "scylla", Used, "Set the prefix of the exported Prometheus metrics. Changing this will break Scylla's dashboard compatibility, do not change unless you know what you are doing.") \
    val(abort_on_lsa_bad_alloc, bool, false, Used, "Abort when allocation in LSA region fails") \
    val(murmur3_partitioner_ignore_msb_bits, unsigned, 12, Used, "Number of most siginificant token bits to ignore in murmur3 partitioner; increase for very large clusters") \
    val(virtual_dirty_soft_limit, double, 0.6, Used, "Soft limit of virtual dirty memory expressed as a portion of the hard limit") \
    val(sstable_summary_ratio, double, 0.0005, Used, "Enforces that 1 byte of summary is written for every N (2000 by default) " \
        "bytes written to data file. Value must be between 0 and 1.") \
    val(large_memory_allocation_warning_threshold, size_t, size_t(1) << 20, Used, "Warn about memory allocations above this size; set to zero to disable") \
    val(enable_deprecated_partitioners, bool, false, Used, "Enable the byteordered and murmurs partitioners. These partitioners are deprecated and will be removed in a future version.") \
    val(enable_keyspace_column_family_metrics, bool, false, Used, "Enable per keyspace and per column family metrics reporting") \
    val(enable_sstable_data_integrity_check, bool, false, Used, "Enable interposer which checks for integrity of every sstable write." \
        " Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.") \
    val(cpu_scheduler, bool, true, Used, "Enable cpu scheduling") \
    val(view_building, bool, true, Used, "Enable view building; should only be set to false when the node is experience issues due to view building") \
    val(enable_sstables_mc_format, bool, true, Used, "Enable SSTables 'mc' format to be used as the default file format") \
    val(enable_dangerous_direct_import_of_cassandra_counters, bool, false, Used, "Only turn this option on if you want to import tables from Cassandra containing counters, and you are SURE that no counters in that table were created in a version earlier than Cassandra 2.1." \
        " It is not enough to have ever since upgraded to newer versions of Cassandra. If you EVER used a version earlier than 2.1 in the cluster where these SSTables come from, DO NOT TURN ON THIS OPTION! You will corrupt your data. You have been warned.") \
    val(enable_shard_aware_drivers, bool, true, Used, "Enable native transport drivers to use connection-per-shard for better performance") \
    val(abort_on_internal_error, bool, false, Used, "Abort the server instead of throwing exception when internal invariants are violated.") \
    val(enable_3_1_0_compatibility_mode, bool, false, Used, "Set to true if the cluster was initially installed from 3.1.0. If it was upgraded from an earlier version, or installed from a later version, leave this set to false. This adjusts the communication protocol to work around a bug in Scylla 3.1.0") \
    /* done! */

#define _make_value_member(name, type, deflt, status, desc, ...)    \
    named_value<type, value_status::status> name;

    _make_config_values(_make_value_member)

    seastar::logging_settings logging_settings(const boost::program_options::variables_map&) const;

    boost::program_options::options_description_easy_init&
    add_options(boost::program_options::options_description_easy_init&);

    const db::extensions& extensions() const;

    static const sstring default_tls_priority;
private:
    template<typename T>
    struct log_legacy_value : public named_value<T, value_status::Used> {
        using MyBase = named_value<T, value_status::Used>;

        using MyBase::MyBase;

        T value_or(T&& t) const {
            return this->is_set() ? (*this)() : t;
        }
        // do not add to boost::options. We only care about yaml config
        void add_command_line_option(boost::program_options::options_description_easy_init&,
                        const std::string_view&, const std::string_view&) override {}
    };

    log_legacy_value<seastar::log_level> default_log_level;
    log_legacy_value<std::unordered_map<sstring, seastar::log_level>> logger_log_level;
    log_legacy_value<bool> log_to_stdout, log_to_syslog;

    std::shared_ptr<db::extensions> _extensions;
};

}
