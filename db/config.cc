/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <unordered_map>
#include <regex>
#include <sstream>

#include <boost/any.hpp>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

#include "cdc/cdc_extension.hh"
#include "config.hh"
#include "extensions.hh"
#include "log.hh"
#include "utils/config_file_impl.hh"

namespace utils {

template <typename T>
static
json::json_return_type
value_to_json(const T& value) {
    return json::json_return_type(value);
}

static
json::json_return_type
log_level_to_json(const log_level& ll) {
    return value_to_json(format("{}", ll));
}

static
json::json_return_type
log_level_map_to_json(const std::unordered_map<sstring, log_level>& llm) {
    std::unordered_map<sstring, sstring> converted;
    for (auto&& [k, v] : llm) {
        converted[k] = format("{}", v);
    }
    return value_to_json(converted);
}

static
json::json_return_type
seed_provider_to_json(const db::seed_provider_type& spt) {
    return value_to_json("seed_provider_type");
}

static
json::json_return_type
hinted_handoff_enabled_to_json(const db::config::hinted_handoff_enabled_type& h) {
    return value_to_json(h.to_configuration_string());
}

template <>
const config_type config_type_for<bool> = config_type("bool", value_to_json<bool>);

template <>
const config_type config_type_for<uint16_t> = config_type("integer", value_to_json<uint16_t>);

template <>
const config_type config_type_for<uint32_t> = config_type("integer", value_to_json<uint32_t>);

template <>
const config_type config_type_for<uint64_t> = config_type("integer", value_to_json<uint64_t>);

template <>
const config_type config_type_for<float> = config_type("float", value_to_json<float>);

template <>
const config_type config_type_for<double> = config_type("double", value_to_json<double>);

template <>
const config_type config_type_for<log_level> = config_type("string", log_level_to_json);

template <>
const config_type config_type_for<sstring> = config_type("string", value_to_json<sstring>);

template <>
const config_type config_type_for<std::vector<sstring>> = config_type("string list", value_to_json<std::vector<sstring>>);

template <>
const config_type config_type_for<std::unordered_map<sstring, sstring>> = config_type("string map", value_to_json<std::unordered_map<sstring, sstring>>);

template <>
const config_type config_type_for<std::unordered_map<sstring, log_level>> = config_type("string map", log_level_map_to_json);

template <>
const config_type config_type_for<int64_t> = config_type("integer", value_to_json<int64_t>);

template <>
const config_type config_type_for<int32_t> = config_type("integer", value_to_json<int32_t>);

template <>
const config_type config_type_for<db::seed_provider_type> = config_type("seed provider", seed_provider_to_json);

template <>
const config_type config_type_for<std::vector<enum_option<db::experimental_features_t>>> = config_type(
        "experimental features", value_to_json<std::vector<sstring>>);

template <>
const config_type config_type_for<db::config::hinted_handoff_enabled_type> = config_type("hinted handoff enabled", hinted_handoff_enabled_to_json);

}

namespace YAML {

// yaml-cpp conversion would do well to have some enable_if-stuff to make it possible
// to do more broad spectrum converters.
template<>
struct convert<seastar::log_level> {
    static bool decode(const Node& node, seastar::log_level& rhs) {
        std::string tmp;
        if (!convert<std::string>::decode(node, tmp)) {
            return false;
        }
        rhs = boost::lexical_cast<seastar::log_level>(tmp);
        return true;
    }
};

template<>
struct convert<db::config::seed_provider_type> {
    static bool decode(const Node& node, db::config::seed_provider_type& rhs) {
        if (!node.IsSequence()) {
            return false;
        }
        rhs = db::config::seed_provider_type();
        for (auto& n : node) {
            if (!n.IsMap()) {
                continue;
            }
            for (auto& n2 : n) {
                if (n2.first.as<sstring>() == "class_name") {
                    rhs.class_name = n2.second.as<sstring>();
                }
                if (n2.first.as<sstring>() == "parameters") {
                    auto v = n2.second.as<std::vector<db::config::string_map>>();
                    if (!v.empty()) {
                        rhs.parameters = v.front();
                    }
                }
            }
        }
        return true;
    }
};

template<>
struct convert<db::config::hinted_handoff_enabled_type> {
    static bool decode(const Node& node, db::config::hinted_handoff_enabled_type& rhs) {
        std::string opt;
        if (!convert<std::string>::decode(node, opt)) {
            return false;
        }
        rhs = db::hints::host_filter::parse_from_config_string(std::move(opt));
        return true;
    }
};

template <>
class convert<enum_option<db::experimental_features_t>> {
public:
    static bool decode(const Node& node, enum_option<db::experimental_features_t>& rhs) {
        std::string name;
        if (!convert<std::string>::decode(node, name)) {
            return false;
        }
        try {
            std::istringstream(name) >> rhs;
        } catch (boost::program_options::invalid_option_value&) {
            return false;
        }
        return true;
    }
};

}

#if defined(DEBUG)
#define ENABLE_SSTABLE_KEY_VALIDATION true
#else
#define ENABLE_SSTABLE_KEY_VALIDATION false
#endif

#define str(x)  #x
#define _mk_init(name, type, deflt, status, desc, ...)  , name(this, str(name), value_status::status, type(deflt), desc)

db::config::config(std::shared_ptr<db::extensions> exts)
    : utils::config_file()

    , background_writer_scheduling_quota(this, "background_writer_scheduling_quota", value_status::Unused, 1.0,
        "max cpu usage ratio (between 0 and 1) for compaction process. Not intended for setting in normal operations. Setting it to 1 or higher will disable it, recommended operational setting is 0.5.")
    , auto_adjust_flush_quota(this, "auto_adjust_flush_quota", value_status::Unused, false,
        "true: auto-adjust memtable shares for flush processes")
    , memtable_flush_static_shares(this, "memtable_flush_static_shares", value_status::Used, 0,
        "If set to higher than 0, ignore the controller's output and set the memtable shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity")
    , compaction_static_shares(this, "compaction_static_shares", value_status::Used, 0,
        "If set to higher than 0, ignore the controller's output and set the compaction shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity")
    , compaction_enforce_min_threshold(this, "compaction_enforce_min_threshold", liveness::LiveUpdate, value_status::Used, false,
        "If set to true, enforce the min_threshold option for compactions strictly. If false (default), Scylla may decide to compact even if below min_threshold")
    /* Initialization properties */
    /* The minimal properties needed for configuring a cluster. */
    , cluster_name(this, "cluster_name", value_status::Used, "",
        "The name of the cluster; used to prevent machines in one logical cluster from joining another. All nodes participating in a cluster must have the same value.")
    , listen_address(this, "listen_address", value_status::Used, "localhost",
        "The IP address or hostname that Scylla binds to for connecting to other Scylla nodes. You must change the default setting for multiple nodes to communicate. Do not set to 0.0.0.0, unless you have set broadcast_address to an address that other nodes can use to reach this node.")
    , listen_interface(this, "listen_interface", value_status::Unused, "eth0",
        "The interface that Scylla binds to for connecting to other Scylla nodes. Interfaces must correspond to a single address, IP aliasing is not supported. See listen_address.")
    , listen_interface_prefer_ipv6(this, "listen_interface_prefer_ipv6", value_status::Used, false,
        "If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address\n"
        "you can specify which should be chosen using listen_interface_prefer_ipv6. If false the first ipv4\n"
        "address will be used. If true the first ipv6 address will be used. Defaults to false preferring\n"
        "ipv4. If there is only one address it will be selected regardless of ipv4/ipv6."
    )
    /* Default directories */
    /* If you have changed any of the default directories during installation, make sure you have root access and set these properties: */
    , work_directory(this, "workdir,W", value_status::Used, "/var/lib/scylla",
        "The directory in which Scylla will put all its subdirectories. The location of individual subdirs can be overriden by the respective *_directory options.")
    , commitlog_directory(this, "commitlog_directory", value_status::Used, "",
        "The directory where the commit log is stored. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories.")
    , data_file_directories(this, "data_file_directories", "datadir", value_status::Used, { },
        "The directory location where table data (SSTables) is stored")
    , hints_directory(this, "hints_directory", value_status::Used, "",
        "The directory where hints files are stored if hinted handoff is enabled.")
    , view_hints_directory(this, "view_hints_directory", value_status::Used, "",
        "The directory where materialized-view updates are stored while a view replica is unreachable.")
    , saved_caches_directory(this, "saved_caches_directory", value_status::Unused, "",
        "The directory location where table key and row caches are stored.")
    /* Commonly used properties */
    /* Properties most frequently used when configuring Scylla. */
    /* Before starting a node for the first time, you should carefully evaluate your requirements. */
    /* Common initialization properties */
    /* Note: Be sure to set the properties in the Quick start section as well. */
    , commit_failure_policy(this, "commit_failure_policy", value_status::Unused, "stop",
        "Policy for commit disk failures:\n"
        "\n"
        "\tdie          Shut down gossip and Thrift and kill the JVM, so the node can be replaced.\n"
        "\tstop         Shut down gossip and Thrift, leaving the node effectively dead, but can be inspected using JMX.\n"
        "\tstop_commit  Shut down the commit log, letting writes collect but continuing to service reads (as in pre-2.0.5 Cassandra).\n"
        "\tignore       Ignore fatal errors and let the batches fail."
        , {"die", "stop", "stop_commit", "ignore"})
    , disk_failure_policy(this, "disk_failure_policy", value_status::Unused, "stop",
        "Sets how Scylla responds to disk failure. Recommend settings are stop or best_effort.\n"
        "\n"
        "\tdie              Shut down gossip and Thrift and kill the JVM for any file system errors or single SSTable errors, so the node can be replaced.\n"
        "\tstop_paranoid    Shut down gossip and Thrift even for single SSTable errors.\n"
        "\tstop             Shut down gossip and Thrift, leaving the node effectively dead, but available for inspection using JMX.\n"
        "\tbest_effort      Stop using the failed disk and respond to requests based on the remaining available SSTables. This means you will see obsolete data at consistency level of ONE.\n"
        "\tignore           Ignores fatal errors and lets the requests fail; all file system errors are logged but otherwise ignored. Scylla acts as in versions prior to Cassandra 1.2.\n"
        "\n"
        "Related information: Handling Disk Failures In Cassandra 1.2 blog and Recovering from a single disk failure using JBOD.\n"
        , {"die", "stop_paranoid", "stop", "best_effort", "ignore"})
    , endpoint_snitch(this, "endpoint_snitch", value_status::Used, "org.apache.cassandra.locator.SimpleSnitch",
        "Set to a class that implements the IEndpointSnitch. Scylla uses snitches for locating nodes and routing requests.\n\n"
        "\tSimpleSnitch: Use for single-data center deployments or single-zone in public clouds. Does not recognize data center or rack information. It treats strategy order as proximity, which can improve cache locality when disabling read repair.\n\n"
        "\tGossipingPropertyFileSnitch: Recommended for production. The rack and data center for the local node are defined in the cassandra-rackdc.properties file and propagated to other nodes via gossip. To allow migration from the PropertyFileSnitch, it uses the cassandra-topology.properties file if it is present.\n\n"
        /*"\tPropertyFileSnitch: Determines proximity by rack and data center, which are explicitly configured in the cassandra-topology.properties file.\n\n"    */
        "\tEc2Snitch: For EC2 deployments in a single region. Loads region and availability zone information from the EC2 API. The region is treated as the data center and the availability zone as the rack. Uses only private IPs. Subsequently it does not work across multiple regions.\n\n"
        "\tEc2MultiRegionSnitch: Uses public IPs as the broadcast_address to allow cross-region connectivity. This means you must also set seed addresses to the public IP and open the storage_port or ssl_storage_port on the public IP firewall. For intra-region traffic, Scylla switches to the private IP after establishing a connection.\n\n"
        "\tGoogleCloudSnitch: For deployments on Google Cloud Platform across one or more regions. The region is treated as a datacenter and the availability zone is treated as a rack within the datacenter. The communication should occur over private IPs within the same logical network.\n\n"
        "\tRackInferringSnitch: Proximity is determined by rack and data center, which are assumed to correspond to the 3rd and 2nd octet of each node's IP address, respectively. This snitch is best used as an example for writing a custom snitch class (unless this happens to match your deployment conventions).\n"
        "\n"
        "Related information: Snitches\n")
    , rpc_address(this, "rpc_address", value_status::Used, "localhost",
        "The listen address for client connections (Thrift RPC service and native transport).Valid values are:\n"
        "\n"
        "\tunset:   Resolves the address using the hostname configuration of the node. If left unset, the hostname must resolve to the IP address of this node using /etc/hostname, /etc/hosts, or DNS.\n"
        "\t0.0.0.0 : Listens on all configured interfaces, but you must set the broadcast_rpc_address to a value other than 0.0.0.0.\n"
        "\tIP address\n"
        "\thostname\n"
        "Related information: Network\n")
    , rpc_interface(this, "rpc_interface", value_status::Unused, "eth1",
        "The listen address for client connections. Interfaces must correspond to a single address, IP aliasing is not supported. See rpc_address.")
    , rpc_interface_prefer_ipv6(this, "rpc_interface_prefer_ipv6", value_status::Used, false,
        "If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address\n"
        "you can specify which should be chosen using rpc_interface_prefer_ipv6. If false the first ipv4\n"
        "address will be used. If true the first ipv6 address will be used. Defaults to false preferring\n"
        "ipv4. If there is only one address it will be selected regardless of ipv4/ipv6"
    )
    , seed_provider(this, "seed_provider", value_status::Used, seed_provider_type("org.apache.cassandra.locator.SimpleSeedProvider"),
        "The addresses of hosts deemed contact points. Scylla nodes use the -seeds list to find each other and learn the topology of the ring.\n"
        "\n"
        "  class_name (Default: org.apache.cassandra.locator.SimpleSeedProvider)\n"
        "  \tThe class within Scylla that handles the seed logic. It can be customized, but this is typically not required.\n"
        "  \t- seeds (Default: 127.0.0.1)    A comma-delimited list of IP addresses used by gossip for bootstrapping new nodes joining a cluster. When running multiple nodes, you must change the list from the default value. In multiple data-center clusters, the seed list should include at least one node from each data center (replication group). More than a single seed node per data center is recommended for fault tolerance. Otherwise, gossip has to communicate with another data center when bootstrapping a node. Making every node a seed node is not recommended because of increased maintenance and reduced gossip performance. Gossip optimization is not critical, but it is recommended to use a small seed list (approximately three nodes per data center).\n"
        "\n"
        "Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers).")
    /* Common compaction settings */
    , compaction_throughput_mb_per_sec(this, "compaction_throughput_mb_per_sec", value_status::Unused, 16,
        "Throttles compaction to the specified total throughput across the entire system. The faster you insert data, the faster you need to compact in order to keep the SSTable count down. The recommended Value is 16 to 32 times the rate of write throughput (in MBs/second). Setting the value to 0 disables compaction throttling.\n"
        "Related information: Configuring compaction")
    , compaction_large_partition_warning_threshold_mb(this, "compaction_large_partition_warning_threshold_mb", value_status::Used, 1000,
        "Log a warning when writing partitions larger than this value")
    , compaction_large_row_warning_threshold_mb(this, "compaction_large_row_warning_threshold_mb", value_status::Used, 10,
        "Log a warning when writing rows larger than this value")
    , compaction_large_cell_warning_threshold_mb(this, "compaction_large_cell_warning_threshold_mb", value_status::Used, 1,
        "Log a warning when writing cells larger than this value")
    , compaction_rows_count_warning_threshold(this, "compaction_rows_count_warning_threshold", value_status::Used, 100000,
        "Log a warning when writing a number of rows larger than this value")
    /* Common memtable settings */
    , memtable_total_space_in_mb(this, "memtable_total_space_in_mb", value_status::Invalid, 0,
        "Specifies the total memory used for all memtables on a node. This replaces the per-table storage settings memtable_operations_in_millions and memtable_throughput_in_mb.")
    /* Common disk settings */
    , concurrent_reads(this, "concurrent_reads", value_status::Invalid, 32,
        "For workloads with more data than can fit in memory, the bottleneck is reads fetching data from disk. Setting to (16 × number_of_drives) allows operations to queue low enough in the stack so that the OS and drives can reorder them.")
    , concurrent_writes(this, "concurrent_writes", value_status::Invalid, 32,
        "Writes in Cassandra are rarely I/O bound, so the ideal number of concurrent writes depends on the number of CPU cores in your system. The recommended value is (8 x number_of_cpu_cores).")
    , concurrent_counter_writes(this, "concurrent_counter_writes", value_status::Unused, 32,
        "Counter writes read the current values before incrementing and writing them back. The recommended value is (16 × number_of_drives) .")
    /* Common automatic backup settings */
    , incremental_backups(this, "incremental_backups", value_status::Used, false,
        "Backs up data updated since the last snapshot was taken. When enabled, Scylla creates a hard link to each SSTable flushed or streamed locally in a backups/ subdirectory of the keyspace data. Removing these links is the operator's responsibility.\n"
        "Related information: Enabling incremental backups")
    , snapshot_before_compaction(this, "snapshot_before_compaction", value_status::Unused, false,
        "Enable or disable taking a snapshot before each compaction. This option is useful to back up data when there is a data format change. Be careful using this option because Cassandra does not clean up older snapshots automatically.\n"
        "Related information: Configuring compaction")
    /* Common fault detection setting */
    , phi_convict_threshold(this, "phi_convict_threshold", value_status::Used, 8,
        "Adjusts the sensitivity of the failure detector on an exponential scale. Generally this setting never needs adjusting.\n"
        "Related information: Failure detection and recovery")
    , failure_detector_timeout_in_ms(this, "failure_detector_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 20 * 1000, "Maximum time between two successful echo message before gossip mark a node down in milliseconds.\n")
    /* Performance tuning properties */
    /* Tuning performance and system reso   urce utilization, including commit log, compaction, memory, disk I/O, CPU, reads, and writes. */
    /* Commit log settings */
    , commitlog_sync(this, "commitlog_sync", value_status::Used, "periodic",
        "The method that Scylla uses to acknowledge writes in milliseconds:\n"
        "\n"
        "\tperiodic : Used with commitlog_sync_period_in_ms (Default: 10000 - 10 seconds ) to control how often the commit log is synchronized to disk. Periodic syncs are acknowledged immediately.\n"
        "\tbatch : Used with commitlog_sync_batch_window_in_ms (Default: disabled **) to control how long Scylla waits for other writes before performing a sync. When using this method, writes are not acknowledged until fsynced to disk.\n"
        "Related information: Durability")
    , commitlog_segment_size_in_mb(this, "commitlog_segment_size_in_mb", value_status::Used, 64,
        "Sets the size of the individual commitlog file segments. A commitlog segment may be archived, deleted, or recycled after all its data has been flushed to SSTables. This amount of data can potentially include commitlog segments from every table in the system. The default size is usually suitable for most commitlog archiving, but if you want a finer granularity, 8 or 16 MB is reasonable. See Commit log archive configuration.\n"
        "Related information: Commit log archive configuration")
    /* Note: does not exist on the listing page other than in above comment, wtf? */
    , commitlog_sync_period_in_ms(this, "commitlog_sync_period_in_ms", value_status::Used, 10000,
        "Controls how long the system waits for other writes before performing a sync in \"periodic\" mode.")
    /* Note: does not exist on the listing page other than in above comment, wtf? */
    , commitlog_sync_batch_window_in_ms(this, "commitlog_sync_batch_window_in_ms", value_status::Used, 10000,
        "Controls how long the system waits for other writes before performing a sync in \"batch\" mode.")
    , commitlog_total_space_in_mb(this, "commitlog_total_space_in_mb", value_status::Used, -1,
        "Total space used for commitlogs. If the used space goes above this value, Scylla rounds up to the next nearest segment multiple and flushes memtables to disk for the oldest commitlog segments, removing those log segments. This reduces the amount of data to replay on startup, and prevents infrequently-updated tables from indefinitely keeping commitlog segments. A small total commitlog space tends to cause more flush activity on less-active tables.\n"
        "Related information: Configuring memtable throughput")
    , commitlog_reuse_segments(this, "commitlog_reuse_segments", value_status::Used, true,
        "Whether or not to re-use commitlog segments when finished instead of deleting them. Can improve commitlog latency on some file systems.\n")
    , commitlog_use_o_dsync(this, "commitlog_use_o_dsync", value_status::Used, true,
        "Whether or not to use O_DSYNC mode for commitlog segments IO. Can improve commitlog latency on some file systems.\n")
    /* Compaction settings */
    /* Related information: Configuring compaction */
    , compaction_preheat_key_cache(this, "compaction_preheat_key_cache", value_status::Unused, true,
        "When set to true , cached row keys are tracked during compaction, and re-cached to their new positions in the compacted SSTable. If you have extremely large key caches for tables, set the value to false ; see Global row and key caches properties.")
    , concurrent_compactors(this, "concurrent_compactors", value_status::Invalid, 0,
        "Sets the number of concurrent compaction processes allowed to run simultaneously on a node, not including validation compactions for anti-entropy repair. Simultaneous compactions help preserve read performance in a mixed read-write workload by mitigating the tendency of small SSTables to accumulate during a single long-running compaction. If compactions run too slowly or too fast, change compaction_throughput_mb_per_sec first.")
    , in_memory_compaction_limit_in_mb(this, "in_memory_compaction_limit_in_mb", value_status::Invalid, 64,
        "Size limit for rows being compacted in memory. Larger rows spill to disk and use a slower two-pass compaction process. When this occurs, a message is logged specifying the row key. The recommended value is 5 to 10 percent of the available Java heap size.")
    , preheat_kernel_page_cache(this, "preheat_kernel_page_cache", value_status::Unused, false,
        "Enable or disable kernel page cache preheating from contents of the key cache after compaction. When enabled it preheats only first page (4KB) of each row to optimize for sequential access. It can be harmful for fat rows, see CASSANDRA-4937 for more details.")
    , sstable_preemptive_open_interval_in_mb(this, "sstable_preemptive_open_interval_in_mb", value_status::Unused, 50,
        "When compacting, the replacement opens SSTables before they are completely written and uses in place of the prior SSTables for any range previously written. This setting helps to smoothly transfer reads between the SSTables by reducing page cache churn and keeps hot rows hot.")
    , defragment_memory_on_idle(this, "defragment_memory_on_idle", value_status::Used, false, "When set to true, will defragment memory when the cpu is idle.  This reduces the amount of work Scylla performs when processing client requests.")
    /* Memtable settings */
    , memtable_allocation_type(this, "memtable_allocation_type", value_status::Invalid, "heap_buffers",
        "Specify the way Cassandra allocates and manages memtable memory. See Off-heap memtables in Cassandra 2.1. Options are:\n"
        "\theap_buffers     On heap NIO (non-blocking I/O) buffers.\n"
        "\toffheap_buffers  Off heap (direct) NIO buffers.\n"
        "\toffheap_objects  Native memory, eliminating NIO buffer heap overhead.")
    , memtable_cleanup_threshold(this, "memtable_cleanup_threshold", value_status::Invalid, .11,
        "Ratio of occupied non-flushing memtable size to total permitted size for triggering a flush of the largest memtable. Larger values mean larger flushes and less compaction, but also less concurrent flush activity, which can make it difficult to keep your disks saturated under heavy write load.")
    , file_cache_size_in_mb(this, "file_cache_size_in_mb", value_status::Unused, 512,
        "Total memory to use for SSTable-reading buffers.")
    , memtable_flush_queue_size(this, "memtable_flush_queue_size", value_status::Unused, 4,
        "The number of full memtables to allow pending flush (memtables waiting for a write thread). At a minimum, set to the maximum number of indexes created on a single table.\n"
        "Related information: Flushing data from the memtable")
    , memtable_flush_writers(this, "memtable_flush_writers", value_status::Invalid, 1,
        "Sets the number of memtable flush writer threads. These threads are blocked by disk I/O, and each one holds a memtable in memory while blocked. If you have a large Java heap size and many data directories, you can increase the value for better flush performance.")
    , memtable_heap_space_in_mb(this, "memtable_heap_space_in_mb", value_status::Unused, 0,
        "Total permitted memory to use for memtables. Triggers a flush based on memtable_cleanup_threshold. Cassandra stops accepting writes when the limit is exceeded until a flush completes. If unset, sets to default.")
    , memtable_offheap_space_in_mb(this, "memtable_offheap_space_in_mb", value_status::Unused, 0,
        "See memtable_heap_space_in_mb")
    /* Cache and index settings */
    , column_index_size_in_kb(this, "column_index_size_in_kb", value_status::Used, 64,
        "Granularity of the index of rows within a partition. For huge rows, decrease this setting to improve seek time. If you use key cache, be careful not to make this setting too large because key cache will be overwhelmed. If you're unsure of the size of the rows, it's best to use the default setting.")
    , index_summary_capacity_in_mb(this, "index_summary_capacity_in_mb", value_status::Unused, 0,
        "Fixed memory pool size in MB for SSTable index summaries. If the memory usage of all index summaries exceeds this limit, any SSTables with low read rates shrink their index summaries to meet this limit. This is a best-effort process. In extreme conditions, Cassandra may need to use more than this amount of memory.")
    , index_summary_resize_interval_in_minutes(this, "index_summary_resize_interval_in_minutes", value_status::Unused, 60,
        "How frequently index summaries should be re-sampled. This is done periodically to redistribute memory from the fixed-size pool to SSTables proportional their recent read rates. To disable, set to -1. This leaves existing index summaries at their current sampling level.")
    , reduce_cache_capacity_to(this, "reduce_cache_capacity_to", value_status::Invalid, .6,
        "Sets the size percentage to which maximum cache capacity is reduced when Java heap usage reaches the threshold defined by reduce_cache_sizes_at. Together with flush_largest_memtables_at, these properties constitute an emergency measure for preventing sudden out-of-memory (OOM) errors.")
    , reduce_cache_sizes_at(this, "reduce_cache_sizes_at", value_status::Invalid, .85,
        "When Java heap usage (after a full concurrent mark sweep (CMS) garbage collection) exceeds this percentage, Cassandra reduces the cache capacity to the fraction of the current size as specified by reduce_cache_capacity_to. To disable, set the value to 1.0.")
    /* Disks settings */
    , stream_throughput_outbound_megabits_per_sec(this, "stream_throughput_outbound_megabits_per_sec", value_status::Unused, 400,
        "Throttles all outbound streaming file transfers on a node to the specified throughput. Cassandra does mostly sequential I/O when streaming data during bootstrap or repair, which can lead to saturating the network connection and degrading client (RPC) performance.")
    , inter_dc_stream_throughput_outbound_megabits_per_sec(this, "inter_dc_stream_throughput_outbound_megabits_per_sec", value_status::Unused, 0,
        "Throttles all streaming file transfer between the data centers. This setting allows throttles streaming throughput betweens data centers in addition to throttling all network stream traffic as configured with stream_throughput_outbound_megabits_per_sec.")
    , trickle_fsync(this, "trickle_fsync", value_status::Unused, false,
        "When doing sequential writing, enabling this option tells fsync to force the operating system to flush the dirty buffers at a set interval trickle_fsync_interval_in_kb. Enable this parameter to avoid sudden dirty buffer flushing from impacting read latencies. Recommended to use on SSDs, but not on HDDs.")
    , trickle_fsync_interval_in_kb(this, "trickle_fsync_interval_in_kb", value_status::Unused, 10240,
        "Sets the size of the fsync in kilobytes.")
    /* Advanced properties */
    /* Properties for advanced users or properties that are less commonly used. */
    /* Advanced initialization properties */
    , auto_bootstrap(this, "auto_bootstrap", value_status::Used, true,
        "This setting has been removed from default configuration. It makes new (non-seed) nodes automatically migrate the right data to themselves. Do not set this to false unless you really know what you are doing.\n"
        "Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers).")
    , batch_size_warn_threshold_in_kb(this, "batch_size_warn_threshold_in_kb", value_status::Used, 128,
        "Log WARN on any batch size exceeding this value in kilobytes. Caution should be taken on increasing the size of this threshold as it can lead to node instability.")
    , batch_size_fail_threshold_in_kb(this, "batch_size_fail_threshold_in_kb", value_status::Used, 1024,
        "Fail any multiple-partition batch exceeding this value. 1 MiB (8x warn threshold) by default.")
    , broadcast_address(this, "broadcast_address", value_status::Used, {/* listen_address */},
        "The IP address a node tells other nodes in the cluster to contact it by. It allows public and private address to be different. For example, use the broadcast_address parameter in topologies where not all nodes have access to other nodes by their private IP addresses.\n"
        "If your Scylla cluster is deployed across multiple Amazon EC2 regions and you use the EC2MultiRegionSnitch , set the broadcast_address to public IP address of the node and the listen_address to the private IP.")
    , listen_on_broadcast_address(this, "listen_on_broadcast_address", value_status::Used, false, "When using multiple physical network interfaces, set this to true to listen on broadcast_address in addition to the listen_address, allowing nodes to communicate in both interfaces.  Ignore this property if the network configuration automatically routes between the public and private networks such as EC2."
    )
    , initial_token(this, "initial_token", value_status::Used, {/* N/A */},
        "Used in the single-node-per-token architecture, where a node owns exactly one contiguous range in the ring space. Setting this property overrides num_tokens.\n"
        "If you not using vnodes or have num_tokens set it to 1 or unspecified (#num_tokens), you should always specify this parameter when setting up a production cluster for the first time and when adding capacity. For more information, see this parameter in the Cassandra 1.1 Node and Cluster Configuration documentation.\n"
        "This parameter can be used with num_tokens (vnodes ) in special cases such as Restoring from a snapshot.")
    , num_tokens(this, "num_tokens", value_status::Used, 1,
        "Defines the number of tokens randomly assigned to this node on the ring when using virtual nodes (vnodes). The more tokens, relative to other nodes, the larger the proportion of data that the node stores. Generally all nodes should have the same number of tokens assuming equal hardware capability. The recommended value is 256. If unspecified (#num_tokens), Scylla uses 1 (equivalent to #num_tokens : 1) for legacy compatibility and uses the initial_token setting.\n"
        "If not using vnodes, comment #num_tokens : 256 or set num_tokens : 1 and use initial_token. If you already have an existing cluster with one token per node and wish to migrate to vnodes, see Enabling virtual nodes on an existing production cluster.\n"
        "Note: If using DataStax Enterprise, the default setting of this property depends on the type of node and type of install.")
    , partitioner(this, "partitioner", value_status::Used, "org.apache.cassandra.dht.Murmur3Partitioner",
        "Distributes rows (by partition key) across all nodes in the cluster. At the moment, only Murmur3Partitioner is supported. For new clusters use the default partitioner.\n"
        "\n"
        "Related information: Partitioners"
        , {"org.apache.cassandra.dht.Murmur3Partitioner"})
    , storage_port(this, "storage_port", value_status::Used, 7000,
        "The port for inter-node communication.")
    /* Advanced automatic backup setting */
    , auto_snapshot(this, "auto_snapshot", value_status::Used, true,
        "Enable or disable whether a snapshot is taken of the data before keyspace truncation or dropping of tables. To prevent data loss, using the default setting is strongly advised. If you set to false, you will lose data on truncation or drop.")
    /* Key caches and global row properties */
    /* When creating or modifying tables, you enable or disable the key cache (partition key cache) or row cache for that table by setting the caching parameter. Other row and key cache tuning and configuration options are set at the global (node) level. Cassandra uses these settings to automatically distribute memory for each table on the node based on the overall workload and specific table usage. You can also configure the save periods for these caches globally. */
    /* Related information: Configuring caches */
    , key_cache_keys_to_save(this, "key_cache_keys_to_save", value_status::Unused, 0,
        "Number of keys from the key cache to save. (0: all)")
    , key_cache_save_period(this, "key_cache_save_period", value_status::Unused, 14400,
        "Duration in seconds that keys are saved in cache. Caches are saved to saved_caches_directory. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O.")
    , key_cache_size_in_mb(this, "key_cache_size_in_mb", value_status::Unused, 100,
        "A global cache setting for tables. It is the maximum size of the key cache in memory. To disable set to 0.\n"
        "Related information: nodetool setcachecapacity.")
    , row_cache_keys_to_save(this, "row_cache_keys_to_save", value_status::Unused, 0,
        "Number of keys from the row cache to save.")
    , row_cache_size_in_mb(this, "row_cache_size_in_mb", value_status::Unused, 0,
        "Maximum size of the row cache in memory. Row cache can save more time than key_cache_size_in_mb, but is space-intensive because it contains the entire row. Use the row cache only for hot rows or static rows. If you reduce the size, you may not get you hottest keys loaded on start up.")
    , row_cache_save_period(this, "row_cache_save_period", value_status::Unused, 0,
        "Duration in seconds that rows are saved in cache. Caches are saved to saved_caches_directory.")
    , memory_allocator(this, "memory_allocator", value_status::Invalid, "NativeAllocator",
        "The off-heap memory allocator. In addition to caches, this property affects storage engine meta data. Supported values:\n"
        "\tNativeAllocator\n"
        "\tJEMallocAllocator\n"
        "\n"
        "Experiments show that jemalloc saves some memory compared to the native allocator because it is more fragmentation resistant. To use, install jemalloc as a library and modify cassandra-env.sh (instructions in file).")
    /* Counter caches properties */
    /* Counter cache helps to reduce counter locks' contention for hot counter cells. In case of RF = 1 a counter cache hit will cause Cassandra to skip the read before write entirely. With RF > 1 a counter cache hit will still help to reduce the duration of the lock hold, helping with hot counter cell updates, but will not allow skipping the read entirely. Only the local (clock, count) tuple of a counter cell is kept in memory, not the whole counter, so it's relatively cheap. */
    /* Note: Reducing the size counter cache may result in not getting the hottest keys loaded on start-up. */
    , counter_cache_size_in_mb(this, "counter_cache_size_in_mb", value_status::Unused, 0,
        "When no value is specified a minimum of 2.5% of Heap or 50MB. If you perform counter deletes and rely on low gc_grace_seconds, you should disable the counter cache. To disable, set to 0")
    , counter_cache_save_period(this, "counter_cache_save_period", value_status::Unused, 7200,
        "Duration after which Cassandra should save the counter cache (keys only). Caches are saved to saved_caches_directory.")
    , counter_cache_keys_to_save(this, "counter_cache_keys_to_save", value_status::Unused, 0,
        "Number of keys from the counter cache to save. When disabled all keys are saved.")
    /* Tombstone settings */
    /* When executing a scan, within or across a partition, tombstones must be kept in memory to allow returning them to the coordinator. The coordinator uses them to ensure other replicas know about the deleted rows. Workloads that generate numerous tombstones may cause performance problems and exhaust the server heap. See Cassandra anti-patterns: Queues and queue-like datasets. Adjust these thresholds only if you understand the impact and want to scan more tombstones. Additionally, you can adjust these thresholds at runtime using the StorageServiceMBean. */
    /* Related information: Cassandra anti-patterns: Queues and queue-like datasets */
    , tombstone_warn_threshold(this, "tombstone_warn_threshold", value_status::Unused, 1000,
        "The maximum number of tombstones a query can scan before warning.")
    , tombstone_failure_threshold(this, "tombstone_failure_threshold", value_status::Unused, 100000,
        "The maximum number of tombstones a query can scan before aborting.")
    /* Network timeout settings */
    , range_request_timeout_in_ms(this, "range_request_timeout_in_ms", value_status::Used, 10000,
        "The time in milliseconds that the coordinator waits for sequential or index scans to complete.")
    , read_request_timeout_in_ms(this, "read_request_timeout_in_ms", value_status::Used, 5000,
        "The time that the coordinator waits for read operations to complete")
    , counter_write_request_timeout_in_ms(this, "counter_write_request_timeout_in_ms", value_status::Used, 5000,
        "The time that the coordinator waits for counter writes to complete.")
    , cas_contention_timeout_in_ms(this, "cas_contention_timeout_in_ms", value_status::Used, 1000,
        "The time that the coordinator continues to retry a CAS (compare and set) operation that contends with other proposals for the same row.")
    , truncate_request_timeout_in_ms(this, "truncate_request_timeout_in_ms", value_status::Used, 60000,
        "The time that the coordinator waits for truncates (remove all data from a table) to complete. The long default value allows for a snapshot to be taken before removing the data. If auto_snapshot is disabled (not recommended), you can reduce this time.")
    , write_request_timeout_in_ms(this, "write_request_timeout_in_ms", value_status::Used, 2000,
        "The time in milliseconds that the coordinator waits for write operations to complete.\n"
        "Related information: About hinted handoff writes")
    , request_timeout_in_ms(this, "request_timeout_in_ms", value_status::Used, 10000,
        "The default timeout for other, miscellaneous operations.\n"
        "Related information: About hinted handoff writes")
    /* Inter-node settings */
    , cross_node_timeout(this, "cross_node_timeout", value_status::Unused, false,
        "Enable or disable operation timeout information exchange between nodes (to accurately measure request timeouts). If disabled Cassandra assumes the request was forwarded to the replica instantly by the coordinator.\n"
        "CAUTION:\n"
        "Before enabling this property make sure NTP (network time protocol) is installed and the times are synchronized between the nodes.")
    , internode_send_buff_size_in_bytes(this, "internode_send_buff_size_in_bytes", value_status::Unused, 0,
        "Sets the sending socket buffer size in bytes for inter-node calls.\n"
        "When setting this parameter and internode_recv_buff_size_in_bytes, the buffer size is limited by net.core.wmem_max. When unset, buffer size is defined by net.ipv4.tcp_wmem. See man tcp and:\n"
        "\n"
        "\t/proc/sys/net/core/wmem_max\n"
        "\t/proc/sys/net/core/rmem_max\n"
        "\t/proc/sys/net/ipv4/tcp_wmem\n"
        "\t/proc/sys/net/ipv4/tcp_wmem\n")
    , internode_recv_buff_size_in_bytes(this, "internode_recv_buff_size_in_bytes", value_status::Unused, 0,
        "Sets the receiving socket buffer size in bytes for inter-node calls.")
    , internode_compression(this, "internode_compression", value_status::Used, "none",
        "Controls whether traffic between nodes is compressed. The valid values are:\n"
        "\n"
        "\tall: All traffic is compressed.\n"
        "\tdc : Traffic between data centers is compressed.\n"
        "\tnone : No compression.")
    , inter_dc_tcp_nodelay(this, "inter_dc_tcp_nodelay", value_status::Used, false,
        "Enable or disable tcp_nodelay for inter-data center communication. When disabled larger, but fewer, network packets are sent. This reduces overhead from the TCP protocol itself. However, if cross data-center responses are blocked, it will increase latency.")
    , streaming_socket_timeout_in_ms(this, "streaming_socket_timeout_in_ms", value_status::Unused, 0,
        "Enable or disable socket timeout for streaming operations. When a timeout occurs during streaming, streaming is retried from the start of the current file. Avoid setting this value too low, as it can result in a significant amount of data re-streaming.")
    /* Native transport (CQL Binary Protocol) */
    , start_native_transport(this, "start_native_transport", value_status::Used, true,
        "Enable or disable the native transport server. Uses the same address as the rpc_address, but the port is different from the rpc_port. See native_transport_port.")
    , native_transport_port(this, "native_transport_port", "cql_port", value_status::Used, 9042,
        "Port on which the CQL native transport listens for clients.")
    , native_transport_port_ssl(this, "native_transport_port_ssl", value_status::Used, 9142,
        "Port on which the CQL TLS native transport listens for clients."
        "Enabling client encryption and keeping native_transport_port_ssl disabled will use encryption"
        "for native_transport_port. Setting native_transport_port_ssl to a different value"
        "from native_transport_port will use encryption for native_transport_port_ssl while"
        "keeping native_transport_port unencrypted")
    , native_shard_aware_transport_port(this, "native_shard_aware_transport_port", value_status::Used, 19042,
        "Like native_transport_port, but clients-side port number (modulo smp) is used to route the connection to the specific shard.")
    , native_shard_aware_transport_port_ssl(this, "native_shard_aware_transport_port_ssl", value_status::Used, 19142,
        "Like native_transport_port_ssl, but clients-side port number (modulo smp) is used to route the connection to the specific shard.")
    , native_transport_max_threads(this, "native_transport_max_threads", value_status::Invalid, 128,
        "The maximum number of thread handling requests. The meaning is the same as rpc_max_threads.\n"
        "Default is different (128 versus unlimited).\n"
        "No corresponding native_transport_min_threads.\n"
        "Idle threads are stopped after 30 seconds.\n")
    , native_transport_max_frame_size_in_mb(this, "native_transport_max_frame_size_in_mb", value_status::Unused, 256,
        "The maximum size of allowed frame. Frame (requests) larger than this are rejected as invalid.")
    /* RPC (remote procedure call) settings */
    /* Settings for configuring and tuning client connections. */
    , broadcast_rpc_address(this, "broadcast_rpc_address", value_status::Used, {/* unset */},
        "RPC address to broadcast to drivers and other Scylla nodes. This cannot be set to 0.0.0.0. If blank, it is set to the value of the rpc_address or rpc_interface. If rpc_address or rpc_interfaceis set to 0.0.0.0, this property must be set.\n")
    , rpc_port(this, "rpc_port", "thrift_port", value_status::Used, 9160,
        "Thrift port for client connections.")
    , start_rpc(this, "start_rpc", value_status::Used, false,
        "Starts the Thrift RPC server")
    , rpc_keepalive(this, "rpc_keepalive", value_status::Used, true,
        "Enable or disable keepalive on client connections (RPC or native).")
    , rpc_max_threads(this, "rpc_max_threads", value_status::Invalid, 0,
        "Regardless of your choice of RPC server (rpc_server_type), the number of maximum requests in the RPC thread pool dictates how many concurrent requests are possible. However, if you are using the parameter sync in the rpc_server_type, it also dictates the number of clients that can be connected. For a large number of client connections, this could cause excessive memory usage for the thread stack. Connection pooling on the client side is highly recommended. Setting a maximum thread pool size acts as a safeguard against misbehaved clients. If the maximum is reached, Cassandra blocks additional connections until a client disconnects.")
    , rpc_min_threads(this, "rpc_min_threads", value_status::Invalid, 16,
        "Sets the minimum thread pool size for remote procedure calls.")
    , rpc_recv_buff_size_in_bytes(this, "rpc_recv_buff_size_in_bytes", value_status::Unused, 0,
        "Sets the receiving socket buffer size for remote procedure calls.")
    , rpc_send_buff_size_in_bytes(this, "rpc_send_buff_size_in_bytes", value_status::Unused, 0,
        "Sets the sending socket buffer size in bytes for remote procedure calls.")
    , rpc_server_type(this, "rpc_server_type", value_status::Unused, "sync",
        "Cassandra provides three options for the RPC server. On Windows, sync is about 30% slower than hsha. On Linux, sync and hsha performance is about the same, but hsha uses less memory.\n"
        "\n"
        "\tsync    (Default One thread per Thrift connection.) For a very large number of clients, memory is the limiting factor. On a 64-bit JVM, 180KB is the minimum stack size per thread and corresponds to your use of virtual memory. Physical memory may be limited depending on use of stack space.\n"
        "\thsh      Half synchronous, half asynchronous. All Thrift clients are handled asynchronously using a small number of threads that does not vary with the number of clients and thus scales well to many clients. The RPC requests are synchronous (one thread per active request).\n"
        "\t         Note: When selecting this option, you must change the default value (unlimited) of rpc_max_threads.\n"
        "\tYour own RPC server: You must provide a fully-qualified class name of an o.a.c.t.TServerFactory that can create a server instance.")
    , cache_hit_rate_read_balancing(this, "cache_hit_rate_read_balancing", value_status::Used, true,
        "This boolean controls whether the replicas for read query will be choosen based on cache hit ratio")
    /* Advanced fault detection settings */
    /* Settings to handle poorly performing or failing nodes. */
    , dynamic_snitch_badness_threshold(this, "dynamic_snitch_badness_threshold", value_status::Unused, 0,
        "Sets the performance threshold for dynamically routing requests away from a poorly performing node. A value of 0.2 means Cassandra continues to prefer the static snitch values until the node response time is 20% worse than the best performing node. Until the threshold is reached, incoming client requests are statically routed to the closest replica (as determined by the snitch). Having requests consistently routed to a given replica can help keep a working set of data hot when read repair is less than 1.")
    , dynamic_snitch_reset_interval_in_ms(this, "dynamic_snitch_reset_interval_in_ms", value_status::Unused, 60000,
        "Time interval in milliseconds to reset all node scores, which allows a bad node to recover.")
    , dynamic_snitch_update_interval_in_ms(this, "dynamic_snitch_update_interval_in_ms", value_status::Unused, 100,
        "The time interval for how often the snitch calculates node scores. Because score calculation is CPU intensive, be careful when reducing this interval.")
    , hinted_handoff_enabled(this, "hinted_handoff_enabled", value_status::Used, db::config::hinted_handoff_enabled_type(db::config::hinted_handoff_enabled_type::enabled_for_all_tag()),
        "Enable or disable hinted handoff. To enable per data center, add data center list. For example: hinted_handoff_enabled: DC1,DC2. A hint indicates that the write needs to be replayed to an unavailable node. "
        "Related information: About hinted handoff writes")
    , hinted_handoff_throttle_in_kb(this, "hinted_handoff_throttle_in_kb", value_status::Unused, 1024,
        "Maximum throttle per delivery thread in kilobytes per second. This rate reduces proportionally to the number of nodes in the cluster. For example, if there are two nodes in the cluster, each delivery thread will use the maximum rate. If there are three, each node will throttle to half of the maximum, since the two nodes are expected to deliver hints simultaneously.")
    , max_hint_window_in_ms(this, "max_hint_window_in_ms", value_status::Used, 10800000,
        "Maximum amount of time that hints are generates hints for an unresponsive node. After this interval, new hints are no longer generated until the node is back up and responsive. If the node goes down again, a new interval begins. This setting can prevent a sudden demand for resources when a node is brought back online and the rest of the cluster attempts to replay a large volume of hinted writes.\n"
        "Related information: Failure detection and recovery")
    , max_hints_delivery_threads(this, "max_hints_delivery_threads", value_status::Invalid, 2,
        "Number of threads with which to deliver hints. In multiple data-center deployments, consider increasing this number because cross data-center handoff is generally slower.")
    , batchlog_replay_throttle_in_kb(this, "batchlog_replay_throttle_in_kb", value_status::Unused, 1024,
        "Total maximum throttle. Throttling is reduced proportionally to the number of nodes in the cluster.")
    /* Request scheduler properties */
    /* Settings to handle incoming client requests according to a defined policy. If you need to use these properties, your nodes are overloaded and dropping requests. It is recommended that you add more nodes and not try to prioritize requests. */
    , request_scheduler(this, "request_scheduler", value_status::Unused, "org.apache.cassandra.scheduler.NoScheduler",
        "Defines a scheduler to handle incoming client requests according to a defined policy. This scheduler is useful for throttling client requests in single clusters containing multiple keyspaces. This parameter is specifically for requests from the client and does not affect inter-node communication. Valid values are:\n"
        "\n"
        "\torg.apache.cassandra.scheduler.NoScheduler   No scheduling takes place.\n"
        "\torg.apache.cassandra.scheduler.RoundRobinScheduler   Round robin of client requests to a node with a separate queue for each request_scheduler_id property.\n"
        "\tA Java class that implements the RequestScheduler interface."
        , {"org.apache.cassandra.scheduler.NoScheduler", "org.apache.cassandra.scheduler.RoundRobinScheduler"})
    , request_scheduler_id(this, "request_scheduler_id", value_status::Unused, {/* keyspace */},
        "An identifier on which to perform request scheduling. Currently the only valid value is keyspace. See weights.")
    , request_scheduler_options(this, "request_scheduler_options", value_status::Unused, {/* disabled */},
        "Contains a list of properties that define configuration options for request_scheduler:\n"
        "\n"
        "\tthrottle_limit: The number of in-flight requests per client. Requests beyond this limit are queued up until running requests complete. Recommended value is ((concurrent_reads + concurrent_writes) × 2)\n"
        "\tdefault_weight: (Default: 1 **)  How many requests are handled during each turn of the RoundRobin.\n"
        "\tweights: (Default: Keyspace: 1)  Takes a list of keyspaces. It sets how many requests are handled during each turn of the RoundRobin, based on the request_scheduler_id.")
    /* Thrift interface properties */
    /* Legacy API for older clients. CQL is a simpler and better API for Scylla. */
    , thrift_framed_transport_size_in_mb(this, "thrift_framed_transport_size_in_mb", value_status::Unused, 15,
        "Frame size (maximum field length) for Thrift. The frame is the row or part of the row the application is inserting.")
    , thrift_max_message_length_in_mb(this, "thrift_max_message_length_in_mb", value_status::Used, 16,
        "The maximum length of a Thrift message in megabytes, including all fields and internal Thrift overhead (1 byte of overhead for each frame). Message length is usually used in conjunction with batches. A frame length greater than or equal to 24 accommodates a batch with four inserts, each of which is 24 bytes. The required message length is greater than or equal to 24+24+24+24+4 (number of frames).")
    /* Security properties */
    /* Server and client security settings. */
    , authenticator(this, "authenticator", value_status::Used, "org.apache.cassandra.auth.AllowAllAuthenticator",
        "The authentication backend, used to identify users. The available authenticators are:\n"
        "\n"
        "\torg.apache.cassandra.auth.AllowAllAuthenticator : Disables authentication; no checks are performed.\n"
        "\torg.apache.cassandra.auth.PasswordAuthenticator : Authenticates users with user names and hashed passwords stored in the system_auth.credentials table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.\n"
        "\tcom.scylladb.auth.TransitionalAuthenticator : Wraps around the PasswordAuthenticator, logging them in if username/password pair provided is correct and treating them as anonymous users otherwise.\n"
        "Related information: Internal authentication"
        , {"AllowAllAuthenticator", "PasswordAuthenticator", "org.apache.cassandra.auth.PasswordAuthenticator", "org.apache.cassandra.auth.AllowAllAuthenticator", "com.scylladb.auth.TransitionalAuthenticator"})
    , internode_authenticator(this, "internode_authenticator", value_status::Unused, "enabled",
        "Internode authentication backend. It implements org.apache.cassandra.auth.AllowAllInternodeAuthenticator to allows or disallow connections from peer nodes.")
    , authorizer(this, "authorizer", value_status::Used, "org.apache.cassandra.auth.AllowAllAuthorizer",
        "The authorization backend. It implements IAuthenticator, which limits access and provides permissions. The available authorizers are:\n"
        "\n"
        "\tAllowAllAuthorizer : Disables authorization; allows any action to any user.\n"
        "\tCassandraAuthorizer : Stores permissions in system_auth.permissions table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.\n"
        "\tcom.scylladb.auth.TransitionalAuthorizer : Wraps around the CassandraAuthorizer, which is used to authorize permission management. Other actions are allowed for all users.\n"
        "Related information: Object permissions"
        , {"AllowAllAuthorizer", "CassandraAuthorizer", "org.apache.cassandra.auth.AllowAllAuthorizer", "org.apache.cassandra.auth.CassandraAuthorizer", "com.scylladb.auth.TransitionalAuthorizer"})
    , role_manager(this, "role_manager", value_status::Used, "org.apache.cassandra.auth.CassandraRoleManager",
        "The role-management backend, used to maintain grantts and memberships between roles.\n"
        "The available role-managers are:\n"
        "\tCassandraRoleManager : Stores role data in the system_auth keyspace.")
    , permissions_validity_in_ms(this, "permissions_validity_in_ms", value_status::Used, 10000,
        "How long permissions in cache remain valid. Depending on the authorizer, such as CassandraAuthorizer, fetching permissions can be resource intensive. Permissions caching is disabled when this property is set to 0 or when AllowAllAuthorizer is used. The cached value is considered valid as long as both its value is not older than the permissions_validity_in_ms "
        "and the cached value has been read at least once during the permissions_validity_in_ms time frame. If any of these two conditions doesn't hold the cached value is going to be evicted from the cache.\n"
        "Related information: Object permissions")
    , permissions_update_interval_in_ms(this, "permissions_update_interval_in_ms", value_status::Used, 2000,
        "Refresh interval for permissions cache (if enabled). After this interval, cache entries become eligible for refresh. An async reload is scheduled every permissions_update_interval_in_ms time period and the old value is returned until it completes. If permissions_validity_in_ms has a non-zero value, then this property must also have a non-zero value. It's recommended to set this value to be at least 3 times smaller than the permissions_validity_in_ms.")
    , permissions_cache_max_entries(this, "permissions_cache_max_entries", value_status::Used, 1000,
        "Maximum cached permission entries. Must have a non-zero value if permissions caching is enabled (see a permissions_validity_in_ms description).")
    , server_encryption_options(this, "server_encryption_options", value_status::Used, {/*none*/},
        "Enable or disable inter-node encryption. You must also generate keys and provide the appropriate key and trust store locations and passwords. The available options are:\n"
        "\n"
        "internode_encryption : (Default: none ) Enable or disable encryption of inter-node communication using the TLS_RSA_WITH_AES_128_CBC_SHA cipher suite for authentication, key exchange, and encryption of data transfers. The available inter-node options are:\n"
        "\tall : Encrypt all inter-node communications.\n"
        "\tnone : No encryption.\n"
        "\tdc : Encrypt the traffic between the data centers (server only).\n"
        "\track : Encrypt the traffic between the racks(server only).\n"
        "certificate : (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the internode communication.\n"
        "keyfile : (Default: conf/scylla.key) PEM Key file associated with certificate.\n"
        "truststore : (Default: <system truststore> ) Location of the truststore containing the trusted certificate for authenticating remote servers.\n"
        "\n"
        "The advanced settings are:\n"
        "\n"
        "\tpriority_string : GnuTLS priority string controlling TLS algorithms used/allowed.\n"
        "\trequire_client_auth : (Default: false ) Enables or disables certificate authentication.\n"
        "Related information: Node-to-node encryption")
    , client_encryption_options(this, "client_encryption_options", value_status::Used, {/*none*/},
        "Enable or disable client-to-node encryption. You must also generate keys and provide the appropriate key and certificate. The available options are:\n"
        "\n"
        "\tenabled : (Default: false ) To enable, set to true.\n"
        "\tcertificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.\n"
        "\tkeyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.\n"
        "truststore : (Default: <system truststore> ) Location of the truststore containing the trusted certificate for authenticating remote servers.\n"
        "\n"
        "The advanced settings are:\n"
        "\n"
        "\tpriority_string : GnuTLS priority string controlling TLS algorithms used/allowed.\n"
        "\trequire_client_auth : (Default: false ) Enables or disables certificate authentication.\n"
        "Related information: Client-to-node encryption")
    , alternator_encryption_options(this, "alternator_encryption_options", value_status::Used, {/*none*/},
        "When Alternator via HTTPS is enabled with alternator_https_port, where to take the key and certificate. The available options are:\n"
        "\n"
        "\tcertificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.\n"
        "\tkeyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.\n"
        "\n"
        "The advanced settings are:\n"
        "\n"
        "\tpriority_string : GnuTLS priority string controlling TLS algorithms used/allowed.")
    , ssl_storage_port(this, "ssl_storage_port", value_status::Used, 7001,
        "The SSL port for encrypted communication. Unused unless enabled in encryption_options.")
    , enable_in_memory_data_store(this, "enable_in_memory_data_store", value_status::Used, false, "Enable in memory mode (system tables are always persisted)")
    , enable_cache(this, "enable_cache", value_status::Used, true, "Enable cache")
    , enable_commitlog(this, "enable_commitlog", value_status::Used, true, "Enable commitlog")
    , volatile_system_keyspace_for_testing(this, "volatile_system_keyspace_for_testing", value_status::Used, false, "Don't persist system keyspace - testing only!")
    , api_port(this, "api_port", value_status::Used, 10000, "Http Rest API port")
    , api_address(this, "api_address", value_status::Used, "", "Http Rest API address")
    , api_ui_dir(this, "api_ui_dir", value_status::Used, "swagger-ui/dist/", "The directory location of the API GUI")
    , api_doc_dir(this, "api_doc_dir", value_status::Used, "api/api-doc/", "The API definition file directory")
    , load_balance(this, "load_balance", value_status::Unused, "none", "CQL request load balancing: 'none' or round-robin'")
    , consistent_rangemovement(this, "consistent_rangemovement", value_status::Used, true, "When set to true, range movements will be consistent. It means: 1) it will refuse to bootstrap a new node if other bootstrapping/leaving/moving nodes detected. 2) data will be streamed to a new node only from the node which is no longer responsible for the token range. Same as -Dcassandra.consistent.rangemovement in cassandra")
    , join_ring(this, "join_ring", value_status::Unused, true, "When set to true, a node will join the token ring. When set to false, a node will not join the token ring. User can use nodetool join to initiate ring joinging later. Same as -Dcassandra.join_ring in cassandra.")
    , load_ring_state(this, "load_ring_state", value_status::Used, true, "When set to true, load tokens and host_ids previously saved. Same as -Dcassandra.load_ring_state in cassandra.")
    , replace_node(this, "replace_node", value_status::Used, "", "The UUID of the node to replace. Same as -Dcassandra.replace_node in cssandra.")
    , replace_token(this, "replace_token", value_status::Used, "", "The tokens of the node to replace. Same as -Dcassandra.replace_token in cassandra.")
    , replace_address(this, "replace_address", value_status::Used, "", "The listen_address or broadcast_address of the dead node to replace. Same as -Dcassandra.replace_address.")
    , replace_address_first_boot(this, "replace_address_first_boot", value_status::Used, "", "Like replace_address option, but if the node has been bootstrapped successfully it will be ignored. Same as -Dcassandra.replace_address_first_boot.")
    , override_decommission(this, "override_decommission", value_status::Used, false, "Set true to force a decommissioned node to join the cluster")
    , enable_repair_based_node_ops(this, "enable_repair_based_node_ops", liveness::LiveUpdate, value_status::Used, false, "Set true to use enable repair based node operations instead of streaming based")
    , wait_for_hint_replay_before_repair(this, "wait_for_hint_replay_before_repair", liveness::LiveUpdate, value_status::Used, true, "If set to true, the cluster will first wait until the cluster sends its hints towards the nodes participating in repair before proceeding with the repair itself. This reduces the amount of data needed to be transferred during repair.")
    , ring_delay_ms(this, "ring_delay_ms", value_status::Used, 30 * 1000, "Time a node waits to hear from other nodes before joining the ring in milliseconds. Same as -Dcassandra.ring_delay_ms in cassandra.")
    , shadow_round_ms(this, "shadow_round_ms", value_status::Used, 300 * 1000, "The maximum gossip shadow round time. Can be used to reduce the gossip feature check time during node boot up.")
    , fd_max_interval_ms(this, "fd_max_interval_ms", value_status::Used, 2 * 1000, "The maximum failure_detector interval time in milliseconds. Interval larger than the maximum will be ignored. Larger cluster may need to increase the default.")
    , fd_initial_value_ms(this, "fd_initial_value_ms", value_status::Used, 2 * 1000, "The initial failure_detector interval time in milliseconds.")
    , shutdown_announce_in_ms(this, "shutdown_announce_in_ms", value_status::Used, 2 * 1000, "Time a node waits after sending gossip shutdown message in milliseconds. Same as -Dcassandra.shutdown_announce_in_ms in cassandra.")
    , developer_mode(this, "developer_mode", value_status::Used, false, "Relax environment checks. Setting to true can reduce performance and reliability significantly.")
    , skip_wait_for_gossip_to_settle(this, "skip_wait_for_gossip_to_settle", value_status::Used, -1, "An integer to configure the wait for gossip to settle. -1: wait normally, 0: do not wait at all, n: wait for at most n polls. Same as -Dcassandra.skip_wait_for_gossip_to_settle in cassandra.")
    , force_gossip_generation(this, "force_gossip_generation", liveness::LiveUpdate, value_status::Used, -1 , "Force gossip to use the generation number provided by user")
    , experimental(this, "experimental", value_status::Used, false, "Set to true to unlock all experimental features.")
    , experimental_features(this, "experimental_features", value_status::Used, {}, "Unlock experimental features provided as the option arguments (possible values: 'lwt', 'cdc', 'udf'). Can be repeated.")
    , lsa_reclamation_step(this, "lsa_reclamation_step", value_status::Used, 1, "Minimum number of segments to reclaim in a single step")
    , prometheus_port(this, "prometheus_port", value_status::Used, 9180, "Prometheus port, set to zero to disable")
    , prometheus_address(this, "prometheus_address", value_status::Used, "0.0.0.0", "Prometheus listening address")
    , prometheus_prefix(this, "prometheus_prefix", value_status::Used, "scylla", "Set the prefix of the exported Prometheus metrics. Changing this will break Scylla's dashboard compatibility, do not change unless you know what you are doing.")
    , abort_on_lsa_bad_alloc(this, "abort_on_lsa_bad_alloc", value_status::Used, false, "Abort when allocation in LSA region fails")
    , murmur3_partitioner_ignore_msb_bits(this, "murmur3_partitioner_ignore_msb_bits", value_status::Used, 12, "Number of most siginificant token bits to ignore in murmur3 partitioner; increase for very large clusters")
    , virtual_dirty_soft_limit(this, "virtual_dirty_soft_limit", value_status::Used, 0.6, "Soft limit of virtual dirty memory expressed as a portion of the hard limit")
    , sstable_summary_ratio(this, "sstable_summary_ratio", value_status::Used, 0.0005, "Enforces that 1 byte of summary is written for every N (2000 by default) "
        "bytes written to data file. Value must be between 0 and 1.")
    , large_memory_allocation_warning_threshold(this, "large_memory_allocation_warning_threshold", value_status::Used, size_t(1) << 20, "Warn about memory allocations above this size; set to zero to disable")
    , enable_deprecated_partitioners(this, "enable_deprecated_partitioners", value_status::Used, false, "Enable the byteordered and random partitioners. These partitioners are deprecated and will be removed in a future version.")
    , enable_keyspace_column_family_metrics(this, "enable_keyspace_column_family_metrics", value_status::Used, false, "Enable per keyspace and per column family metrics reporting")
    , enable_sstable_data_integrity_check(this, "enable_sstable_data_integrity_check", value_status::Used, false, "Enable interposer which checks for integrity of every sstable write."
        " Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.")
    , enable_sstable_key_validation(this, "enable_sstable_key_validation", value_status::Used, ENABLE_SSTABLE_KEY_VALIDATION, "Enable validation of partition and clustering keys monotonicity"
        " Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.")
    , cpu_scheduler(this, "cpu_scheduler", value_status::Used, true, "Enable cpu scheduling")
    , view_building(this, "view_building", value_status::Used, true, "Enable view building; should only be set to false when the node is experience issues due to view building")
    , enable_sstables_mc_format(this, "enable_sstables_mc_format", value_status::Unused, true, "Enable SSTables 'mc' format to be used as the default file format")
    , enable_sstables_md_format(this, "enable_sstables_md_format", value_status::Used, true, "Enable SSTables 'md' format to be used as the default file format")
    , enable_dangerous_direct_import_of_cassandra_counters(this, "enable_dangerous_direct_import_of_cassandra_counters", value_status::Used, false, "Only turn this option on if you want to import tables from Cassandra containing counters, and you are SURE that no counters in that table were created in a version earlier than Cassandra 2.1."
        " It is not enough to have ever since upgraded to newer versions of Cassandra. If you EVER used a version earlier than 2.1 in the cluster where these SSTables come from, DO NOT TURN ON THIS OPTION! You will corrupt your data. You have been warned.")
    , enable_shard_aware_drivers(this, "enable_shard_aware_drivers", value_status::Used, true, "Enable native transport drivers to use connection-per-shard for better performance")
    , enable_ipv6_dns_lookup(this, "enable_ipv6_dns_lookup", value_status::Used, false, "Use IPv6 address resolution")
    , abort_on_internal_error(this, "abort_on_internal_error", liveness::LiveUpdate, value_status::Used, false, "Abort the server instead of throwing exception when internal invariants are violated")
    , max_partition_key_restrictions_per_query(this, "max_partition_key_restrictions_per_query", liveness::LiveUpdate, value_status::Used, 100,
            "Maximum number of distinct partition keys restrictions per query. This limit places a bound on the size of IN tuples, "
            "especially when multiple partition key columns have IN restrictions. Increasing this value can result in server instability.")
    , max_clustering_key_restrictions_per_query(this, "max_clustering_key_restrictions_per_query", liveness::LiveUpdate, value_status::Used, 100,
            "Maximum number of distinct clustering key restrictions per query. This limit places a bound on the size of IN tuples, "
            "especially when multiple clustering key columns have IN restrictions. Increasing this value can result in server instability.")
    , max_memory_for_unlimited_query_soft_limit(this, "max_memory_for_unlimited_query_soft_limit", liveness::LiveUpdate, value_status::Used, uint64_t(1) << 20,
            "Maximum amount of memory a query, whose memory consumption is not naturally limited, is allowed to consume, e.g. non-paged and reverse queries. "
            "This is the soft limit, there will be a warning logged for queries violating this limit.")
    , max_memory_for_unlimited_query_hard_limit(this, "max_memory_for_unlimited_query_hard_limit", "max_memory_for_unlimited_query", liveness::LiveUpdate, value_status::Used, (uint64_t(100) << 20),
            "Maximum amount of memory a query, whose memory consumption is not naturally limited, is allowed to consume, e.g. non-paged and reverse queries. "
            "This is the hard limit, queries violating this limit will be aborted.")
    , initial_sstable_loading_concurrency(this, "initial_sstable_loading_concurrency", value_status::Used, 4u,
            "Maximum amount of sstables to load in parallel during initialization. A higher number can lead to more memory consumption. You should not need to touch this")
    , enable_3_1_0_compatibility_mode(this, "enable_3_1_0_compatibility_mode", value_status::Used, false,
        "Set to true if the cluster was initially installed from 3.1.0. If it was upgraded from an earlier version,"
        " or installed from a later version, leave this set to false. This adjusts the communication protocol to"
        " work around a bug in Scylla 3.1.0")
    , enable_user_defined_functions(this, "enable_user_defined_functions", value_status::Used, false,  "Enable user defined functions. You must also set experimental-features=udf")
    , user_defined_function_time_limit_ms(this, "user_defined_function_time_limit_ms", value_status::Used, 10, "The time limit for each UDF invocation")
    , user_defined_function_allocation_limit_bytes(this, "user_defined_function_allocation_limit_bytes", value_status::Used, 1024*1024, "How much memory each UDF invocation can allocate")
    , user_defined_function_contiguous_allocation_limit_bytes(this, "user_defined_function_contiguous_allocation_limit_bytes", value_status::Used, 1024*1024, "How much memory each UDF invocation can allocate in one chunk")
    , schema_registry_grace_period(this, "schema_registry_grace_period", value_status::Used, 1,
        "Time period in seconds after which unused schema versions will be evicted from the local schema registry cache. Default is 1 second.")
    , max_concurrent_requests_per_shard(this, "max_concurrent_requests_per_shard",liveness::LiveUpdate, value_status::Used, std::numeric_limits<uint32_t>::max(),
        "Maximum number of concurrent requests a single shard can handle before it starts shedding extra load. By default, no requests will be shed.")
    , cdc_dont_rewrite_streams(this, "cdc_dont_rewrite_streams", value_status::Used, false,
            "Disable rewriting streams from cdc_streams_descriptions to cdc_streams_descriptions_v2. Should not be necessary, but the procedure is expensive and prone to failures; this config option is left as a backdoor in case some user requires manual intervention.")
    , alternator_port(this, "alternator_port", value_status::Used, 0, "Alternator API port")
    , alternator_https_port(this, "alternator_https_port", value_status::Used, 0, "Alternator API HTTPS port")
    , alternator_address(this, "alternator_address", value_status::Used, "0.0.0.0", "Alternator API listening address")
    , alternator_enforce_authorization(this, "alternator_enforce_authorization", value_status::Used, false, "Enforce checking the authorization header for every request in Alternator")
    , alternator_write_isolation(this, "alternator_write_isolation", value_status::Used, "", "Default write isolation policy for Alternator")
    , alternator_streams_time_window_s(this, "alternator_streams_time_window_s", value_status::Used, 10, "CDC query confidence window for alternator streams")
    , alternator_timeout_in_ms(this, "alternator_timeout_in_ms", value_status::Used, 10000,
        "The server-side timeout for completing Alternator API requests.")
    , abort_on_ebadf(this, "abort_on_ebadf", value_status::Used, true, "Abort the server on incorrect file descriptor access. Throws exception when disabled.")
    , redis_port(this, "redis_port", value_status::Used, 0, "Port on which the REDIS transport listens for clients.")
    , redis_ssl_port(this, "redis_ssl_port", value_status::Used, 0, "Port on which the REDIS TLS native transport listens for clients.")
    , redis_read_consistency_level(this, "redis_read_consistency_level", value_status::Used, "LOCAL_QUORUM", "Consistency level for read operations for redis.")
    , redis_write_consistency_level(this, "redis_write_consistency_level", value_status::Used, "LOCAL_QUORUM", "Consistency level for write operations for redis.")
    , redis_database_count(this, "redis_database_count", value_status::Used, 16, "Database count for the redis. You can use the default settings (16).")
    , redis_keyspace_replication_strategy_options(this, "redis_keyspace_replication_strategy", value_status::Used, {}, 
        "Set the replication strategy for the redis keyspace. The setting is used by the first node in the boot phase when the keyspace is not exists to create keyspace for redis.\n"
        "The replication strategy determines how many copies of the data are kept in a given data center. This setting impacts consistency, availability and request speed.\n"
        "Two strategies are available: SimpleStrategy and NetworkTopologyStrategy.\n\n"
        "\tclass: (Default: SimpleStrategy ). Set the replication strategy for redis keyspace.\n"
        "\t'replication_factor':N, (Default: 'replication_factor':1) IFF the class is SimpleStrategy, assign the same replication factor to the entire cluster.\n"
        "\t'datacenter_name':N [,...], (Default: 'dc1:1') IFF the class is NetworkTopologyStrategy, assign replication factors to each data center in a comma separated list.\n"
        "\n"
        "Related information: About replication strategy.")

    , default_log_level(this, "default_log_level", value_status::Used)
    , logger_log_level(this, "logger_log_level", value_status::Used)
    , log_to_stdout(this, "log_to_stdout", value_status::Used)
    , log_to_syslog(this, "log_to_syslog", value_status::Used)
    , _extensions(std::move(exts))
{}

db::config::config()
    : config(std::make_shared<db::extensions>())
{}

db::config::~config()
{}

void db::config::add_cdc_extension() {
    _extensions->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
}

void db::config::setup_directories() {
    maybe_in_workdir(commitlog_directory, "commitlog");
    maybe_in_workdir(data_file_directories, "data");
    maybe_in_workdir(hints_directory, "hints");
    maybe_in_workdir(view_hints_directory, "view_hints");
    maybe_in_workdir(saved_caches_directory, "saved_caches");
}

void db::config::maybe_in_workdir(named_value<sstring>& to, const char* sub) {
    if (!to.is_set()) {
        to(work_directory() + "/" + sub);
    }
}

void db::config::maybe_in_workdir(named_value<string_list>& tos, const char* sub) {
    if (!tos.is_set()) {
        string_list n;
        n.push_back(work_directory() + "/" + sub);
        tos(n);
    }
}

const sstring db::config::default_tls_priority("SECURE128:-VERS-TLS1.0");


namespace db {

std::ostream& operator<<(std::ostream& os, const db::seed_provider_type& s) {
    os << "seed_provider_type{class=" << s.class_name << ", params=" << s.parameters << "}";
    return os;
}

}

namespace utils {

template<>
void config_file::named_value<db::config::seed_provider_type>::add_command_line_option(
                boost::program_options::options_description_easy_init& init) {
    init((hyphenate(name()) + "-class-name").data(),
                    value_ex<sstring>()->notifier(
                                    [this](sstring new_class_name) {
                                        auto old_seed_provider = operator()();
                                        old_seed_provider.class_name = new_class_name;
                                        set(std::move(old_seed_provider), config_source::CommandLine);
                                    }),
                    desc().data());
    init((hyphenate(name()) + "-parameters").data(),
                    value_ex<std::unordered_map<sstring, sstring>>()->notifier(
                                    [this](std::unordered_map<sstring, sstring> new_parameters) {
                                        auto old_seed_provider = operator()();
                                        old_seed_provider.parameters = new_parameters;
                                        set(std::move(old_seed_provider), config_source::CommandLine);
                                    }),
                    desc().data());
}

}

db::fs::path db::config::get_conf_dir() {
    using namespace db::fs;

    path confdir;
    auto* cd = std::getenv("SCYLLA_CONF");
    if (cd != nullptr) {
        confdir = path(cd);
    } else {
        auto* p = std::getenv("SCYLLA_HOME");
        if (p != nullptr) {
            confdir = path(p);
        }
        confdir /= "conf";
    }

    return confdir;
}

db::fs::path db::config::get_conf_sub(db::fs::path sub) {
    return get_conf_dir() / sub;
}

bool db::config::check_experimental(experimental_features_t::feature f) const {
    if (experimental() && f != experimental_features_t::UNUSED && f != experimental_features_t::UNUSED_CDC) {
        return true;
    }
    const auto& optval = experimental_features();
    return find(begin(optval), end(optval), enum_option<experimental_features_t>{f}) != end(optval);
}

namespace bpo = boost::program_options;

logging::settings db::config::logging_settings(const bpo::variables_map& map) const {
    struct convert {
        std::unordered_map<sstring, seastar::log_level> operator()(const seastar::program_options::string_map& map) const {
            std::unordered_map<sstring, seastar::log_level> res;
            for (auto& p : map) {
                res.emplace(p.first, (*this)(p.second));
            };
            return res;
        }
        seastar::log_level operator()(const sstring& s) const {
            return boost::lexical_cast<seastar::log_level>(s);
        }
        bool operator()(bool b) const {
            return b;
        }
    };

    auto value = [&map](auto& v, auto dummy) {
        auto name = utils::hyphenate(v.name());
        const bpo::variable_value& opt = map[name];

        if (opt.defaulted() && v.is_set()) {
            return v();
        }
        using expected = std::decay_t<decltype(dummy)>;

        return convert()(opt.as<expected>());
    };

    return logging::settings{ value(logger_log_level, seastar::program_options::string_map())
        , value(default_log_level, sstring())
        , value(log_to_stdout, bool())
        , value(log_to_syslog, bool())
    };
}

const db::extensions& db::config::extensions() const {
    return *_extensions;
}

std::unordered_map<sstring, db::experimental_features_t::feature> db::experimental_features_t::map() {
    // We decided against using the construct-on-first-use idiom here:
    // https://github.com/scylladb/scylla/pull/5369#discussion_r353614807
    // Lightweight transactions are no longer experimental. Map them
    // to UNUSED switch for a while, then remove altogether.
    // Change Data Capture is no longer experimental. Map it
    // to UNUSED_CDC switch for a while, then remove altogether.
    return {{"lwt", UNUSED}, {"udf", UDF}, {"cdc", UNUSED_CDC}, {"alternator-streams", ALTERNATOR_STREAMS}};
}

std::vector<enum_option<db::experimental_features_t>> db::experimental_features_t::all() {
    return {UDF, ALTERNATOR_STREAMS};
}

template struct utils::config_file::named_value<seastar::log_level>;

namespace utils {

sstring
config_value_as_json(const db::seed_provider_type& v) {
    // We don't support converting this to json yet
    return "seed_provider_type";
}

sstring
config_value_as_json(const log_level& v) {
    // We don't support converting this to json yet; and because the log_level config items
    // aren't part of config_file::value(), it won't be converted to json in REST
    throw std::runtime_error("config_value_as_json(log_level) is not implemented");
}

sstring config_value_as_json(const std::unordered_map<sstring, log_level>& v) {
    // We don't support converting this to json yet; and because the log_level config items
    // aren't part of config_file::value(), it won't be listed
    throw std::runtime_error("config_value_as_json(const std::unordered_map<sstring, log_level>& v) is not implemented");
}

}
