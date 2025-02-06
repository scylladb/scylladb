/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <unordered_map>
#include <sstream>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <boost/any.hpp>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include <fmt/ranges.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/log.hh>
#include <seastar/util/log-cli.hh>
#include <seastar/net/tls.hh>

#include "cdc/cdc_extension.hh"
#include "tombstone_gc_extension.hh"
#include "db/per_partition_rate_limit_extension.hh"
#include "db/tags/extension.hh"
#include "config.hh"
#include "extensions.hh"
#include "utils/log.hh"
#include "service/tablet_allocator_fwd.hh"
#include "utils/config_file_impl.hh"
#include <seastar/core/metrics_api.hh>
#include <seastar/core/relabel_config.hh>
#include <seastar/util/file.hh>

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

// Convert a value that can be printed with fmt::format, or a vector of
// such values, to JSON. An example is enum_option<T>, because enum_option<T>
// has a specialization for fmt::formatter.
template <typename T>
static json::json_return_type
printable_to_json(const T& e) {
    return value_to_json(format("{}", e));
}
template <typename T>
static json::json_return_type
printable_vector_to_json(const std::vector<T>& e) {
    std::vector<sstring> converted;
    converted.reserve(e.size());
    for (const auto& option : e) {
        converted.push_back(format("{}", option));
    }
    return value_to_json(converted);
}

static
json::json_return_type
error_injection_list_to_json(const std::vector<db::config::error_injection_at_startup>& eil) {
    return value_to_json("error_injection_list");
}

template <>
bool
config_from_string(std::string_view value) {
    // boost::lexical_cast doesn't accept true/false, which are our output representations
    // for bools. We want round-tripping, so we need to accept true/false. For backward
    // compatibility, we also accept 1/0. #19791.
    if (value == "true" || value == "1") {
        return true;
    } else if (value == "false" || value == "0") {
        return false;
    } else {
        throw boost::bad_lexical_cast(typeid(std::string_view), typeid(bool));
    }
}

template <>
const config_type& config_type_for<bool>() {
    static config_type ct("bool", value_to_json<bool>);
    return ct;
}

template <>
const config_type& config_type_for<uint16_t>() {
    static config_type ct("integer", value_to_json<uint16_t>);
    return ct;
}

template <>
const config_type& config_type_for<uint32_t>() {
    static config_type ct("integer", value_to_json<uint32_t>);
    return ct;
}

template <>
const config_type& config_type_for<uint64_t>() {
    static config_type ct("integer", value_to_json<uint64_t>);
    return ct;
}

template <>
const config_type& config_type_for<float>() {
    static config_type ct("float", value_to_json<float>);
    return ct;
}

template <>
const config_type& config_type_for<double>() {
    static config_type ct("double", value_to_json<double>);
    return ct;
}

template <>
const config_type& config_type_for<log_level>() {
    static config_type ct("string", log_level_to_json);
    return ct;
}

template <>
const config_type& config_type_for<sstring>() {
    static config_type ct("string", value_to_json<sstring>);
    return ct;
}

template <>
const config_type& config_type_for<std::string>() {
    static config_type ct("string", value_to_json<std::string>);
    return ct;
}

template <>
const config_type& config_type_for<std::vector<sstring>>() {
    static config_type ct("string list", value_to_json<std::vector<sstring>>);
    return ct;
}

template <>
const config_type& config_type_for<std::unordered_map<sstring, std::unordered_map<sstring, sstring>>>() {
    static config_type ct("string map map", value_to_json<std::unordered_map<sstring, std::unordered_map<sstring, sstring>>>);
    return ct;
}

template <>
const config_type& config_type_for<std::unordered_map<sstring, sstring>>() {
    static config_type ct("string map", value_to_json<std::unordered_map<sstring, sstring>>);
    return ct;
}

template <>
const config_type& config_type_for<std::vector<std::unordered_map<sstring, sstring>>>() {
    static config_type ct("string map list", value_to_json<std::vector<std::unordered_map<sstring, sstring>>>);
    return ct;
}

template <>
const config_type& config_type_for<std::unordered_map<sstring, log_level>>() {
    static config_type ct("string map", log_level_map_to_json);
    return ct;
}

template <>
const config_type& config_type_for<int64_t>() {
    static config_type ct("integer", value_to_json<int64_t>);
    return ct;
}

template <>
const config_type& config_type_for<int32_t>() {
    static config_type ct("integer", value_to_json<int32_t>);
    return ct;
}

template <>
const config_type& config_type_for<db::seed_provider_type>() {
    static config_type ct("seed provider", seed_provider_to_json);
    return ct;
}

template <>
const config_type& config_type_for<std::vector<enum_option<db::experimental_features_t>>>() {
    static config_type ct(
        "experimental features", printable_vector_to_json<enum_option<db::experimental_features_t>>);
    return ct;
}

template <>
const config_type& config_type_for<std::vector<enum_option<db::replication_strategy_restriction_t>>>() {
    static config_type ct(
        "replication strategy list", printable_vector_to_json<enum_option<db::replication_strategy_restriction_t>>);
    return ct;
}

template <>
const config_type& config_type_for<enum_option<db::tri_mode_restriction_t>>() {
    static config_type ct(
        "restriction mode", printable_to_json<enum_option<db::tri_mode_restriction_t>>);
    return ct;
}

template <>
const config_type& config_type_for<db::config::hinted_handoff_enabled_type>() {
    static config_type ct("hinted handoff enabled", hinted_handoff_enabled_to_json);
    return ct;
}

template <>
const config_type& config_type_for<std::vector<db::config::error_injection_at_startup>>() {
    static config_type ct("error injection list", error_injection_list_to_json);
    return ct;
}

template <>
const config_type& config_type_for<enum_option<utils::dict_training_loop::when>>() {
    static config_type ct(
        "dictionary training conditions", printable_to_json<enum_option<utils::dict_training_loop::when>>);
    return ct;
}

template <>
const config_type& config_type_for<utils::advanced_rpc_compressor::tracker::algo_config>() {
    static config_type ct(
        "advanced rpc compressor config", printable_vector_to_json<enum_option<compression_algorithm>>);
    return ct;
}

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
        rhs = utils::config_from_string<seastar::log_level>(tmp);
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

template <>
class convert<enum_option<db::replication_strategy_restriction_t>> {
public:
    static bool decode(const Node& node, enum_option<db::replication_strategy_restriction_t>& rhs) {
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

template <>
class convert<enum_option<db::tri_mode_restriction_t>> {
public:
    static bool decode(const Node& node, enum_option<db::tri_mode_restriction_t>& rhs) {
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

template<>
struct convert<db::config::error_injection_at_startup> {
    static bool decode(const Node& node, db::config::error_injection_at_startup& rhs) {
        rhs = db::config::error_injection_at_startup();
        if (node.IsScalar()) {
            rhs.name = node.as<sstring>();
            return true;
        } else if (node.IsMap()) {
            for (auto& n : node) {
                const auto key = n.first.as<sstring>();
                if (key == "name") {
                    rhs.name = n.second.as<sstring>();
                } else if (key == "one_shot") {
                    rhs.one_shot = n.second.as<bool>();
                } else {
                    rhs.parameters.insert({key, n.second.as<sstring>()});
                }
            }
            return !rhs.name.empty();
        } else {
            return false;
        }
    }
};


template <>
class convert<enum_option<utils::dict_training_loop::when>> {
public:
    static bool decode(const Node& node, enum_option<utils::dict_training_loop::when>& rhs) {
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

template <>
class convert<enum_option<utils::compression_algorithm>> {
public:
    static bool decode(const Node& node, enum_option<utils::compression_algorithm>& rhs) {
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

#if defined(DEBUG) || defined(DEVEL)
#define DEVELOPER_MODE_DEFAULT true
#else
#define DEVELOPER_MODE_DEFAULT false
#endif

#define str(x)  #x
#define _mk_init(name, type, deflt, status, desc, ...)  , name(this, str(name), value_status::status, type(deflt), desc)

#if defined(SCYLLA_ENABLE_ERROR_INJECTION)
#define ENABLE_ERROR_INJECTION_OPTIONS true
#else
#define ENABLE_ERROR_INJECTION_OPTIONS false
#endif

static const db::config::value_status error_injection_value_status =
        ENABLE_ERROR_INJECTION_OPTIONS ? db::config::value_status::Used : db::config::value_status::Unused;

static db::tri_mode_restriction_t::mode strict_allow_filtering_default() {
    return db::tri_mode_restriction_t::mode::WARN; // TODO: make it TRUE after Scylla 4.6.
}

static std::vector<sstring> experimental_feature_names() {
    std::vector<sstring> ret;
    for (const auto& f : db::experimental_features_t::map()) {
        if (f.second != db::experimental_features_t::feature::UNUSED) {
            ret.push_back(f.first);
        }
    }
    return ret;
}

// Inconveniently, help strings MUST be a string_view, so they cannot be
// created on-the-fly below with format(). Instead, we need to save the
// help string to a static object, and return a string_view to it:
static std::string_view experimental_features_help_string() {
    static sstring s = seastar::format("Unlock experimental features provided as the "
        "option arguments (possible values: {}). Can be repeated.",
        experimental_feature_names());
    return s;
}

using namespace std::chrono_literals;

db::config::config(std::shared_ptr<db::extensions> exts)
    : utils::config_file()
    /**
    * Annotations used for autogenerating documentation. 
    * @Group: Names the category of subsequent config properties.
    * @GroupDescription: Provides an overview of the group.
    */
    /**
    * @Group Ungrouped properties
    */
    , background_writer_scheduling_quota(this, "background_writer_scheduling_quota", value_status::Deprecated, 1.0,
        "max cpu usage ratio (between 0 and 1) for compaction process. Not intended for setting in normal operations. Setting it to 1 or higher will disable it, recommended operational setting is 0.5.")
    , auto_adjust_flush_quota(this, "auto_adjust_flush_quota", value_status::Deprecated, false,
        "true: auto-adjust memtable shares for flush processes")
    , memtable_flush_static_shares(this, "memtable_flush_static_shares", liveness::LiveUpdate, value_status::Used, 0,
        "If set to higher than 0, ignore the controller's output and set the memtable shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity.")
    , compaction_static_shares(this, "compaction_static_shares", liveness::LiveUpdate, value_status::Used, 0,
        "If set to higher than 0, ignore the controller's output and set the compaction shares statically. Do not set this unless you know what you are doing and suspect a problem in the controller. This option will be retired when the controller reaches more maturity.")
    , compaction_enforce_min_threshold(this, "compaction_enforce_min_threshold", liveness::LiveUpdate, value_status::Used, false,
        "If set to true, enforce the min_threshold option for compactions strictly. If false (default), Scylla may decide to compact even if below min_threshold.")
    , compaction_flush_all_tables_before_major_seconds(this, "compaction_flush_all_tables_before_major_seconds", value_status::Used, 86400,
        "Set the minimum interval in seconds between flushing all tables before each major compaction (default is 86400)."
        "This option is useful for maximizing tombstone garbage collection by releasing all active commitlog segments."
        "Set to 0 to disable automatic flushing all tables before major compaction.")
    /**
    * @Group Initialization properties
    * @GroupDescription The minimal properties needed for configuring a cluster.
    */
    , cluster_name(this, "cluster_name", value_status::Used, "",
        "The name of the cluster; used to prevent machines in one logical cluster from joining another. All nodes participating in a cluster must have the same value.")
    , listen_address(this, "listen_address", value_status::Used, "localhost",
        "The IP address or hostname that Scylla binds to for connecting to other Scylla nodes. You must change the default setting for multiple nodes to communicate. Do not set to 0.0.0.0, unless you have set broadcast_address to an address that other nodes can use to reach this node.")
    , listen_interface(this, "listen_interface", value_status::Unused, "eth0",
        "The interface that Scylla binds to for connecting to other Scylla nodes. Interfaces must correspond to a single address, IP aliasing is not supported. See listen_address.")
    , listen_interface_prefer_ipv6(this, "listen_interface_prefer_ipv6", value_status::Used, false,
        "If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address"
        "you can specify which should be chosen using listen_interface_prefer_ipv6. If false the first ipv4"
        "address will be used. If true the first ipv6 address will be used. Defaults to false preferring"
        "ipv4. If there is only one address it will be selected regardless of ipv4/ipv6."
    )
    /**
    * @Group Default directories
    * @GroupDescription If you have changed any of the default directories during installation, make sure you have root access and set these properties.
    */
    , work_directory(this, "workdir,W", value_status::Used, "/var/lib/scylla",
        "The directory in which Scylla will put all its subdirectories. The location of individual subdirs can be overridden by the respective ``*_directory`` options.")
    , commitlog_directory(this, "commitlog_directory", value_status::Used, "",
        "The directory where the commit log is stored. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories.")
    , schema_commitlog_directory(this, "schema_commitlog_directory", value_status::Used, "",
        "The directory where the schema commit log is stored. This is a special commitlog instance used for schema and system tables. For optimal write performance, it is recommended the commit log be on a separate disk partition (ideally, a separate physical device) from the data file directories.")
    , data_file_directories(this, "data_file_directories", "datadir", value_status::Used, { },
        "The directory location where table data (SSTables) is stored.")
    , hints_directory(this, "hints_directory", value_status::Used, "",
        "The directory where hints files are stored if hinted handoff is enabled.")
    , view_hints_directory(this, "view_hints_directory", value_status::Used, "",
        "The directory where materialized-view updates are stored while a view replica is unreachable.")
    , saved_caches_directory(this, "saved_caches_directory", value_status::Unused, "",
        "The directory location where table key and row caches are stored.")
    /**
    * @Group Commonly used properties
    * @GroupDescription Properties most frequently used when configuring Scylla.
     Before starting a node for the first time, you should carefully evaluate your requirements.
    */
    /**
    * @Group Common initialization properties
    * @GroupDescription Be sure to set the properties in the Quick start section as well.
    */
    , commit_failure_policy(this, "commit_failure_policy", value_status::Unused, "stop",
        "Policy for commit disk failures:\n"
        "* die          Shut down gossip, so the node can be replaced.\n"
        "* stop         Shut down gossip, leaving the node effectively dead, but can be inspected using the RESTful APIs.\n"
        "* stop_commit  Shut down the commit log, letting writes collect but continuing to service reads (as in pre-2.0.5 Cassandra).\n"
        "* ignore       Ignore fatal errors and let the batches fail."
        , {"die", "stop", "stop_commit", "ignore"})
    , disk_failure_policy(this, "disk_failure_policy", value_status::Unused, "stop",
        "Sets how Scylla responds to disk failure. Recommend settings are stop or best_effort.\n"
        "* die              Shut down gossip for any file system errors or single SSTable errors, so the node can be replaced.\n"
        "* stop_paranoid    Shut down gossip even for single SSTable errors.\n"
        "* stop             Shut down gossip, leaving the node effectively dead, but available for inspection using the RESTful APIs.\n"
        "* best_effort      Stop using the failed disk and respond to requests based on the remaining available SSTables. This means you will see obsolete data at consistency level of ONE.\n"
        "* ignore           Ignores fatal errors and lets the requests fail; all file system errors are logged but otherwise ignored. Scylla acts as in versions prior to Cassandra 1.2.\n"
        "Related information: Handling Disk Failures In Cassandra 1.2 blog and Recovering from a single disk failure using JBOD.\n"
        , {"die", "stop_paranoid", "stop", "best_effort", "ignore"})
    , endpoint_snitch(this, "endpoint_snitch", value_status::Used, "org.apache.cassandra.locator.SimpleSnitch",
        "Set to a class that implements the IEndpointSnitch. Scylla uses snitches for locating nodes and routing requests.\n"
        "* SimpleSnitch: Use for single-data center deployments or single-zone in public clouds. Does not recognize data center or rack information. It treats strategy order as proximity, which can improve cache locality when disabling read repair.\n"
        "* GossipingPropertyFileSnitch: Recommended for production. The rack and data center for the local node are defined in the cassandra-rackdc.properties file and propagated to other nodes via gossip. To allow migration from the PropertyFileSnitch, it uses the cassandra-topology.properties file if it is present.\n"
        /*"* PropertyFileSnitch: Determines proximity by rack and data center, which are explicitly configured in the cassandra-topology.properties file.\n"    */
        "* Ec2Snitch: For EC2 deployments in a single region. Loads region and availability zone information from the EC2 API. The region is treated as the data center and the availability zone as the rack. Uses only private IPs. Subsequently it does not work across multiple regions.\n"
        "* Ec2MultiRegionSnitch: Uses public IPs as the broadcast_address to allow cross-region connectivity. This means you must also set seed addresses to the public IP and open the storage_port or ssl_storage_port on the public IP firewall. For intra-region traffic, Scylla switches to the private IP after establishing a connection.\n"
        "* GoogleCloudSnitch: For deployments on Google Cloud Platform across one or more regions. The region is treated as a datacenter and the availability zone is treated as a rack within the datacenter. The communication should occur over private IPs within the same logical network.\n"
        "* RackInferringSnitch: Proximity is determined by rack and data center, which are assumed to correspond to the 3rd and 2nd octet of each node's IP address, respectively. This snitch is best used as an example for writing a custom snitch class (unless this happens to match your deployment conventions).\n"
        "\n"
        "Related information: Snitches\n")
    , rpc_address(this, "rpc_address", value_status::Used, "localhost",
        "The listen address for client connections (native transport).Valid values are:\n"
        "* unset: Resolves the address using the hostname configuration of the node. If left unset, the hostname must resolve to the IP address of this node using /etc/hostname, /etc/hosts, or DNS.\n"
        "* 0.0.0.0: Listens on all configured interfaces, but you must set the broadcast_rpc_address to a value other than 0.0.0.0.\n"
        "* IP address\n"
        "* hostname\n"
        "\n"
        "Related information: Network\n")
    , rpc_interface(this, "rpc_interface", value_status::Unused, "eth1",
        "The listen address for client connections. Interfaces must correspond to a single address, IP aliasing is not supported. See rpc_address.")
    , rpc_interface_prefer_ipv6(this, "rpc_interface_prefer_ipv6", value_status::Used, false,
        "If you choose to specify the interface by name and the interface has an ipv4 and an ipv6 address"
        "you can specify which should be chosen using rpc_interface_prefer_ipv6. If false the first ipv4"
        "address will be used. If true the first ipv6 address will be used. Defaults to false preferring"
        "ipv4. If there is only one address it will be selected regardless of ipv4/ipv6."
    )
    , seed_provider(this, "seed_provider", value_status::Used, seed_provider_type("org.apache.cassandra.locator.SimpleSeedProvider"),
        "The addresses of hosts deemed contact points. Scylla nodes use the -seeds list to find each other and learn the topology of the ring.\n"
        " \n"
        " * class_name (Default: org.apache.cassandra.locator.SimpleSeedProvider): The class within Scylla that handles the seed logic. It can be customized, but this is typically not required.\n"
        " * seeds (Default: 127.0.0.1): A comma-delimited list of IP addresses used by gossip for bootstrapping new nodes joining a cluster. When running multiple nodes, you must change the list from the default value. In multiple data-center clusters, the seed list should include at least one node from each data center (replication group). More than a single seed node per data center is recommended for fault tolerance. Otherwise, gossip has to communicate with another data center when bootstrapping a node. Making every node a seed node is not recommended because of increased maintenance and reduced gossip performance. Gossip optimization is not critical, but it is recommended to use a small seed list (approximately three nodes per data center).\n"
        "\n"
        "Related information: Initializing a multiple node cluster (single data center) and Initializing a multiple node cluster (multiple data centers).")
    /**
    * @Group Common compaction settings
    * @GroupDescription Be sure to set the properties in the Quick start section as well.
    */
    , compaction_throughput_mb_per_sec(this, "compaction_throughput_mb_per_sec", liveness::LiveUpdate, value_status::Used, 0,
        "Throttles compaction to the specified total throughput across the entire system. The faster you insert data, the faster you need to compact in order to keep the SSTable count down. The recommended Value is 16 to 32 times the rate of write throughput (in MBs/second). Setting the value to 0 disables compaction throttling.\n"
        "\n"
        "Related information: Configuring compaction")
    , compaction_large_partition_warning_threshold_mb(this, "compaction_large_partition_warning_threshold_mb", liveness::LiveUpdate, value_status::Used, 1000,
        "Log a warning when writing partitions larger than this value.")
    , compaction_large_row_warning_threshold_mb(this, "compaction_large_row_warning_threshold_mb", liveness::LiveUpdate, value_status::Used, 10,
        "Log a warning when writing rows larger than this value.")
    , compaction_large_cell_warning_threshold_mb(this, "compaction_large_cell_warning_threshold_mb", liveness::LiveUpdate, value_status::Used, 1,
        "Log a warning when writing cells larger than this value.")
    , compaction_rows_count_warning_threshold(this, "compaction_rows_count_warning_threshold", liveness::LiveUpdate, value_status::Used, 100000,
        "Log a warning when writing a number of rows larger than this value.")
    , compaction_collection_elements_count_warning_threshold(this, "compaction_collection_elements_count_warning_threshold", liveness::LiveUpdate, value_status::Used, 10000,
        "Log a warning when writing a collection containing more elements than this value.")
    /**
    * @Group Common memtable settings
    */
    , memtable_total_space_in_mb(this, "memtable_total_space_in_mb", value_status::Invalid, 0,
        "Specifies the total memory used for all memtables on a node. This replaces the per-table storage settings memtable_operations_in_millions and memtable_throughput_in_mb.")
    /**
    * @Group Common disk settings
    */
    , concurrent_reads(this, "concurrent_reads", value_status::Invalid, 32,
        "For workloads with more data than can fit in memory, the bottleneck is reads fetching data from disk. Setting to (16 × number_of_drives) allows operations to queue low enough in the stack so that the OS and drives can reorder them.")
    , concurrent_writes(this, "concurrent_writes", value_status::Invalid, 32,
        "Writes in Cassandra are rarely I/O bound, so the ideal number of concurrent writes depends on the number of CPU cores in your system. The recommended value is (8 x number_of_cpu_cores).")
    , concurrent_counter_writes(this, "concurrent_counter_writes", value_status::Unused, 32,
        "Counter writes read the current values before incrementing and writing them back. The recommended value is (16 × number_of_drives) .")
    /**
    * @Group Common automatic backup settings
    */
    , incremental_backups(this, "incremental_backups", value_status::Used, false,
        "Backs up data updated since the last snapshot was taken. When enabled, Scylla creates a hard link to each SSTable flushed or streamed locally in a backups/ subdirectory of the keyspace data. Removing these links is the operator's responsibility.\n"
        "\n"
        "Related information: Enabling incremental backups")
    , snapshot_before_compaction(this, "snapshot_before_compaction", value_status::Unused, false,
        "Enable or disable taking a snapshot before each compaction. This option is useful to back up data when there is a data format change. Be careful using this option because Cassandra does not clean up older snapshots automatically.\n"
        "\n"
        "Related information: Configuring compaction")
    /**
    * @Group Common fault detection setting
    */
    , phi_convict_threshold(this, "phi_convict_threshold", value_status::Used, 8,
        "Adjusts the sensitivity of the failure detector on an exponential scale. Generally this setting never needs adjusting.\n"
        "\n"
        "Related information: Failure detection and recovery")
    , failure_detector_timeout_in_ms(this, "failure_detector_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 20 * 1000, "Maximum time between two successful echo message before gossip mark a node down in milliseconds.\n")
    , direct_failure_detector_ping_timeout_in_ms(this, "direct_failure_detector_ping_timeout_in_ms", value_status::Used, 600, "Duration after which the direct failure detector aborts a ping message, so the next ping can start.\n"
        "Note: this failure detector is used by Raft, and is different from gossiper's failure detector (configured by `failure_detector_timeout_in_ms`).\n")
    /**
    * @Group Performance tuning properties
    * @GroupDescription Tuning performance and system resource utilization, including commit log, compaction, memory, disk I/O, CPU, reads, and writes.
    */
    /**
    * @Group Commit log settings
    */
    , commitlog_sync(this, "commitlog_sync", value_status::Used, "periodic",
        "The method that Scylla uses to acknowledge writes in milliseconds:\n"
        "* periodic: Used with commitlog_sync_period_in_ms (Default: 10000 - 10 seconds ) to control how often the commit log is synchronized to disk. Periodic syncs are acknowledged immediately.\n"
        "* batch: Used with commitlog_sync_batch_window_in_ms (Default: disabled ``**``) to control how long Scylla waits for other writes before performing a sync. When using this method, writes are not acknowledged until fsynced to disk.\n"
        "\n"
        "Related information: Durability")
    , commitlog_segment_size_in_mb(this, "commitlog_segment_size_in_mb", value_status::Used, 64,
        "Sets the size of the individual commitlog file segments. A commitlog segment may be archived, deleted, or recycled after all its data has been flushed to SSTables. This amount of data can potentially include commitlog segments from every table in the system. The default size is usually suitable for most commitlog archiving, but if you want a finer granularity, 8 or 16 MB is reasonable. See Commit log archive configuration.\n"
        "\n"
        "Related information: Commit log archive configuration")
    , schema_commitlog_segment_size_in_mb(this, "schema_commitlog_segment_size_in_mb", value_status::Used, 128,
        "Sets the size of the individual schema commitlog file segments. The default size is larger than the default size of the data commitlog because the segment size puts a limit on the mutation size that can be written at once, and some schema mutation writes are much larger than average.\n"
        "\n"
        "Related information: Commit log archive configuration")
    /* Note: does not exist on the listing page other than in above comment, wtf? */
    , commitlog_sync_period_in_ms(this, "commitlog_sync_period_in_ms", value_status::Used, 10000,
        "Controls how long the system waits for other writes before performing a sync in ``periodic`` mode.")
    /* Note: does not exist on the listing page other than in above comment, wtf? */
    , commitlog_sync_batch_window_in_ms(this, "commitlog_sync_batch_window_in_ms", value_status::Used, 10000,
        "Controls how long the system waits for other writes before performing a sync in ``batch`` mode.")
    , commitlog_max_data_lifetime_in_seconds(this, "commitlog_max_data_lifetime_in_seconds", liveness::LiveUpdate, value_status::Used, 24*60*60,
        "Controls how long data remains in commit log before the system tries to evict it to sstable, regardless of usage pressure. (0 disables)")
    , commitlog_total_space_in_mb(this, "commitlog_total_space_in_mb", value_status::Used, -1,
        "Total space used for commitlogs. If the used space goes above this value, Scylla rounds up to the next nearest segment multiple and flushes memtables to disk for the oldest commitlog segments, removing those log segments. This reduces the amount of data to replay on startup, and prevents infrequently-updated tables from indefinitely keeping commitlog segments. A small total commitlog space tends to cause more flush activity on less-active tables.\n"
        "\n"
        "Related information: Configuring memtable throughput")
    /* Note: Unused. Retained for upgrade compat. Deprecate and remove in a cycle or two. */
    , commitlog_reuse_segments(this, "commitlog_reuse_segments", value_status::Unused, true,
        "Whether or not to reuse commitlog segments when finished instead of deleting them. Can improve commitlog latency on some file systems.\n")
    , commitlog_flush_threshold_in_mb(this, "commitlog_flush_threshold_in_mb", value_status::Used, -1,
        "Threshold for commitlog disk usage. When used disk space goes above this value, Scylla initiates flushes of memtables to disk for the oldest commitlog segments, removing those log segments. Adjusting this affects disk usage vs. write latency. Default is (approximately) commitlog_total_space_in_mb - <num shards>*commitlog_segment_size_in_mb.")
    , commitlog_use_o_dsync(this, "commitlog_use_o_dsync", value_status::Used, true,
        "Whether or not to use O_DSYNC mode for commitlog segments IO. Can improve commitlog latency on some file systems.\n")
    , commitlog_use_hard_size_limit(this, "commitlog_use_hard_size_limit", value_status::Deprecated, true,
        "Whether or not to use a hard size limit for commitlog disk usage. Default is true. Enabling this can cause latency spikes, whereas disabling this can lead to occasional disk usage peaks.\n")
    , commitlog_use_fragmented_entries(this, "commitlog_use_fragmented_entries", value_status::Used, true,
        "Whether or not to allow commitlog entries to fragment across segments, allowing for larger entry sizes.\n")
    /**
    * @Group Compaction settings
    * @GroupDescription Related information: Configuring compaction
    */
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
    /**
    * @Group Memtable settings
    */
    , memtable_allocation_type(this, "memtable_allocation_type", value_status::Invalid, "heap_buffers",
        "Specify the way Cassandra allocates and manages memtable memory. See Off-heap memtables in Cassandra 2.1. Options are:\n"
        "* heap_buffers     On heap NIO (non-blocking I/O) buffers.\n"
        "* offheap_buffers  Off heap (direct) NIO buffers.\n"
        "* offheap_objects  Native memory, eliminating NIO buffer heap overhead.")
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
    /**
    * @Group Cache and index settings
    */
    , column_index_size_in_kb(this, "column_index_size_in_kb", value_status::Used, 64,
        "Granularity of the index of rows within a partition. For huge rows, decrease this setting to improve seek time. If you use key cache, be careful not to make this setting too large because key cache will be overwhelmed. If you're unsure of the size of the rows, it's best to use the default setting.")
    , column_index_auto_scale_threshold_in_kb(this, "column_index_auto_scale_threshold_in_kb", liveness::LiveUpdate, value_status::Used, 10240,
        "Auto-reduce the promoted index granularity by half when reaching this threshold, to prevent promoted index bloating due to partitions with too many rows. Set to 0 to disable this feature.")
    , index_summary_capacity_in_mb(this, "index_summary_capacity_in_mb", value_status::Unused, 0,
        "Fixed memory pool size in MB for SSTable index summaries. If the memory usage of all index summaries exceeds this limit, any SSTables with low read rates shrink their index summaries to meet this limit. This is a best-effort process. In extreme conditions, Cassandra may need to use more than this amount of memory.")
    , index_summary_resize_interval_in_minutes(this, "index_summary_resize_interval_in_minutes", value_status::Unused, 60,
        "How frequently index summaries should be re-sampled. This is done periodically to redistribute memory from the fixed-size pool to SSTables proportional their recent read rates. To disable, set to -1. This leaves existing index summaries at their current sampling level.")
    , reduce_cache_capacity_to(this, "reduce_cache_capacity_to", value_status::Invalid, .6,
        "Sets the size percentage to which maximum cache capacity is reduced when Java heap usage reaches the threshold defined by reduce_cache_sizes_at. Together with flush_largest_memtables_at, these properties constitute an emergency measure for preventing sudden out-of-memory (OOM) errors.")
    , reduce_cache_sizes_at(this, "reduce_cache_sizes_at", value_status::Invalid, .85,
        "When Java heap usage (after a full concurrent mark sweep (CMS) garbage collection) exceeds this percentage, Cassandra reduces the cache capacity to the fraction of the current size as specified by reduce_cache_capacity_to. To disable, set the value to 1.0.")
    /**
    * @Group Disks settings
    */
    , stream_throughput_outbound_megabits_per_sec(this, "stream_throughput_outbound_megabits_per_sec", value_status::Unused, 400,
        "Throttles all outbound streaming file transfers on a node to the specified throughput. Cassandra does mostly sequential I/O when streaming data during bootstrap or repair, which can lead to saturating the network connection and degrading client (RPC) performance.")
    , inter_dc_stream_throughput_outbound_megabits_per_sec(this, "inter_dc_stream_throughput_outbound_megabits_per_sec", value_status::Unused, 0,
        "Throttles all streaming file transfer between the data centers. This setting allows throttles streaming throughput betweens data centers in addition to throttling all network stream traffic as configured with stream_throughput_outbound_megabits_per_sec.")
    , stream_io_throughput_mb_per_sec(this, "stream_io_throughput_mb_per_sec", liveness::LiveUpdate, value_status::Used, 0,
        "Throttles streaming I/O to the specified total throughput (in MiBs/s) across the entire system. Streaming I/O includes the one performed by repair and both RBNO and legacy topology operations such as adding or removing a node. Setting the value to 0 disables stream throttling.")
    , stream_plan_ranges_fraction(this, "stream_plan_ranges_fraction", liveness::LiveUpdate, value_status::Used, 0.1,
        "Specify the fraction of ranges to stream in a single stream plan. Value is between 0 and 1.")
    , enable_file_stream(this, "enable_file_stream", liveness::LiveUpdate, value_status::Used, true, "Set true to use file based stream for tablet instead of mutation based stream")
    , trickle_fsync(this, "trickle_fsync", value_status::Unused, false,
        "When doing sequential writing, enabling this option tells fsync to force the operating system to flush the dirty buffers at a set interval trickle_fsync_interval_in_kb. Enable this parameter to avoid sudden dirty buffer flushing from impacting read latencies. Recommended to use on SSDs, but not on HDDs.")
    , trickle_fsync_interval_in_kb(this, "trickle_fsync_interval_in_kb", value_status::Unused, 10240,
        "Sets the size of the fsync in kilobytes.")
    /**
    * @Group Advanced properties
    * @GroupDescription Properties for advanced users or properties that are less commonly used.
    */
    /**
    * @Group Advanced initialization properties
    * @GroupDescription Properties for advanced users or properties that are less commonly used.
    */
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
        "This parameter can be used with num_tokens (vnodes) in special cases such as Restoring from a snapshot.")
    , num_tokens(this, "num_tokens", value_status::Used, 1,
        "Defines the number of tokens randomly assigned to this node on the ring when using virtual nodes (vnodes). The more tokens, relative to other nodes, the larger the proportion of data that the node stores. Generally all nodes should have the same number of tokens assuming equal hardware capability. The recommended value is 256. If unspecified (#num_tokens), Scylla uses 1 (equivalent to #num_tokens : 1) for legacy compatibility and uses the initial_token setting.\n"
        "If not using vnodes, comment #num_tokens : 256 or set num_tokens : 1 and use initial_token. If you already have an existing cluster with one token per node and wish to migrate to vnodes, see Enabling virtual nodes on an existing production cluster.\n"
        "\n"
        ".. note:: If using DataStax Enterprise, the default setting of this property depends on the type of node and type of install.")
    , partitioner(this, "partitioner", value_status::Used, "org.apache.cassandra.dht.Murmur3Partitioner",
        "Distributes rows (by partition key) across all nodes in the cluster. At the moment, only Murmur3Partitioner is supported. For new clusters use the default partitioner.\n"
        "\n"
        "Related information: Partitioners",
        {"org.apache.cassandra.dht.Murmur3Partitioner"})
    , storage_port(this, "storage_port", value_status::Used, 7000,
        "The port for inter-node communication.")
    /**
    * @Group Advanced automatic backup setting
    */
    , auto_snapshot(this, "auto_snapshot", value_status::Used, true,
        "Enable or disable whether a snapshot is taken of the data before keyspace truncation or dropping of tables. To prevent data loss, using the default setting is strongly advised. If you set to false, you will lose data on truncation or drop.")
    /**
    * @Group Key caches and global row properties
    * @GroupDescription When creating or modifying tables, you enable or disable the key cache (partition key cache) or row cache for that table by setting the caching parameter. Other row and key cache tuning and configuration options are set at the global (node) level. Cassandra uses these settings to automatically distribute memory for each table on the node based on the overall workload and specific table usage. You can also configure the save periods for these caches globally.
      Related information: Configuring caches.
    */
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
        "* NativeAllocator\n"
        "* JEMallocAllocator\n"
        "\n"
        "Experiments show that jemalloc saves some memory compared to the native allocator because it is more fragmentation resistant. To use, install jemalloc as a library and modify cassandra-env.sh (instructions in file).")
    /**
    * @Group Counter caches properties
    * @GroupDescription Counter cache helps to reduce counter locks' contention for hot counter cells. In case of RF = 1 a counter cache hit will cause Cassandra to skip the read before write entirely. With RF > 1 a counter cache hit will still help to reduce the duration of the lock hold, helping with hot counter cell updates, but will not allow skipping the read entirely. Only the local (clock, count) tuple of a counter cell is kept in memory, not the whole counter, so it's relatively cheap.
      Note: Reducing the size counter cache may result in not getting the hottest keys loaded on start-up.
    */
    , counter_cache_size_in_mb(this, "counter_cache_size_in_mb", value_status::Unused, 0,
        "When no value is specified a minimum of 2.5% of Heap or 50MB. If you perform counter deletes and rely on low gc_grace_seconds, you should disable the counter cache. To disable, set to 0")
    , counter_cache_save_period(this, "counter_cache_save_period", value_status::Unused, 7200,
        "Duration after which Cassandra should save the counter cache (keys only). Caches are saved to saved_caches_directory.")
    , counter_cache_keys_to_save(this, "counter_cache_keys_to_save", value_status::Unused, 0,
        "Number of keys from the counter cache to save. When disabled all keys are saved.")
    /**
    * @Group Tombstone settings
    * @GroupDescription When executing a scan, within or across a partition, tombstones must be kept in memory to allow returning them to the coordinator. The coordinator uses them to ensure other replicas know about the deleted rows. Workloads that generate numerous tombstones may cause performance problems and exhaust the server heap. See Cassandra anti-patterns: Queues and queue-like datasets. Adjust these thresholds only if you understand the impact and want to scan more tombstones. Additionally, you can adjust these thresholds at runtime using the StorageServiceMBean.
      Related information: Cassandra anti-patterns: Queues and queue-like datasets.
    */
    , tombstone_warn_threshold(this, "tombstone_warn_threshold", value_status::Used, 1000,
        "The maximum number of tombstones a query can scan before warning.")
    , tombstone_failure_threshold(this, "tombstone_failure_threshold", value_status::Unused, 100000,
        "The maximum number of tombstones a query can scan before aborting.")
    , query_tombstone_page_limit(this, "query_tombstone_page_limit", liveness::LiveUpdate, value_status::Used, 10000,
        "The number of tombstones after which a query cuts a page, even if not full or even empty.")
    , query_page_size_in_bytes(this, "query_page_size_in_bytes", liveness::LiveUpdate, value_status::Used, 1 << 20,
        "The size of pages in bytes, after a page accumulates this much data, the page is cut and sent to the client."
        " Setting a too large value increases the risk of OOM.")
    , group0_tombstone_gc_refresh_interval_in_ms(this, "group0_tombstone_gc_refresh_interval_in_ms", value_status::Used,
              std::chrono::duration_cast<std::chrono::milliseconds>(60min).count(),
              "The interval in milliseconds at which we update the time point for safe tombstone expiration in group0 tables.")
    /**
    * @Group Network timeout settings
    */
    , range_request_timeout_in_ms(this, "range_request_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 10000,
        "The time in milliseconds that the coordinator waits for sequential or index scans to complete.")
    , read_request_timeout_in_ms(this, "read_request_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 5000,
        "The time that the coordinator waits for read operations to complete")
    , counter_write_request_timeout_in_ms(this, "counter_write_request_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 5000,
        "The time that the coordinator waits for counter writes to complete.")
    , cas_contention_timeout_in_ms(this, "cas_contention_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 1000,
        "The time that the coordinator continues to retry a CAS (compare and set) operation that contends with other proposals for the same row.")
    , truncate_request_timeout_in_ms(this, "truncate_request_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 60000,
        "The time that the coordinator waits for truncates (remove all data from a table) to complete. The long default value allows for a snapshot to be taken before removing the data. If auto_snapshot is disabled (not recommended), you can reduce this time.")
    , write_request_timeout_in_ms(this, "write_request_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 2000,
        "The time in milliseconds that the coordinator waits for write operations to complete.\n"
        "\n"
        "Related information: About hinted handoff writes")
    , request_timeout_in_ms(this, "request_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 10000,
        "The default timeout for other, miscellaneous operations.\n"
        "\n"
        "Related information: About hinted handoff writes")
    /**
    * @Group Inter-node settings
    */
    , cross_node_timeout(this, "cross_node_timeout", value_status::Unused, false,
        "Enable or disable operation timeout information exchange between nodes (to accurately measure request timeouts). If disabled Cassandra assumes the request was forwarded to the replica instantly by the coordinator.\n"
        "CAUTION:\n"
        "Before enabling this property make sure NTP (network time protocol) is installed and the times are synchronized between the nodes.")
    , internode_send_buff_size_in_bytes(this, "internode_send_buff_size_in_bytes", value_status::Unused, 0,
        "Sets the sending socket buffer size in bytes for inter-node calls.\n"
        "When setting this parameter and internode_recv_buff_size_in_bytes, the buffer size is limited by net.core.wmem_max. When unset, buffer size is defined by net.ipv4.tcp_wmem. See man tcp and:\n"
        "* /proc/sys/net/core/wmem_max\n"
        "* /proc/sys/net/core/rmem_max\n"
        "* /proc/sys/net/ipv4/tcp_wmem\n"
        "* /proc/sys/net/ipv4/tcp_wmem\n")
    , internode_recv_buff_size_in_bytes(this, "internode_recv_buff_size_in_bytes", value_status::Unused, 0,
        "Sets the receiving socket buffer size in bytes for inter-node calls.")
    , internode_compression(this, "internode_compression", value_status::Used, "none",
        "Controls whether traffic between nodes is compressed. The valid values are:\n"
        "\tall: All traffic is compressed.\n"
        "\tdc : Traffic between data centers is compressed.\n"
        "\tnone : No compression.")
    , internode_compression_zstd_max_cpu_fraction(this, "internode_compression_zstd_max_cpu_fraction", liveness::LiveUpdate, value_status::Used, 0.000,
        "ZSTD compression of RPC will consume at most this fraction of each internode_compression_zstd_quota_refresh_period_ms time slice.\n"
        "If you wish to try out zstd for RPC compression, 0.05 is a reasonable starting point.")
    , internode_compression_zstd_cpu_quota_refresh_period_ms(this, "internode_compression_zstd_cpu_quota_refresh_period_ms", liveness::LiveUpdate, value_status::Used, 20,
        "Advanced. ZSTD compression of RPC will consume at most internode_compression_zstd_max_cpu_fraction (plus one message) of in each time slice of this length.")
    , internode_compression_zstd_max_longterm_cpu_fraction(this, "internode_compression_zstd_max_longterm_cpu_fraction", liveness::LiveUpdate, value_status::Used, 1.000,
        "ZSTD compression of RPC will consume at most this fraction of each internode_compression_zstd_longterm_cpu_quota_refresh_period_ms time slice.")
    , internode_compression_zstd_longterm_cpu_quota_refresh_period_ms(this, "internode_compression_zstd_longterm_cpu_quota_refresh_period_ms", liveness::LiveUpdate, value_status::Used, 10000,
        "Advanced. ZSTD compression of RPC will consume at most internode_compression_zstd_max_longterm_cpu_fraction (plus one message) of in each time slice of this length.")
    , internode_compression_zstd_min_message_size(this, "internode_compression_zstd_min_message_size", liveness::LiveUpdate, value_status::Used, 1024,
        "Minimum RPC message size which can be compressed with ZSTD. Messages smaller than this threshold will always be compressed with LZ4. "
        "ZSTD has high per-message overhead, and might be a bad choice for small messages. This knob allows for some experimentation with that. ")
    , internode_compression_zstd_max_message_size(this, "internode_compression_zstd_max_message_size", liveness::LiveUpdate, value_status::Used, std::numeric_limits<uint32_t>::max(),
        "Maximum RPC message size which can be compressed with ZSTD. RPC messages might be large, but they are always compressed at once. This might cause reactor stalls. "
        "If this happens, this option can be used to make the stalls less severe.")
    , internode_compression_checksumming(this, "internode_compression_checksumming", liveness::LiveUpdate, value_status::Used, true,
        "Computes and checks checksums for compressed RPC frames. This is a paranoid precaution against corruption bugs in the compression protocol.")
    , internode_compression_algorithms(this, "internode_compression_algorithms", liveness::LiveUpdate, value_status::Used,
            { utils::compression_algorithm::type::ZSTD, utils::compression_algorithm::type::LZ4, },
        "Specifies RPC compression algorithms supported by this node. ")
    , internode_compression_enable_advanced(this, "internode_compression_enable_advanced", liveness::MustRestart, value_status::Used, false,
        "Enables the new implementation of RPC compression. If disabled, Scylla will fall back to the old implementation.")
    , rpc_dict_training_when(this, "rpc_dict_training_when", liveness::LiveUpdate, value_status::Used, utils::dict_training_loop::when::type::NEVER,
        "Specifies when RPC compression dictionary training is performed by this node.\n"
        "* `never` disables it unconditionally.\n"
        "* `when_leader` enables it only whenever the node is the Raft leader.\n"
        "* `always` (not recommended) enables it unconditionally.\n"
        "\n"
        "Training shouldn't be enabled on more than one node at a time, because overly-frequent dictionary announcements might indefinitely delay nodes from agreeing on a new dictionary.")
    , rpc_dict_training_min_time_seconds(this, "rpc_dict_training_min_time_seconds", liveness::LiveUpdate, value_status::Used, 3600,
        "Specifies the minimum duration of RPC compression dictionary training.")
    , rpc_dict_training_min_bytes(this, "rpc_dict_training_min_bytes", liveness::LiveUpdate, value_status::Used, 1'000'000'000,
        "Specifies the minimum volume of RPC compression dictionary training.")
    , inter_dc_tcp_nodelay(this, "inter_dc_tcp_nodelay", value_status::Used, false,
        "Enable or disable tcp_nodelay for inter-data center communication. When disabled larger, but fewer, network packets are sent. This reduces overhead from the TCP protocol itself. However, if cross data-center responses are blocked, it will increase latency.")
    , streaming_socket_timeout_in_ms(this, "streaming_socket_timeout_in_ms", value_status::Unused, 0,
        "Enable or disable socket timeout for streaming operations. When a timeout occurs during streaming, streaming is retried from the start of the current file. Avoid setting this value too low, as it can result in a significant amount of data re-streaming.")
    /**
    * @Group Native transport (CQL Binary Protocol)
    */
    , start_native_transport(this, "start_native_transport", value_status::Used, true,
        "Enable or disable the native transport server. Uses the same address as the rpc_address, but the port is different from the rpc_port. See native_transport_port.")
    , native_transport_port(this, "native_transport_port", "cql_port", value_status::Used, 9042,
        "Port on which the CQL native transport listens for clients.")
    , maintenance_socket(this, "maintenance_socket", value_status::Used, "ignore",
        "The Unix Domain Socket the node uses for maintenance socket.\n"
        "The possible options are:\n"
        "\tignore         the node will not open the maintenance socket.\n"
        "\tworkdir        the node will open the maintenance socket on the path <scylla's workdir>/cql.m,\n"
        "\t               where <scylla's workdir> is a path defined by the workdir configuration option\n"
        "\t<socket path>  the node will open the maintenance socket on the path <socket path>")
    , maintenance_socket_group(this, "maintenance_socket_group", value_status::Used, "",
        "The group that the maintenance socket will be owned by. If not set, the group will be the same as the user running the scylla node.")
    , maintenance_mode(this, "maintenance_mode", value_status::Used, false, "If set to true, the node will not connect to other nodes. It will only serve requests to its local data.")
    , native_transport_port_ssl(this, "native_transport_port_ssl", value_status::Used, 9142,
        "Port on which the CQL TLS native transport listens for clients."
        "Enabling client encryption and keeping native_transport_port_ssl disabled will use encryption"
        "for native_transport_port. Setting native_transport_port_ssl to a different value"
        "from native_transport_port will use encryption for native_transport_port_ssl while"
        "keeping native_transport_port unencrypted.")
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
    /**
    * @Group RPC (remote procedure call) settings
    * @GroupDescription Settings for configuring and tuning client connections.
    */
    , broadcast_rpc_address(this, "broadcast_rpc_address", value_status::Used, {/* unset */},
        "RPC address to broadcast to drivers and other Scylla nodes. This cannot be set to 0.0.0.0. If blank, it is set to the value of the rpc_address or rpc_interface. If rpc_address or rpc_interfaceis set to 0.0.0.0, this property must be set.\n")
    , rpc_port(this, "rpc_port", "thrift_port", value_status::Unused, 0,
        "Thrift port for client connections.")
    , start_rpc(this, "start_rpc", value_status::Unused, false,
        "Starts the Thrift RPC server")
    , rpc_keepalive(this, "rpc_keepalive", value_status::Used, true,
        "Enable or disable keepalive on client connections (CQL native, Redis and the maintenance socket).")
    , cache_hit_rate_read_balancing(this, "cache_hit_rate_read_balancing", value_status::Used, true,
        "This boolean controls whether the replicas for read query will be chosen based on cache hit ratio.")
    /**
    * @Group Advanced fault detection settings
    * @GroupDescription Settings to handle poorly performing or failing nodes.
    */
    , dynamic_snitch_badness_threshold(this, "dynamic_snitch_badness_threshold", value_status::Unused, 0,
        "Sets the performance threshold for dynamically routing requests away from a poorly performing node. A value of 0.2 means Cassandra continues to prefer the static snitch values until the node response time is 20% worse than the best performing node. Until the threshold is reached, incoming client requests are statically routed to the closest replica (as determined by the snitch). Having requests consistently routed to a given replica can help keep a working set of data hot when read repair is less than 1.")
    , dynamic_snitch_reset_interval_in_ms(this, "dynamic_snitch_reset_interval_in_ms", value_status::Unused, 60000,
        "Time interval in milliseconds to reset all node scores, which allows a bad node to recover.")
    , dynamic_snitch_update_interval_in_ms(this, "dynamic_snitch_update_interval_in_ms", value_status::Unused, 100,
        "The time interval for how often the snitch calculates node scores. Because score calculation is CPU intensive, be careful when reducing this interval.")
    , hinted_handoff_enabled(this, "hinted_handoff_enabled", value_status::Used, db::config::hinted_handoff_enabled_type(db::config::hinted_handoff_enabled_type::enabled_for_all_tag()),
        "Enable or disable hinted handoff. To enable per data center, add data center list. For example: hinted_handoff_enabled: DC1,DC2. A hint indicates that the write needs to be replayed to an unavailable node. "
        "\n"
        "Related information: About hinted handoff writes")
    , max_hinted_handoff_concurrency(this, "max_hinted_handoff_concurrency", liveness::LiveUpdate, value_status::Used, 0,
        "Maximum concurrency allowed for sending hints. The concurrency is divided across shards and rounded up if not divisible by the number of shards. By default (or when set to 0), concurrency of 8*shard_count will be used.")
    , hinted_handoff_throttle_in_kb(this, "hinted_handoff_throttle_in_kb", value_status::Unused, 1024,
        "Maximum throttle per delivery thread in kilobytes per second. This rate reduces proportionally to the number of nodes in the cluster. For example, if there are two nodes in the cluster, each delivery thread will use the maximum rate. If there are three, each node will throttle to half of the maximum, since the two nodes are expected to deliver hints simultaneously.")
    , max_hint_window_in_ms(this, "max_hint_window_in_ms", value_status::Used, 10800000,
        "Maximum amount of time that hints are generates hints for an unresponsive node. After this interval, new hints are no longer generated until the node is back up and responsive. If the node goes down again, a new interval begins. This setting can prevent a sudden demand for resources when a node is brought back online and the rest of the cluster attempts to replay a large volume of hinted writes.\n"
        "\n"
        "Related information: Failure detection and recovery")
    , max_hints_delivery_threads(this, "max_hints_delivery_threads", value_status::Invalid, 2,
        "Number of threads with which to deliver hints. In multiple data-center deployments, consider increasing this number because cross data-center handoff is generally slower.")
    , batchlog_replay_throttle_in_kb(this, "batchlog_replay_throttle_in_kb", value_status::Unused, 1024,
        "Total maximum throttle. Throttling is reduced proportionally to the number of nodes in the cluster.")
    , batchlog_replay_cleanup_after_replays(this, "batchlog_replay_cleanup_after_replays", liveness::LiveUpdate, value_status::Used, 60,
        "Clean up batchlog memtable after every N replays. Replays are issued on a timer, every 60 seconds. So if batchlog_replay_cleanup_after_replays is set to 60, the batchlog memtable is flushed every 60 * 60 seconds.")
    /**
    * @Group Request scheduler properties
    * @GroupDescription Settings to handle incoming client requests according to a defined policy. If you need to use these properties, your nodes are overloaded and dropping requests. It is recommended that you add more nodes and not try to prioritize requests.
    */
    , request_scheduler(this, "request_scheduler", value_status::Unused, "org.apache.cassandra.scheduler.NoScheduler",
        "Defines a scheduler to handle incoming client requests according to a defined policy. This scheduler is useful for throttling client requests in single clusters containing multiple keyspaces. This parameter is specifically for requests from the client and does not affect inter-node communication. Valid values are:\n"
        "* org.apache.cassandra.scheduler.NoScheduler   No scheduling takes place.\n"
        "* org.apache.cassandra.scheduler.RoundRobinScheduler   Round robin of client requests to a node with a separate queue for each request_scheduler_id property.\n"
        "* A Java class that implements the RequestScheduler interface."
        , {"org.apache.cassandra.scheduler.NoScheduler", "org.apache.cassandra.scheduler.RoundRobinScheduler"})
    , request_scheduler_id(this, "request_scheduler_id", value_status::Unused, {/* keyspace */},
        "An identifier on which to perform request scheduling. Currently the only valid value is keyspace. See weights.")
    , request_scheduler_options(this, "request_scheduler_options", value_status::Unused, {/* disabled */},
        "Contains a list of properties that define configuration options for request_scheduler:\n"
        "* throttle_limit: The number of in-flight requests per client. Requests beyond this limit are queued up until running requests complete. Recommended value is ((concurrent_reads + concurrent_writes) × 2)\n"
        "* default_weight: (Default: 1 **)  How many requests are handled during each turn of the RoundRobin.\n"
        "* weights: (Default: Keyspace: 1)  Takes a list of keyspaces. It sets how many requests are handled during each turn of the RoundRobin, based on the request_scheduler_id.")
    /**
    * @Group Security properties
    * @GroupDescription Server and client security settings.
    */
    , authenticator(this, "authenticator", value_status::Used, "org.apache.cassandra.auth.AllowAllAuthenticator",
        "The authentication backend, used to identify users. The available authenticators are:\n"
        "* org.apache.cassandra.auth.AllowAllAuthenticator: Disables authentication; no checks are performed.\n"
        "* org.apache.cassandra.auth.PasswordAuthenticator: Authenticates users with user names and hashed passwords stored in the system_auth.credentials table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.\n"
        "* com.scylladb.auth.CertificateAuthenticator: Authenticates users based on TLS certificate authentication subject. Roles and permissions still need to be defined as normal. Super user can be set using the 'auth_superuser_name' configuration value. Query to extract role name from subject string is set using 'auth_certificate_role_queries'.\n"
        "* com.scylladb.auth.TransitionalAuthenticator: Wraps around the PasswordAuthenticator, logging them in if username/password pair provided is correct and treating them as anonymous users otherwise.\n"
        "* com.scylladb.auth.SaslauthdAuthenticator : Use saslauthd for authentication.\n"
        "\n"
        "Related information: Internal authentication", 
        {"AllowAllAuthenticator", "PasswordAuthenticator", "CertificateAuthenticator", "org.apache.cassandra.auth.PasswordAuthenticator", "com.scylladb.auth.SaslauthdAuthenticator", "org.apache.cassandra.auth.AllowAllAuthenticator", "com.scylladb.auth.TransitionalAuthenticator", "com.scylladb.auth.CertificateAuthenticator"})
    , internode_authenticator(this, "internode_authenticator", value_status::Unused, "enabled",
        "Internode authentication backend. It implements org.apache.cassandra.auth.AllowAllInternodeAuthenticator to allows or disallow connections from peer nodes.")
    , authorizer(this, "authorizer", value_status::Used, "org.apache.cassandra.auth.AllowAllAuthorizer",
        "The authorization backend. It implements IAuthenticator, which limits access and provides permissions. The available authorizers are:\n"
        "* AllowAllAuthorizer: Disables authorization; allows any action to any user.\n"
        "* CassandraAuthorizer: Stores permissions in system_auth.permissions table. If you use the default, 1, and the node with the lone replica goes down, you will not be able to log into the cluster because the system_auth keyspace was not replicated.\n"
        "* com.scylladb.auth.TransitionalAuthorizer: Wraps around the CassandraAuthorizer, which is used to authorize permission management. Other actions are allowed for all users.\n"
        "\n"
        "Related information: Object permissions",
        {"AllowAllAuthorizer", "CassandraAuthorizer", "org.apache.cassandra.auth.AllowAllAuthorizer", "org.apache.cassandra.auth.CassandraAuthorizer", "com.scylladb.auth.TransitionalAuthorizer"})
    , role_manager(this, "role_manager", value_status::Used, "org.apache.cassandra.auth.CassandraRoleManager",
        "The role-management backend, used to maintain grants and memberships between roles."
        "The available role-managers are:\n"
        "* org.apache.cassandra.auth.CassandraRoleManager: Stores role data in the system_auth keyspace;\n"
        "* com.scylladb.auth.LDAPRoleManager: Fetches role data from an LDAP server.")
    , permissions_validity_in_ms(this, "permissions_validity_in_ms", liveness::LiveUpdate, value_status::Used, 10000,
        "How long permissions in cache remain valid. Depending on the authorizer, such as CassandraAuthorizer, fetching permissions can be resource intensive. Permissions caching is disabled when this property is set to 0 or when AllowAllAuthorizer is used. The cached value is considered valid as long as both its value is not older than the permissions_validity_in_ms "
        "and the cached value has been read at least once during the permissions_validity_in_ms time frame. If any of these two conditions doesn't hold the cached value is going to be evicted from the cache.\n"
        "\n"
        "Related information: Object permissions")
    , permissions_update_interval_in_ms(this, "permissions_update_interval_in_ms", liveness::LiveUpdate, value_status::Used, 2000,
        "Refresh interval for permissions cache (if enabled). After this interval, cache entries become eligible for refresh. An async reload is scheduled every permissions_update_interval_in_ms time period and the old value is returned until it completes. If permissions_validity_in_ms has a non-zero value, then this property must also have a non-zero value. It's recommended to set this value to be at least 3 times smaller than the permissions_validity_in_ms.")
    , permissions_cache_max_entries(this, "permissions_cache_max_entries", liveness::LiveUpdate, value_status::Used, 1000,
        "Maximum cached permission entries. Must have a non-zero value if permissions caching is enabled (see a permissions_validity_in_ms description).")
    , server_encryption_options(this, "server_encryption_options", value_status::Used, {/*none*/},
        "Enable or disable inter-node encryption. You must also generate keys and provide the appropriate key and trust store locations and passwords. The available options are:\n"
        "* internode_encryption: (Default: none) Enable or disable encryption of inter-node communication using the TLS_RSA_WITH_AES_128_CBC_SHA cipher suite for authentication, key exchange, and encryption of data transfers. The available inter-node options are:\n"
        "   * all: Encrypt all inter-node communications.\n"
        "   * none: No encryption.\n"
        "   * dc: Encrypt the traffic between the data centers (server only).\n"
        "   * rack: Encrypt the traffic between the racks(server only).\n"
        "* certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the internode communication.\n"
        "* keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.\n"
        "* truststore: (Default: <not set, use system truststore> ) Location of the truststore containing the trusted certificate for authenticating remote servers.\n"
        "* certficate_revocation_list: (Default: <not set>) PEM encoded certificate revocation list.\n"
        "\n"
        "The advanced settings are:\n"
        "\n"
        "* priority_string: (Default: not set, use default) GnuTLS priority string controlling TLS algorithms used/allowed.\n"
        "* require_client_auth: (Default: false ) Enables or disables certificate authentication.\n"
        "\n"
        "Related information: Node-to-node encryption")
    , client_encryption_options(this, "client_encryption_options", value_status::Used, {/*none*/},
        "Enable or disable client-to-node encryption. You must also generate keys and provide the appropriate key and certificate. The available options are:\n"
        "* enabled: (Default: false) To enable, set to true.\n"
        "* certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.\n"
        "* keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.\n"
        "* truststore: (Default: <not set. use system truststore>) Location of the truststore containing the trusted certificate for authenticating remote servers.\n"
        "* certficate_revocation_list: (Default: <not set> ) PEM encoded certificate revocation list.\n"
        "\n"
        "The advanced settings are:\n"
        "\n"
        "* priority_string: (Default: not set, use default) GnuTLS priority string controlling TLS algorithms used/allowed.\n"
        "* require_client_auth: (Default: false) Enables or disables certificate authentication.\n"
        "\n"
        "Related information: Client-to-node encryption")
    , alternator_encryption_options(this, "alternator_encryption_options", value_status::Used, {/*none*/},
        "When Alternator via HTTPS is enabled with alternator_https_port, where to take the key and certificate. The available options are:\n"
        "* certificate: (Default: conf/scylla.crt) The location of a PEM-encoded x509 certificate used to identify and encrypt the client/server communication.\n"
        "* keyfile: (Default: conf/scylla.key) PEM Key file associated with certificate.\n"
        "\n"
        "The advanced settings are:\n"
        "\n"
        "* priority_string: GnuTLS priority string controlling TLS algorithms used/allowed.")
    , ssl_storage_port(this, "ssl_storage_port", value_status::Used, 7001,
        "The SSL port for encrypted communication. Unused unless enabled in encryption_options.")
    , enable_in_memory_data_store(this, "enable_in_memory_data_store", value_status::Used, false, "Enable in memory mode (system tables are always persisted).")
    , enable_cache(this, "enable_cache", value_status::Used, true, "Enable cache.")
    , enable_commitlog(this, "enable_commitlog", value_status::Used, true, "Enable commitlog.")
    , volatile_system_keyspace_for_testing(this, "volatile_system_keyspace_for_testing", value_status::Used, false, "Don't persist system keyspace - testing only!")
    , api_port(this, "api_port", value_status::Used, 10000, "Http Rest API port.")
    , api_address(this, "api_address", value_status::Used, "", "Http Rest API address.")
    , api_ui_dir(this, "api_ui_dir", value_status::Used, "swagger-ui/dist/", "The directory location of the API GUI.")
    , api_doc_dir(this, "api_doc_dir", value_status::Used, "api/api-doc/", "The API definition file directory.")
    , load_balance(this, "load_balance", value_status::Unused, "none", "CQL request load balancing: 'none' or round-robin.'")
    , consistent_rangemovement(this, "consistent_rangemovement", value_status::Used, true, "When set to true, range movements will be consistent. It means: 1) it will refuse to bootstrap a new node if other bootstrapping/leaving/moving nodes detected. 2) data will be streamed to a new node only from the node which is no longer responsible for the token range. Same as -Dcassandra.consistent.rangemovement in cassandra.")
    , join_ring(this, "join_ring", value_status::Used, true, "When set to true, a node will join the token ring. When set to false, a node will not join the token ring. This option cannot be changed after a node joins the cluster. If set to false, it overwrites the num_tokens and initial_token options. Setting to false is supported only if the cluster uses the raft-managed topology.")
    , load_ring_state(this, "load_ring_state", value_status::Used, true, "When set to true, load tokens and host_ids previously saved. Same as -Dcassandra.load_ring_state in cassandra.")
    , replace_node_first_boot(this, "replace_node_first_boot", value_status::Used, "", "The Host ID of a dead node to replace. If the replacing node has already been bootstrapped successfully, this option will be ignored.")
    , replace_address(this, "replace_address", value_status::Used, "", "[[deprecated]] The listen_address or broadcast_address of the dead node to replace. Same as -Dcassandra.replace_address.")
    , replace_address_first_boot(this, "replace_address_first_boot", value_status::Used, "", "[[deprecated]] Like replace_address option, but if the node has been bootstrapped successfully it will be ignored. Same as -Dcassandra.replace_address_first_boot.")
    , ignore_dead_nodes_for_replace(this, "ignore_dead_nodes_for_replace", value_status::Used, "", "List dead nodes to ignore for replace operation using a comma-separated list of host IDs. E.g., scylla --ignore-dead-nodes-for-replace 8d5ed9f4-7764-4dbd-bad8-43fddce94b7c,125ed9f4-7777-1dbn-mac8-43fddce9123e")
    , override_decommission(this, "override_decommission", value_status::Deprecated, false, "Set true to force a decommissioned node to join the cluster (cannot be set if consistent-cluster-management is enabled).")
    , enable_repair_based_node_ops(this, "enable_repair_based_node_ops", liveness::LiveUpdate, value_status::Used, true, "Set true to use enable repair based node operations instead of streaming based.")
    , allowed_repair_based_node_ops(this, "allowed_repair_based_node_ops", liveness::LiveUpdate, value_status::Used, "replace,removenode,rebuild,bootstrap,decommission", "A comma separated list of node operations which are allowed to enable repair based node operations. The operations can be bootstrap, replace, removenode, decommission and rebuild.")
    , enable_compacting_data_for_streaming_and_repair(this, "enable_compacting_data_for_streaming_and_repair", liveness::LiveUpdate, value_status::Used, true, "Enable the compacting reader, which compacts the data for streaming and repair (load'n'stream included) before sending it to, or synchronizing it with peers. Can reduce the amount of data to be processed by removing dead data, but adds CPU overhead.")
    , enable_tombstone_gc_for_streaming_and_repair(this, "enable_tombstone_gc_for_streaming_and_repair", liveness::LiveUpdate, value_status::Used, false,
            "If the compacting reader is enabled for streaming and repair (see enable_compacting_data_for_streaming_and_repair), allow it to garbage-collect tombstones."
            " This can reduce the amount of data repair has to process.")
    , repair_partition_count_estimation_ratio(this, "repair_partition_count_estimation_ratio", liveness::LiveUpdate, value_status::Used, 0.1,
        "Specify the fraction of partitions written by repair out of the total partitions. The value is currently only used for bloom filter estimation. Value is between 0 and 1.")
    , repair_hints_batchlog_flush_cache_time_in_ms(this, "repair_hints_batchlog_flush_cache_time_in_ms", liveness::LiveUpdate, value_status::Used, 60 * 1000, "The repair hints and batchlog flush request cache time. Setting 0 disables the flush cache. The cache reduces the number of hints and batchlog flushes during repair when tombstone_gc is set to repair mode. When the cache is on, a slightly smaller repair time will be used with the benefits of dropped hints and batchlog flushes.")
    , repair_multishard_reader_buffer_hint_size(this, "repair_multishard_reader_buffer_hint_size", liveness::LiveUpdate, value_status::Used, 1 * 1024 * 1024,
        "The buffer size to use for the buffer-hint feature of the multishard reader when running repair in mixed-shard clusters. This can help the performance of mixed-shard repair (including RBNO). Set to 0 to disable the hint feature altogether.")
    , repair_multishard_reader_enable_read_ahead(this, "repair_multishard_reader_enable_read_ahead", liveness::LiveUpdate, value_status::Used, false,
        "The multishard reader has a read-ahead feature to improve latencies of range-scans. This feature can be detrimental when the multishard reader is used under repair, as is the case in repair in mixed-shard clusters."
        " This know allows disabling this read-ahead (default), this can help the performance of mixed-shard repair (including RBNO).")
    , enable_small_table_optimization_for_rbno(this, "enable_small_table_optimization_for_rbno", liveness::LiveUpdate, value_status::Used, true, "Set true to enable small table optimization for repair based node operations")
    , ring_delay_ms(this, "ring_delay_ms", value_status::Used, 30 * 1000, "Time a node waits to hear from other nodes before joining the ring in milliseconds. Same as -Dcassandra.ring_delay_ms in cassandra.")
    , shadow_round_ms(this, "shadow_round_ms", value_status::Used, 300 * 1000, "The maximum gossip shadow round time. Can be used to reduce the gossip feature check time during node boot up.")
    , fd_max_interval_ms(this, "fd_max_interval_ms", value_status::Used, 2 * 1000, "The maximum failure_detector interval time in milliseconds. Interval larger than the maximum will be ignored. Larger cluster may need to increase the default.")
    , fd_initial_value_ms(this, "fd_initial_value_ms", value_status::Used, 2 * 1000, "The initial failure_detector interval time in milliseconds.")
    , shutdown_announce_in_ms(this, "shutdown_announce_in_ms", value_status::Used, 2 * 1000, "Time a node waits after sending gossip shutdown message in milliseconds. Same as -Dcassandra.shutdown_announce_in_ms in cassandra.")
    , developer_mode(this, "developer_mode", value_status::Used, DEVELOPER_MODE_DEFAULT, "Relax environment checks. Setting to true can reduce performance and reliability significantly.")
    , skip_wait_for_gossip_to_settle(this, "skip_wait_for_gossip_to_settle", value_status::Used, -1, "An integer to configure the wait for gossip to settle. -1: wait normally, 0: do not wait at all, n: wait for at most n polls. Same as -Dcassandra.skip_wait_for_gossip_to_settle in cassandra.")
    , force_gossip_generation(this, "force_gossip_generation", liveness::LiveUpdate, value_status::Used, -1 , "Force gossip to use the generation number provided by user.")
    , experimental_features(this, "experimental_features", value_status::Used, {}, experimental_features_help_string())
    , lsa_reclamation_step(this, "lsa_reclamation_step", value_status::Used, 1, "Minimum number of segments to reclaim in a single step.")
    , prometheus_port(this, "prometheus_port", value_status::Used, 9180, "Prometheus port, set to zero to disable.")
    , prometheus_address(this, "prometheus_address", value_status::Used, {/* listen_address */}, "Prometheus listening address, defaulting to listen_address if not explicitly set.")
    , prometheus_prefix(this, "prometheus_prefix", value_status::Used, "scylla", "Set the prefix of the exported Prometheus metrics. Changing this will break Scylla's dashboard compatibility, do not change unless you know what you are doing.")
    , prometheus_allow_protobuf(this, "prometheus_allow_protobuf", value_status::Used, false, "If set allows the experimental Prometheus protobuf with native histogram")
    , abort_on_lsa_bad_alloc(this, "abort_on_lsa_bad_alloc", value_status::Used, false, "Abort when allocation in LSA region fails.")
    , murmur3_partitioner_ignore_msb_bits(this, "murmur3_partitioner_ignore_msb_bits", value_status::Used, default_murmur3_partitioner_ignore_msb_bits, "Number of most significant token bits to ignore in murmur3 partitioner; increase for very large clusters.")
    , unspooled_dirty_soft_limit(this, "unspooled_dirty_soft_limit", value_status::Used, 0.6, "Soft limit of unspooled dirty memory expressed as a portion of the hard limit.")
    , sstable_summary_ratio(this, "sstable_summary_ratio", value_status::Used, 0.0005, "Enforces that 1 byte of summary is written for every N (2000 by default)"
        "bytes written to data file. Value must be between 0 and 1.")
    , components_memory_reclaim_threshold(this, "components_memory_reclaim_threshold", liveness::LiveUpdate, value_status::Used, .2, "Ratio of available memory for all in-memory components of SSTables in a shard beyond which the memory will be reclaimed from components until it falls back under the threshold. Currently, this limit is only enforced for bloom filters.")
    , large_memory_allocation_warning_threshold(this, "large_memory_allocation_warning_threshold", value_status::Used, size_t(1) << 20, "Warn about memory allocations above this size; set to zero to disable.")
    , enable_deprecated_partitioners(this, "enable_deprecated_partitioners", value_status::Used, false, "Enable the byteordered and random partitioners. These partitioners are deprecated and will be removed in a future version.")
    , enable_keyspace_column_family_metrics(this, "enable_keyspace_column_family_metrics", value_status::Used, false, "Enable per keyspace and per column family metrics reporting.")
    , enable_node_aggregated_table_metrics(this, "enable_node_aggregated_table_metrics", value_status::Used, true, "Enable aggregated per node, per keyspace and per table metrics reporting, applicable if enable_keyspace_column_family_metrics is false.")
    , enable_sstable_data_integrity_check(this, "enable_sstable_data_integrity_check", value_status::Used, false, "Enable interposer which checks for integrity of every sstable write."
        " Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.")
    , enable_sstable_key_validation(this, "enable_sstable_key_validation", value_status::Used, ENABLE_SSTABLE_KEY_VALIDATION, "Enable validation of partition and clustering keys monotonicity"
        " Performance is affected to some extent as a result. Useful to help debugging problems that may arise at another layers.")
    , cpu_scheduler(this, "cpu_scheduler", value_status::Used, true, "Enable cpu scheduling.")
    , view_building(this, "view_building", value_status::Used, true, "Enable view building; should only be set to false when the node is experience issues due to view building.")
    , enable_sstables_mc_format(this, "enable_sstables_mc_format", value_status::Unused, true, "Enable SSTables 'mc' format to be used as the default file format.  Deprecated, please use \"sstable_format\" instead.")
    , enable_sstables_md_format(this, "enable_sstables_md_format", value_status::Unused, true, "Enable SSTables 'md' format to be used as the default file format.  Deprecated, please use \"sstable_format\" instead.")
    , sstable_format(this, "sstable_format", value_status::Used, "me", "Default sstable file format", {"md", "me"})
    , uuid_sstable_identifiers_enabled(this,
            "uuid_sstable_identifiers_enabled", liveness::LiveUpdate, value_status::Used, true, "If set to true, each newly created sstable will have a UUID "
            "based generation identifier, and such files are not readable by previous Scylla versions.")
    , table_digest_insensitive_to_expiry(this, "table_digest_insensitive_to_expiry", liveness::MustRestart, value_status::Used, true,
            "When enabled, per-table schema digest calculation ignores empty partitions.")
    , enable_dangerous_direct_import_of_cassandra_counters(this, "enable_dangerous_direct_import_of_cassandra_counters", value_status::Used, false, "Only turn this option on if you want to import tables from Cassandra containing counters, and you are SURE that no counters in that table were created in a version earlier than Cassandra 2.1."
        " It is not enough to have ever since upgraded to newer versions of Cassandra. If you EVER used a version earlier than 2.1 in the cluster where these SSTables come from, DO NOT TURN ON THIS OPTION! You will corrupt your data. You have been warned.")
    , enable_shard_aware_drivers(this, "enable_shard_aware_drivers", value_status::Used, true, "Enable native transport drivers to use connection-per-shard for better performance.")
    , enable_ipv6_dns_lookup(this, "enable_ipv6_dns_lookup", value_status::Used, false, "Use IPv6 address resolution")
    , abort_on_internal_error(this, "abort_on_internal_error", liveness::LiveUpdate, value_status::Used, false, "Abort the server instead of throwing exception when internal invariants are violated.")
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
    , reader_concurrency_semaphore_serialize_limit_multiplier(this, "reader_concurrency_semaphore_serialize_limit_multiplier", liveness::LiveUpdate, value_status::Used, 2,
            "Start serializing reads after their collective memory consumption goes above $normal_limit * $multiplier.")
    , reader_concurrency_semaphore_kill_limit_multiplier(this, "reader_concurrency_semaphore_kill_limit_multiplier", liveness::LiveUpdate, value_status::Used, 4,
            "Start killing reads after their collective memory consumption goes above $normal_limit * $multiplier.")
    , reader_concurrency_semaphore_cpu_concurrency(this, "reader_concurrency_semaphore_cpu_concurrency", liveness::LiveUpdate, value_status::Used, 2,
            "Admit new reads while there are less than this number of requests that need CPU.")
    , view_update_reader_concurrency_semaphore_serialize_limit_multiplier(this, "view_update_reader_concurrency_semaphore_serialize_limit_multiplier", liveness::LiveUpdate, value_status::Used, 2,
            "Start serializing view update reads after their collective memory consumption goes above $normal_limit * $multiplier.")
    , view_update_reader_concurrency_semaphore_kill_limit_multiplier(this, "view_update_reader_concurrency_semaphore_kill_limit_multiplier", liveness::LiveUpdate, value_status::Used, 4,
            "Start killing view update reads after their collective memory consumption goes above $normal_limit * $multiplier.")
    , view_update_reader_concurrency_semaphore_cpu_concurrency(this, "view_update_reader_concurrency_semaphore_cpu_concurrency", liveness::LiveUpdate, value_status::Used, 1,
            "Admit new view update reads while there are less than this number of requests that need CPU.")
    , maintenance_reader_concurrency_semaphore_count_limit(this, "maintenance_reader_concurrency_semaphore_count_limit", liveness::LiveUpdate, value_status::Used, 10,
            "Allow up to this many maintenance (e.g. streaming and repair) reads per shard to progress at the same time.")
    , twcs_max_window_count(this, "twcs_max_window_count", liveness::LiveUpdate, value_status::Used, 50,
            "The maximum number of compaction windows allowed when making use of TimeWindowCompactionStrategy. A setting of 0 effectively disables the restriction.")
    , initial_sstable_loading_concurrency(this, "initial_sstable_loading_concurrency", value_status::Used, 4u,
            "Maximum amount of sstables to load in parallel during initialization. A higher number can lead to more memory consumption. You should not need to touch this.")
    , enable_3_1_0_compatibility_mode(this, "enable_3_1_0_compatibility_mode", value_status::Used, false,
        "Set to true if the cluster was initially installed from 3.1.0. If it was upgraded from an earlier version,"
        " or installed from a later version, leave this set to false. This adjusts the communication protocol to"
        " work around a bug in Scylla 3.1.0.")
    , enable_user_defined_functions(this, "enable_user_defined_functions", value_status::Used, false,  "Enable user defined functions. You must also set ``experimental-features=udf``.")
    , user_defined_function_time_limit_ms(this, "user_defined_function_time_limit_ms", value_status::Used, 10, "The time limit for each UDF invocation.")
    , user_defined_function_allocation_limit_bytes(this, "user_defined_function_allocation_limit_bytes", value_status::Used, 1024*1024, "How much memory each UDF invocation can allocate.")
    , user_defined_function_contiguous_allocation_limit_bytes(this, "user_defined_function_contiguous_allocation_limit_bytes", value_status::Used, 1024*1024, "How much memory each UDF invocation can allocate in one chunk.")
    , schema_registry_grace_period(this, "schema_registry_grace_period", value_status::Used, 1,
        "Time period in seconds after which unused schema versions will be evicted from the local schema registry cache. Default is 1 second.")
    , max_concurrent_requests_per_shard(this, "max_concurrent_requests_per_shard", liveness::LiveUpdate, value_status::Used, std::numeric_limits<uint32_t>::max(),
        "Maximum number of concurrent requests a single shard can handle before it starts shedding extra load. By default, no requests will be shed.")
    , cdc_dont_rewrite_streams(this, "cdc_dont_rewrite_streams", value_status::Used, false,
            "Disable rewriting streams from cdc_streams_descriptions to cdc_streams_descriptions_v2. Should not be necessary, but the procedure is expensive and prone to failures; this config option is left as a backdoor in case some user requires manual intervention.")
    , strict_allow_filtering(this, "strict_allow_filtering", liveness::LiveUpdate, value_status::Used, strict_allow_filtering_default(), "Match Cassandra in requiring ALLOW FILTERING on slow queries. Can be true, false, or warn. When false, Scylla accepts some slow queries even without ALLOW FILTERING that Cassandra rejects. Warn is same as false, but with warning.")
    , strict_is_not_null_in_views(this, "strict_is_not_null_in_views", liveness::LiveUpdate, value_status::Used,db::tri_mode_restriction_t::mode::WARN, 
        "In materialized views, restrictions are allowed only on the view's primary key columns.\n"
        "In old versions Scylla mistakenly allowed IS NOT NULL restrictions on columns which were not part of the view's"
        " primary key. These invalid restrictions were ignored.\n"
        "This option controls the behavior when someone tries to create a view with such invalid IS NOT NULL restrictions.\n\n"
        "Can be true, false, or warn:\n"
        " * `true`: IS NOT NULL is allowed only on the view's primary key columns, "
        "trying to use it on other columns will cause an error, as it should.\n"
        " * `false`: Scylla accepts IS NOT NULL restrictions on regular columns, but they're silently ignored. "
        "It's useful for backwards compatibility.\n"
        " * `warn`: The same as false, but there's a warning about invalid view restrictions.\n\n"
        "To preserve backwards compatibility on old clusters, Scylla's default setting is `warn`. "
        "New clusters have this option set to `true` by scylla.yaml (which overrides the default `warn`), "
        "to make sure that trying to create an invalid view causes an error.")
    , enable_cql_config_updates(this, "enable_cql_config_updates", liveness::LiveUpdate, value_status::Used, true,
            "Make the system.config table UPDATEable.")
    , enable_parallelized_aggregation(this, "enable_parallelized_aggregation", liveness::LiveUpdate, value_status::Used, true,
            "Use on a new, parallel algorithm for performing aggregate queries.")
    , cql_duplicate_bind_variable_names_refer_to_same_variable(this, "cql_duplicate_bind_variable_names_refer_to_same_variable", liveness::LiveUpdate, value_status::Used, true,
            "A bind variable that appears twice in a CQL query refers to a single variable (if false, no name matching is performed).")
    , alternator_port(this, "alternator_port", value_status::Used, 0, "Alternator API port.")
    , alternator_https_port(this, "alternator_https_port", value_status::Used, 0, "Alternator API HTTPS port.")
    , alternator_address(this, "alternator_address", value_status::Used, "0.0.0.0", "Alternator API listening address.")
    , alternator_enforce_authorization(this, "alternator_enforce_authorization", value_status::Used, false, "Enforce checking the authorization header for every request in Alternator.")
    , alternator_write_isolation(this, "alternator_write_isolation", value_status::Used, "", "Default write isolation policy for Alternator.")
    , alternator_streams_time_window_s(this, "alternator_streams_time_window_s", value_status::Used, 10, "CDC query confidence window for alternator streams.")
    , alternator_timeout_in_ms(this, "alternator_timeout_in_ms", liveness::LiveUpdate, value_status::Used, 10000,
        "The server-side timeout for completing Alternator API requests.")
    , alternator_ttl_period_in_seconds(this, "alternator_ttl_period_in_seconds", value_status::Used,
        60*60*24,
        "The default period for Alternator's expiration scan. Alternator attempts to scan every table within that period.")
    , alternator_describe_endpoints(this, "alternator_describe_endpoints", liveness::LiveUpdate, value_status::Used,
        "",
        "Overrides the behavior of Alternator's DescribeEndpoints operation. "
        "An empty value (the default) means DescribeEndpoints will return "
        "the same endpoint used in the request. The string 'disabled' "
        "disables the DescribeEndpoints operation. Any other string is the "
        "fixed value that will be returned by DescribeEndpoints operations.")
    , abort_on_ebadf(this, "abort_on_ebadf", value_status::Used, true, "Abort the server on incorrect file descriptor access. Throws exception when disabled.")
    , redis_port(this, "redis_port", value_status::Used, 0, "Port on which the REDIS transport listens for clients.")
    , redis_ssl_port(this, "redis_ssl_port", value_status::Used, 0, "Port on which the REDIS TLS native transport listens for clients.")
    , redis_read_consistency_level(this, "redis_read_consistency_level", value_status::Used, "LOCAL_QUORUM", "Consistency level for read operations for redis.")
    , redis_write_consistency_level(this, "redis_write_consistency_level", value_status::Used, "LOCAL_QUORUM", "Consistency level for write operations for redis.")
    , redis_database_count(this, "redis_database_count", value_status::Used, 16, "Database count for the redis. You can use the default settings (16).")
    , redis_keyspace_replication_strategy_options(this, "redis_keyspace_replication_strategy", value_status::Used, {}, 
        "Set the replication strategy for the redis keyspace. The setting is used by the first node in the boot phase when the keyspace is not exists to create keyspace for redis.\n"
        "The replication strategy determines how many copies of the data are kept in a given data center. This setting impacts consistency, availability and request speed.\n"
        "Two strategies are available: SimpleStrategy and NetworkTopologyStrategy.\n"
        "\n"
        "* class: (Default: SimpleStrategy ). Set the replication strategy for redis keyspace.\n"
        "* 'replication_factor': N, (Default: 'replication_factor':1) IFF the class is SimpleStrategy, assign the same replication factor to the entire cluster.\n"
        "* 'datacenter_name': N [,...], (Default: 'dc1:1') IFF the class is NetworkTopologyStrategy, assign replication factors to each data center in a comma separated list.\n"
        "\n"
        "Related information: About replication strategy.")
    , sanitizer_report_backtrace(this, "sanitizer_report_backtrace", value_status::Used, false,
            "In debug mode, report log-structured allocator sanitizer violations with a backtrace. Slow.")
    , flush_schema_tables_after_modification(this, "flush_schema_tables_after_modification", liveness::LiveUpdate, value_status::Used, true,
        "Flush tables in the system_schema keyspace after schema modification. This is required for crash recovery, but slows down tests and can be disabled for them")
    , restrict_replication_simplestrategy(this, "restrict_replication_simplestrategy", liveness::LiveUpdate, value_status::Deprecated, db::tri_mode_restriction_t::mode::FALSE, "Controls whether to disable SimpleStrategy replication. Can be true, false, or warn.")
    , restrict_dtcs(this, "restrict_dtcs", liveness::LiveUpdate, value_status::Unused, db::tri_mode_restriction_t::mode::TRUE, "Controls whether to prevent setting DateTieredCompactionStrategy. Can be true, false, or warn.")
    , restrict_twcs_without_default_ttl(this, "restrict_twcs_without_default_ttl", liveness::LiveUpdate, value_status::Used, db::tri_mode_restriction_t::mode::WARN, "Controls whether to prevent creating TimeWindowCompactionStrategy tables without a default TTL. Can be true, false, or warn.")
    , restrict_future_timestamp(this, "restrict_future_timestamp",liveness::LiveUpdate, value_status::Used, true, "Controls whether to detect and forbid unreasonable USING TIMESTAMP, more than 3 days into the future.")
    , ignore_truncation_record(this, "unsafe_ignore_truncation_record", value_status::Used, false,
        "Ignore truncation record stored in system tables as if tables were never truncated.")
    , force_schema_commit_log(this, "force_schema_commit_log", value_status::Deprecated, false,
        "Use separate schema commit log unconditionally rater than after restart following discovery of cluster-wide support for it.")
    , task_ttl_seconds(this, "task_ttl_in_seconds", liveness::LiveUpdate, value_status::Used, 0, "Time for which information about finished task started internally stays in memory.")
    , user_task_ttl_seconds(this, "user_task_ttl_in_seconds", liveness::LiveUpdate, value_status::Used, 3600, "Time for which information about finished task started by user stays in memory.")
    , nodeops_watchdog_timeout_seconds(this, "nodeops_watchdog_timeout_seconds", liveness::LiveUpdate, value_status::Used, 120, "Time in seconds after which node operations abort when not hearing from the coordinator.")
    , nodeops_heartbeat_interval_seconds(this, "nodeops_heartbeat_interval_seconds", liveness::LiveUpdate, value_status::Used, 10, "Period of heartbeat ticks in node operations.")
    , cache_index_pages(this, "cache_index_pages", liveness::LiveUpdate, value_status::Used, true,
        "Keep SSTable index pages in the global cache after a SSTable read. Expected to improve performance for workloads with big partitions, but may degrade performance for workloads with small partitions. The amount of memory usable by index cache is limited with ``index_cache_fraction``.")
    , index_cache_fraction(this, "index_cache_fraction", liveness::LiveUpdate, value_status::Used, 0.2,
        "The maximum fraction of cache memory permitted for use by index cache. Clamped to the [0.0; 1.0] range. Must be small enough to not deprive the row cache of memory, but should be big enough to fit a large fraction of the index. The default value 0.2 means that at least 80\% of cache memory is reserved for the row cache, while at most 20\% is usable by the index cache.")
    , consistent_cluster_management(this, "consistent_cluster_management", value_status::Deprecated, true, "Use RAFT for cluster management and DDL.")
    , force_gossip_topology_changes(this, "force_gossip_topology_changes", value_status::Used, false, "Force gossip-based topology operations in a fresh cluster. Only the first node in the cluster must use it. The rest will fall back to gossip-based operations anyway. This option should be used only for testing.  Note: gossip topology changes are incompatible with tablets.")
    , wasm_cache_memory_fraction(this, "wasm_cache_memory_fraction", value_status::Used, 0.01, "Maximum total size of all WASM instances stored in the cache as fraction of total shard memory.")
    , wasm_cache_timeout_in_ms(this, "wasm_cache_timeout_in_ms", value_status::Used, 5000, "Time after which an instance is evicted from the cache.")
    , wasm_cache_instance_size_limit(this, "wasm_cache_instance_size_limit", value_status::Used, 1024*1024, "Instances with size above this limit will not be stored in the cache.")
    , wasm_udf_yield_fuel(this, "wasm_udf_yield_fuel", value_status::Used, 100000, "Wasmtime fuel a WASM UDF can consume before yielding.")
    , wasm_udf_total_fuel(this, "wasm_udf_total_fuel", value_status::Used, 100000000, "Wasmtime fuel a WASM UDF can consume before termination.")
    , wasm_udf_memory_limit(this, "wasm_udf_memory_limit", value_status::Used, 2*1024*1024, "How much memory each WASM UDF can allocate at most.")
    , relabel_config_file(this, "relabel_config_file", value_status::Used, "", "Optionally, read relabel config from file.")
    , object_storage_config_file(this, "object_storage_config_file", value_status::Used, "", "Optionally, read object-storage endpoints config from file.")
    , live_updatable_config_params_changeable_via_cql(this, "live_updatable_config_params_changeable_via_cql", liveness::MustRestart, value_status::Used, true, "If set to true, configuration parameters defined with LiveUpdate can be updated in runtime via CQL (by updating system.config virtual table), otherwise they can't.")
    , auth_superuser_name(this, "auth_superuser_name", value_status::Used, "",
        "Initial authentication super username. Ignored if authentication tables already contain a super user.")
    , auth_superuser_salted_password(this, "auth_superuser_salted_password", value_status::Used, "", 
        "Initial authentication super user salted password. Create using mkpassword or similar. The hashing algorithm used must be available on the node host. "
        "Ignored if authentication tables already contain a super user password.")
    , auth_certificate_role_queries(this, "auth_certificate_role_queries", value_status::Used, { { { "source", "SUBJECT" }, {"query", "CN=([^,]+)" } } },
        "Regular expression used by CertificateAuthenticator to extract role name from an accepted transport authentication certificate subject info.")
    , minimum_replication_factor_fail_threshold(this, "minimum_replication_factor_fail_threshold", liveness::LiveUpdate, value_status::Used, -1, "")
    , minimum_replication_factor_warn_threshold(this, "minimum_replication_factor_warn_threshold", liveness::LiveUpdate, value_status::Used,  3, "")
    , maximum_replication_factor_warn_threshold(this, "maximum_replication_factor_warn_threshold", liveness::LiveUpdate, value_status::Used, -1, "")
    , maximum_replication_factor_fail_threshold(this, "maximum_replication_factor_fail_threshold", liveness::LiveUpdate, value_status::Used, -1, "")
    , tablets_initial_scale_factor(this, "tablets_initial_scale_factor", value_status::Used, 1, "Calculated initial tablets are multiplied by this number")
    , target_tablet_size_in_bytes(this, "target_tablet_size_in_bytes", liveness::LiveUpdate, value_status::Used, service::default_target_tablet_size,
         "Allows target tablet size to be configured. Defaults to 5G (in bytes). Maintaining tablets at reasonable sizes is important to be able to " \
         "redistribute load. A higher value means tablet migration throughput can be reduced. A lower value may cause number of tablets to increase significantly, " \
         "potentially resulting in performance drawbacks.")
    , replication_strategy_warn_list(this, "replication_strategy_warn_list", liveness::LiveUpdate, value_status::Used, {locator::replication_strategy_type::simple}, "Controls which replication strategies to warn about when creating/altering a keyspace. Doesn't affect the pre-existing keyspaces.")
    , replication_strategy_fail_list(this, "replication_strategy_fail_list", liveness::LiveUpdate, value_status::Used, {}, "Controls which replication strategies are disallowed to be used when creating/altering a keyspace. Doesn't affect the pre-existing keyspaces.")
    , service_levels_interval(this, "service_levels_interval_ms", liveness::LiveUpdate, value_status::Used, 10000, "Controls how often service levels module polls configuration table")

    , audit(this, "audit", value_status::Used, "none",
        "Controls the audit feature:\n"
        "\n"
        "\tnone   : No auditing enabled.\n"
        "\tsyslog : Audit messages sent to Syslog.\n"
        "\ttable  : Audit messages written to column family named audit.audit_log.\n")
    , audit_categories(this, "audit_categories", value_status::Used, "DCL,DDL,AUTH", "Comma separated list of operation categories that should be audited.")
    , audit_tables(this, "audit_tables", value_status::Used, "", "Comma separated list of table names (<keyspace>.<table>) that will be audited.")
    , audit_keyspaces(this, "audit_keyspaces", value_status::Used, "", "Comma separated list of keyspaces that will be audited. All tables in those keyspaces will be audited")
    , audit_unix_socket_path(this, "audit_unix_socket_path", value_status::Used, "/dev/log", "The path to the unix socket used for writting to syslog. Only applicable when audit is set to syslog.")
    , audit_syslog_write_buffer_size(this, "audit_syslog_write_buffer_size", value_status::Used, 1048576, "The size (in bytes) of a write buffer used when writting to syslog socket.")
    , ldap_url_template(this, "ldap_url_template", value_status::Used, "", "LDAP URL template used by LDAPRoleManager for crafting queries.")
    , ldap_attr_role(this, "ldap_attr_role", value_status::Used, "", "LDAP attribute containing Scylla role.")
    , ldap_bind_dn(this, "ldap_bind_dn", value_status::Used, "", "Distinguished name used by LDAPRoleManager for binding to LDAP server.")
    , ldap_bind_passwd(this, "ldap_bind_passwd", value_status::Used, "", "Password used by LDAPRoleManager for binding to LDAP server.")
    , saslauthd_socket_path(this, "saslauthd_socket_path", value_status::Used, "", "UNIX domain socket on which saslauthd is listening.")

    , error_injections_at_startup(this, "error_injections_at_startup", error_injection_value_status, {}, "List of error injections that should be enabled on startup.")
    , topology_barrier_stall_detector_threshold_seconds(this, "topology_barrier_stall_detector_threshold_seconds", value_status::Used, 2, "Report sites blocking topology barrier if it takes longer than this.")
    , enable_tablets(this, "enable_tablets", value_status::Used, false, "Enable tablets for newly created keyspaces.")
    , view_flow_control_delay_limit_in_ms(this, "view_flow_control_delay_limit_in_ms", liveness::LiveUpdate, value_status::Used, 1000,
        "The maximal amount of time that materialized-view update flow control may delay responses "
        "to try to slow down the client and prevent buildup of unfinished view updates. "
        "To be effective, this maximal delay should be larger than the typical latencies. "
        "Setting view_flow_control_delay_limit_in_ms to 0 disables view-update flow control.")
    , disk_space_monitor_normal_polling_interval_in_seconds(this, "disk_space_monitor_normal_polling_interval_in_seconds", value_status::Used, 10, "Disk-space polling interval while below polling threshold")
    , disk_space_monitor_high_polling_interval_in_seconds(this, "disk_space_monitor_high_polling_interval_in_seconds", value_status::Used, 1, "Disk-space polling interval at or above polling threshold")
    , disk_space_monitor_polling_interval_threshold(this, "disk_space_monitor_polling_interval_threshold", value_status::Used, 0.9, "Disk-space polling threshold. Polling interval is increased when disk utilization is greater than or equal to this threshold")
    , enable_create_table_with_compact_storage(this, "enable_create_table_with_compact_storage", liveness::LiveUpdate, value_status::Used, false, "Enable the deprecated feature of CREATE TABLE WITH COMPACT STORAGE.  This feature will eventually be removed in a future version.")
    , default_log_level(this, "default_log_level", value_status::Used, seastar::log_level::info, "Default log level for log messages")
    , logger_log_level(this, "logger_log_level", value_status::Used, {}, "Map of logger name to log level. Valid log levels are 'error', 'warn', 'info', 'debug' and 'trace'")
    , log_to_stdout(this, "log_to_stdout", value_status::Used, true, "Send log output to stdout")
    , log_to_syslog(this, "log_to_syslog", value_status::Used, false, "Send log output to syslog")
    , _extensions(std::move(exts))
{
    add_tombstone_gc_extension();
}

db::config::config()
    : config(std::make_shared<db::extensions>())
{}

db::config::~config()
{}

void db::config::add_cdc_extension() {
    _extensions->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
}

void db::config::add_per_partition_rate_limit_extension() {
    _extensions->add_schema_extension<db::per_partition_rate_limit_extension>(db::per_partition_rate_limit_extension::NAME);
}

void db::config::add_tags_extension() {
    _extensions->add_schema_extension<db::tags_extension>(db::tags_extension::NAME);
}

void db::config::add_tombstone_gc_extension() {
    _extensions->add_schema_extension<tombstone_gc_extension>(tombstone_gc_extension::NAME);
}

void db::config::setup_directories() {
    maybe_in_workdir(commitlog_directory, "commitlog");
    if (!schema_commitlog_directory.is_set()) {
        schema_commitlog_directory(commitlog_directory() + "/schema");
    }
    maybe_in_workdir(schema_commitlog_directory, "schema_commitlog");
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

template <>
struct fmt::formatter<db::seed_provider_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const db::seed_provider_type& s, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "seed_provider_type{{class={}, params={}}}",
                              s.class_name, s.parameters);
    }
};

namespace db {

std::istream& operator>>(std::istream& is, db::seed_provider_type& s) {
    // FIXME -- this operator is used, in particular, by boost lexical_cast<>
    // it's here just to make the code compile, but it's not yet called for real
    throw std::runtime_error("reading seed_provider_type from istream is not implemented");
    return is;
}

std::istream& operator>>(std::istream& is, error_injection_at_startup& eias) {
    eias = error_injection_at_startup();
    is >> eias.name;
    return is;
}

}

auto fmt::formatter<db::error_injection_at_startup>::format(const db::error_injection_at_startup& eias, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "error_injection_at_startup{{name={}, one_short={}, parameters={}}}",
                          eias.name, eias.one_shot, eias.parameters);
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
    enum_option<experimental_features_t> to_check{f};
    return std::ranges::any_of(experimental_features(),
                               [to_check](auto& enabled) {
                                   return to_check == enabled;
                               });
}

logging::settings db::config::logging_settings(const log_cli::options& opts) const {
    auto value = [&](auto& v, const auto& opt) {
        if (opt.defaulted() && v.is_set()) {
            return v();
        }
        return opt.get_value();
    };

    return logging::settings{
        .logger_levels = value(logger_log_level, opts.logger_log_level),
        .default_level = value(default_log_level, opts.default_log_level),
        .stdout_enabled = value(log_to_stdout, opts.log_to_stdout),
        .syslog_enabled = value(log_to_syslog, opts.log_to_syslog),
        .with_color = opts.log_with_color.get_value(),
        .stdout_timestamp_style =  opts.logger_stdout_timestamps.get_value(),
        .logger_ostream = opts.logger_ostream_type.get_value(),
    };
}

const db::extensions& db::config::extensions() const {
    return *_extensions;
}

std::map<sstring, db::experimental_features_t::feature> db::experimental_features_t::map() {
    // We decided against using the construct-on-first-use idiom here:
    // https://github.com/scylladb/scylla/pull/5369#discussion_r353614807
    // Features which are no longer experimental are mapped
    // to UNUSED switch for a while, and can be eventually
    // removed altogether.
    return {
        {"lwt", feature::UNUSED},
        {"udf", feature::UDF},
        {"cdc", feature::UNUSED},
        {"alternator-streams", feature::ALTERNATOR_STREAMS},
        {"alternator-ttl", feature::UNUSED },
        {"consistent-topology-changes", feature::UNUSED},
        {"broadcast-tables", feature::BROADCAST_TABLES},
        {"keyspace-storage-options", feature::KEYSPACE_STORAGE_OPTIONS},
        {"tablets", feature::UNUSED},
        {"views-with-tablets", feature::VIEWS_WITH_TABLETS}
    };
}

std::unordered_map<sstring, locator::replication_strategy_type> db::replication_strategy_restriction_t::map() {
    return {{"SimpleStrategy", locator::replication_strategy_type::simple},
            {"LocalStrategy", locator::replication_strategy_type::local},
            {"NetworkTopologyStrategy", locator::replication_strategy_type::network_topology},
            {"EverywhereStrategy", locator::replication_strategy_type::everywhere_topology}};
}

std::vector<enum_option<db::experimental_features_t>> db::experimental_features_t::all() {
    std::vector<enum_option<db::experimental_features_t>> ret;
    for (const auto& f : db::experimental_features_t::map()) {
        if (f.second != db::experimental_features_t::feature::UNUSED) {
            ret.push_back(f.second);
        }
    }
    return ret;
}

std::unordered_map<sstring, db::tri_mode_restriction_t::mode> db::tri_mode_restriction_t::map() {
    return {{"true", db::tri_mode_restriction_t::mode::TRUE},
            {"1", db::tri_mode_restriction_t::mode::TRUE},
            {"false", db::tri_mode_restriction_t::mode::FALSE},
            {"0", db::tri_mode_restriction_t::mode::FALSE},
            {"warn", db::tri_mode_restriction_t::mode::WARN}};
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

future<> configure_tls_creds_builder(seastar::tls::credentials_builder& creds, db::config::string_map options) {
    creds.set_dh_level(seastar::tls::dh_params::level::MEDIUM);
    creds.set_priority_string(db::config::default_tls_priority);

    if (options.contains("priority_string")) {
        creds.set_priority_string(options.at("priority_string"));
    }
    if (is_true(get_or_default(options, "require_client_auth", "false"))) {
        creds.set_client_auth(seastar::tls::client_auth::REQUIRE);
    }

    auto cert = get_or_default(options, "certificate", db::config::get_conf_sub("scylla.crt").string());
    auto key = get_or_default(options, "keyfile", db::config::get_conf_sub("scylla.key").string());
    co_await creds.set_x509_key_file(cert, key, seastar::tls::x509_crt_format::PEM);

    if (options.contains("truststore")) {
        co_await creds.set_x509_trust_file(options.at("truststore"), seastar::tls::x509_crt_format::PEM);
    }
    if (options.contains("certficate_revocation_list")) {
        co_await creds.set_x509_crl_file(options.at("certficate_revocation_list"), seastar::tls::x509_crt_format::PEM);
    }
}

future<gms::inet_address> resolve(const config_file::named_value<sstring>& address, gms::inet_address::opt_family family, gms::inet_address::opt_family preferred) {
    std::exception_ptr ex;
    try {
        co_return co_await gms::inet_address::lookup(address(), family, preferred);
    } catch (...) {
        try {
            std::throw_with_nested(std::runtime_error(fmt::format("Couldn't resolve {}", address.name())));
        } catch (...) {
            ex = std::current_exception();
        }
    }

    co_return coroutine::exception(std::move(ex));
}

static std::vector<seastar::metrics::relabel_config> get_relable_from_yaml(const YAML::Node& yaml, const std::string& name) {
    std::vector<seastar::metrics::relabel_config> relabels;
    const YAML::Node& relabel_configs = yaml["relabel_configs"];
    relabels.resize(relabel_configs.size());
    size_t i = 0;
    for (auto it = relabel_configs.begin(); it != relabel_configs.end(); ++it, i++) {
        const YAML::Node& element = *it;
        for(YAML::const_iterator e_it = element.begin(); e_it != element.end(); ++e_it) {
            std::string key = e_it->first.as<std::string>();
            if (key == "source_labels") {
                auto labels = e_it->second;
                std::vector<std::string> source_labels;
                source_labels.resize(labels.size());
                size_t j = 0;
                for (auto label_it = labels.begin(); label_it !=  labels.end(); ++label_it, j++) {
                    source_labels[j] = label_it->as<std::string>();
                }
                relabels[i].source_labels = source_labels;
            } else if (key == "action") {
                relabels[i].action = seastar::metrics::relabel_config_action(e_it->second.as<std::string>());
            } else if (key == "replacement") {
                relabels[i].replacement = e_it->second.as<std::string>();
            } else if (key == "target_label") {
                relabels[i].target_label = e_it->second.as<std::string>();
            } else if (key == "separator") {
                relabels[i].separator = e_it->second.as<std::string>();
            } else if (key == "regex") {
                relabels[i].expr = e_it->second.as<std::string>();
            } else {
                throw std::runtime_error("unknown entry '" + key + "' in file " + name);
            }
        }
    }
    return relabels;
}

static std::vector<seastar::metrics::metric_family_config> get_metric_configs_from_yaml(const YAML::Node& yaml, const std::string& name) {
    std::vector<seastar::metrics::metric_family_config> metric_config;
    const YAML::Node& metric_family_configs = yaml["metric_family_configs"];
    metric_config.resize(metric_family_configs.size());
    size_t i = 0;
    for (auto it = metric_family_configs.begin(); it != metric_family_configs.end(); ++it, i++) {
        const YAML::Node& element = *it;
        for(YAML::const_iterator e_it = element.begin(); e_it != element.end(); ++e_it) {
            std::string key = e_it->first.as<std::string>();
            if (key == "aggregate_labels") {
                auto labels = e_it->second;
                std::vector<std::string> aggregate_labels;
                aggregate_labels.resize(labels.size());
                size_t j = 0;
                for (auto label_it = labels.begin(); label_it !=  labels.end(); ++label_it, j++) {
                    aggregate_labels[j] = label_it->as<std::string>();
                }
                metric_config[i].aggregate_labels = aggregate_labels;
            } else if (key == "name") {
                metric_config[i].name = e_it->second.as<std::string>();
            } else if (key == "regex") {
                metric_config[i].regex_name = e_it->second.as<std::string>();
            }
        }
    }
    return metric_config;
}

future<> update_relabel_config_from_file(const std::string& name) {
    if (name.empty()) {
        co_return;
    }
    file f = co_await seastar::open_file_dma(name, open_flags::ro);
    size_t s = co_await f.size();
    seastar::input_stream<char> in = seastar::make_file_input_stream(f);
    temporary_buffer<char> buf = co_await in.read_exactly(s);
    auto yaml = YAML::Load(sstring(buf.begin(), buf.end()));
    std::vector<seastar::metrics::relabel_config> relabels = get_relable_from_yaml(yaml, name);
    std::vector<seastar::metrics::metric_family_config> metric_configs = get_metric_configs_from_yaml(yaml, name);
    bool failed = false;
    co_await smp::invoke_on_all([&relabels, &failed, &metric_configs] {
        metrics::set_metric_family_configs(metric_configs);
        return metrics::set_relabel_configs(relabels).then([&failed](const metrics::metric_relabeling_result& result) {
            if (result.metrics_relabeled_due_to_collision > 0) {
                failed = true;
            }
            return;
        });
    });
    if (failed) {
        throw std::runtime_error("conflicts found during relabeling");
    }
    co_return;
}

std::vector<sstring> split_comma_separated_list(const std::string_view comma_separated_list) {
    std::vector<sstring> strs, trimmed_strs;
    boost::split(strs, comma_separated_list, boost::is_any_of(","));
    trimmed_strs.reserve(strs.size());
    for (sstring& n : strs) {
        std::replace(n.begin(), n.end(), '\"', ' ');
        std::replace(n.begin(), n.end(), '\'', ' ');
        boost::trim_all(n);
        if (!n.empty()) {
            trimmed_strs.push_back(std::move(n));
        }
    }
    return trimmed_strs;
}

} // namespace utils
