/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/program_options.hpp>
#include <unordered_map>

#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/program-options.hh>
#include <seastar/util/log.hh>

#include "locator/abstract_replication_strategy.hh"
#include "seastarx.hh"
#include "utils/config_file.hh"
#include "utils/enum_option.hh"
#include "locator/host_id.hh"
#include "gms/inet_address.hh"
#include "db/hints/host_filter.hh"
#include "utils/updateable_value.hh"
#include "utils/s3/creds.hh"
#include "utils/error_injection.hh"

namespace seastar {
class file;
struct logging_settings;
namespace tls {
class credentials_builder;
}
namespace log_cli {
class options;
}
}

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
    bool operator==(const seed_provider_type& other) const {
        return class_name == other.class_name && parameters == other.parameters;
    }
};

inline std::istream& operator>>(std::istream& is, seed_provider_type&);

// Describes a single error injection that should be enabled at startup.
struct error_injection_at_startup {
    sstring name;
    bool one_shot = false;
    utils::error_injection_parameters parameters;

    bool operator==(const error_injection_at_startup& other) const {
        return name == other.name
            && one_shot == other.one_shot
            && parameters == other.parameters;
    }
};

std::istream& operator>>(std::istream& is, error_injection_at_startup&);

}

template<>
struct fmt::formatter<db::error_injection_at_startup> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const db::error_injection_at_startup&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

namespace utils {

sstring config_value_as_json(const db::seed_provider_type& v);

sstring config_value_as_json(const log_level& v);

sstring config_value_as_json(const std::unordered_map<sstring, log_level>& v);

}

namespace db {

/// Enumeration of all valid values for the `experimental` config entry.
struct experimental_features_t {
    enum class feature {
        UNUSED,
        UDF,
        ALTERNATOR_STREAMS,
        BROADCAST_TABLES,
        KEYSPACE_STORAGE_OPTIONS,
    };
    static std::map<sstring, feature> map(); // See enum_option.
    static std::vector<enum_option<experimental_features_t>> all();
};

/// A restriction that can be in three modes: true (the operation is disabled),
/// false (the operation is allowed), or warn (the operation is allowed but
/// produces a warning in the log).
struct tri_mode_restriction_t {
    enum class mode { FALSE, TRUE, WARN };
    static std::unordered_map<sstring, mode> map(); // for enum_option<>
};
using tri_mode_restriction = enum_option<tri_mode_restriction_t>;

struct replication_strategy_restriction_t {
    static std::unordered_map<sstring, locator::replication_strategy_type> map(); // for enum_option<>
};

constexpr unsigned default_murmur3_partitioner_ignore_msb_bits = 12;

class config final : public utils::config_file {
public:
    config();
    config(std::shared_ptr<db::extensions>);
    ~config();

    // For testing only
    void add_cdc_extension();
    void add_per_partition_rate_limit_extension();
    void add_tags_extension();
    void add_tombstone_gc_extension();

    /// True iff the feature is enabled.
    bool check_experimental(experimental_features_t::feature f) const;

    void setup_directories();

    /**
     * Scans the environment variables for configuration files directory
     * definition. It's either $SCYLLA_CONF, $SCYLLA_HOME/conf or "conf" if none
     * of SCYLLA_CONF and SCYLLA_HOME is defined.
     *
     * @return path of the directory where configuration files are located
     *         according the environment variables definitions.
     */
    static fs::path get_conf_dir();
    static fs::path get_conf_sub(fs::path);

    using string_map = std::unordered_map<sstring, sstring>;
                    //program_options::string_map;
    using string_list = std::vector<sstring>;
    using seed_provider_type = db::seed_provider_type;
    using hinted_handoff_enabled_type = db::hints::host_filter;
    using error_injection_at_startup = db::error_injection_at_startup;

    /*
     * All values and documentation taken from
     * http://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html
     */
    named_value<double> background_writer_scheduling_quota;
    named_value<bool> auto_adjust_flush_quota;
    named_value<float> memtable_flush_static_shares;
    named_value<float> compaction_static_shares;
    named_value<bool> compaction_enforce_min_threshold;
    named_value<uint32_t> compaction_flush_all_tables_before_major_seconds;
    named_value<sstring> cluster_name;
    named_value<sstring> listen_address;
    named_value<sstring> listen_interface;
    named_value<bool> listen_interface_prefer_ipv6;
    named_value<sstring> work_directory;
    named_value<sstring> commitlog_directory;
    named_value<sstring> schema_commitlog_directory;
    named_value<string_list> data_file_directories;
    named_value<sstring> hints_directory;
    named_value<sstring> view_hints_directory;
    named_value<sstring> saved_caches_directory;
    named_value<sstring> commit_failure_policy;
    named_value<sstring> disk_failure_policy;
    named_value<sstring> endpoint_snitch;
    named_value<sstring> rpc_address;
    named_value<sstring> rpc_interface;
    named_value<bool> rpc_interface_prefer_ipv6;
    named_value<seed_provider_type> seed_provider;
    named_value<uint32_t> compaction_throughput_mb_per_sec;
    named_value<uint32_t> compaction_large_partition_warning_threshold_mb;
    named_value<uint32_t> compaction_large_row_warning_threshold_mb;
    named_value<uint32_t> compaction_large_cell_warning_threshold_mb;
    named_value<uint32_t> compaction_rows_count_warning_threshold;
    named_value<uint32_t> compaction_collection_elements_count_warning_threshold;
    named_value<uint32_t> memtable_total_space_in_mb;
    named_value<uint32_t> concurrent_reads;
    named_value<uint32_t> concurrent_writes;
    named_value<uint32_t> concurrent_counter_writes;
    named_value<bool> incremental_backups;
    named_value<bool> snapshot_before_compaction;
    named_value<uint32_t> phi_convict_threshold;
    named_value<uint32_t> failure_detector_timeout_in_ms;
    named_value<uint32_t> direct_failure_detector_ping_timeout_in_ms;
    named_value<sstring> commitlog_sync;
    named_value<uint32_t> commitlog_segment_size_in_mb;
    named_value<uint32_t> schema_commitlog_segment_size_in_mb;
    named_value<uint32_t> commitlog_sync_period_in_ms;
    named_value<uint32_t> commitlog_sync_batch_window_in_ms;
    named_value<uint32_t> commitlog_max_data_lifetime_in_seconds;
    named_value<int64_t> commitlog_total_space_in_mb;
    named_value<bool> commitlog_reuse_segments; // unused. retained for upgrade compat
    named_value<int64_t> commitlog_flush_threshold_in_mb;
    named_value<bool> commitlog_use_o_dsync;
    named_value<bool> commitlog_use_hard_size_limit;
    named_value<bool> compaction_preheat_key_cache;
    named_value<uint32_t> concurrent_compactors;
    named_value<uint32_t> in_memory_compaction_limit_in_mb;
    named_value<bool> preheat_kernel_page_cache;
    named_value<uint32_t> sstable_preemptive_open_interval_in_mb;
    named_value<bool> defragment_memory_on_idle;
    named_value<sstring> memtable_allocation_type;
    named_value<double> memtable_cleanup_threshold;
    named_value<uint32_t> file_cache_size_in_mb;
    named_value<uint32_t> memtable_flush_queue_size;
    named_value<uint32_t> memtable_flush_writers;
    named_value<uint32_t> memtable_heap_space_in_mb;
    named_value<uint32_t> memtable_offheap_space_in_mb;
    named_value<uint32_t> column_index_size_in_kb;
    named_value<uint32_t> column_index_auto_scale_threshold_in_kb;
    named_value<uint32_t> index_summary_capacity_in_mb;
    named_value<uint32_t> index_summary_resize_interval_in_minutes;
    named_value<double> reduce_cache_capacity_to;
    named_value<double> reduce_cache_sizes_at;
    named_value<uint32_t> stream_throughput_outbound_megabits_per_sec;
    named_value<uint32_t> inter_dc_stream_throughput_outbound_megabits_per_sec;
    named_value<uint32_t> stream_io_throughput_mb_per_sec;
    named_value<double> stream_plan_ranges_fraction;
    named_value<bool> trickle_fsync;
    named_value<uint32_t> trickle_fsync_interval_in_kb;
    named_value<bool> auto_bootstrap;
    named_value<uint32_t> batch_size_warn_threshold_in_kb;
    named_value<uint32_t> batch_size_fail_threshold_in_kb;
    named_value<sstring> broadcast_address;
    named_value<bool> listen_on_broadcast_address;
    named_value<sstring> initial_token;
    named_value<uint32_t> num_tokens;
    named_value<sstring> partitioner;
    named_value<uint16_t> storage_port;
    named_value<bool> auto_snapshot;
    named_value<uint32_t> key_cache_keys_to_save;
    named_value<uint32_t> key_cache_save_period;
    named_value<uint32_t> key_cache_size_in_mb;
    named_value<uint32_t> row_cache_keys_to_save;
    named_value<uint32_t> row_cache_size_in_mb;
    named_value<uint32_t> row_cache_save_period;
    named_value<sstring> memory_allocator;
    named_value<uint32_t> counter_cache_size_in_mb;
    named_value<uint32_t> counter_cache_save_period;
    named_value<uint32_t> counter_cache_keys_to_save;
    named_value<uint32_t> tombstone_warn_threshold;
    named_value<uint32_t> tombstone_failure_threshold;
    named_value<uint64_t> query_tombstone_page_limit;
    named_value<uint64_t> query_page_size_in_bytes;
    named_value<uint32_t> range_request_timeout_in_ms;
    named_value<uint32_t> read_request_timeout_in_ms;
    named_value<uint32_t> counter_write_request_timeout_in_ms;
    named_value<uint32_t> cas_contention_timeout_in_ms;
    named_value<uint32_t> truncate_request_timeout_in_ms;
    named_value<uint32_t> write_request_timeout_in_ms;
    named_value<uint32_t> request_timeout_in_ms;
    named_value<bool> cross_node_timeout;
    named_value<uint32_t> internode_send_buff_size_in_bytes;
    named_value<uint32_t> internode_recv_buff_size_in_bytes;
    named_value<sstring> internode_compression;
    named_value<bool> inter_dc_tcp_nodelay;
    named_value<uint32_t> streaming_socket_timeout_in_ms;
    named_value<bool> start_native_transport;
    named_value<uint16_t> native_transport_port;
    named_value<sstring> maintenance_socket;
    named_value<sstring> maintenance_socket_group;
    named_value<bool> maintenance_mode;
    named_value<uint16_t> native_transport_port_ssl;
    named_value<uint16_t> native_shard_aware_transport_port;
    named_value<uint16_t> native_shard_aware_transport_port_ssl;
    named_value<uint32_t> native_transport_max_threads;
    named_value<uint32_t> native_transport_max_frame_size_in_mb;
    named_value<sstring> broadcast_rpc_address;
    named_value<uint16_t> rpc_port;
    named_value<bool> start_rpc;
    named_value<bool> rpc_keepalive;
    named_value<bool> cache_hit_rate_read_balancing;
    named_value<double> dynamic_snitch_badness_threshold;
    named_value<uint32_t> dynamic_snitch_reset_interval_in_ms;
    named_value<uint32_t> dynamic_snitch_update_interval_in_ms;
    named_value<hinted_handoff_enabled_type> hinted_handoff_enabled;
    named_value<uint32_t> max_hinted_handoff_concurrency;
    named_value<uint32_t> hinted_handoff_throttle_in_kb;
    named_value<uint32_t> max_hint_window_in_ms;
    named_value<uint32_t> max_hints_delivery_threads;
    named_value<uint32_t> batchlog_replay_throttle_in_kb;
    named_value<sstring> request_scheduler;
    named_value<sstring> request_scheduler_id;
    named_value<string_map> request_scheduler_options;
    named_value<sstring> authenticator;
    named_value<sstring> internode_authenticator;
    named_value<sstring> authorizer;
    named_value<sstring> role_manager;
    named_value<uint32_t> permissions_validity_in_ms;
    named_value<uint32_t> permissions_update_interval_in_ms;
    named_value<uint32_t> permissions_cache_max_entries;
    named_value<string_map> server_encryption_options;
    named_value<string_map> client_encryption_options;
    named_value<string_map> alternator_encryption_options;
    named_value<uint32_t> ssl_storage_port;
    named_value<bool> enable_in_memory_data_store;
    named_value<bool> enable_cache;
    named_value<bool> enable_commitlog;
    named_value<bool> volatile_system_keyspace_for_testing;
    named_value<uint16_t> api_port;
    named_value<sstring> api_address;
    named_value<sstring> api_ui_dir;
    named_value<sstring> api_doc_dir;
    named_value<sstring> load_balance;
    named_value<bool> consistent_rangemovement;
    named_value<bool> join_ring;
    named_value<bool> load_ring_state;
    named_value<sstring> replace_node_first_boot;
    named_value<sstring> replace_address;
    named_value<sstring> replace_address_first_boot;
    named_value<sstring> ignore_dead_nodes_for_replace;
    named_value<bool> override_decommission;
    named_value<bool> enable_repair_based_node_ops;
    named_value<sstring> allowed_repair_based_node_ops;
    named_value<bool> enable_compacting_data_for_streaming_and_repair;
    named_value<bool> enable_tombstone_gc_for_streaming_and_repair;
    named_value<double> repair_partition_count_estimation_ratio;
    named_value<uint32_t> ring_delay_ms;
    named_value<uint32_t> shadow_round_ms;
    named_value<uint32_t> fd_max_interval_ms;
    named_value<uint32_t> fd_initial_value_ms;
    named_value<uint32_t> shutdown_announce_in_ms;
    named_value<bool> developer_mode;
    named_value<int32_t> skip_wait_for_gossip_to_settle;
    named_value<int32_t> force_gossip_generation;
    named_value<std::vector<enum_option<experimental_features_t>>> experimental_features;
    named_value<size_t> lsa_reclamation_step;
    named_value<uint16_t> prometheus_port;
    named_value<sstring> prometheus_address;
    named_value<sstring> prometheus_prefix;
    named_value<bool> prometheus_allow_protobuf;
    named_value<bool> abort_on_lsa_bad_alloc;
    named_value<unsigned> murmur3_partitioner_ignore_msb_bits;
    named_value<double> unspooled_dirty_soft_limit;
    named_value<double> sstable_summary_ratio;
    named_value<double> components_memory_reclaim_threshold;
    named_value<size_t> large_memory_allocation_warning_threshold;
    named_value<bool> enable_deprecated_partitioners;
    named_value<bool> enable_keyspace_column_family_metrics;
    named_value<bool> enable_node_aggregated_table_metrics;
    named_value<bool> enable_sstable_data_integrity_check;
    named_value<bool> enable_sstable_key_validation;
    named_value<bool> cpu_scheduler;
    named_value<bool> view_building;
    named_value<bool> enable_sstables_mc_format;
    named_value<bool> enable_sstables_md_format;
    named_value<sstring> sstable_format;
    named_value<bool> uuid_sstable_identifiers_enabled;
    named_value<bool> table_digest_insensitive_to_expiry;
    named_value<bool> enable_dangerous_direct_import_of_cassandra_counters;
    named_value<bool> enable_shard_aware_drivers;
    named_value<bool> enable_ipv6_dns_lookup;
    named_value<bool> abort_on_internal_error;
    named_value<uint32_t> max_partition_key_restrictions_per_query;
    named_value<uint32_t> max_clustering_key_restrictions_per_query;
    named_value<uint64_t> max_memory_for_unlimited_query_soft_limit;
    named_value<uint64_t> max_memory_for_unlimited_query_hard_limit;
    named_value<uint32_t> reader_concurrency_semaphore_serialize_limit_multiplier;
    named_value<uint32_t> reader_concurrency_semaphore_kill_limit_multiplier;
    named_value<uint32_t> reader_concurrency_semaphore_cpu_concurrency;
    named_value<int> maintenance_reader_concurrency_semaphore_count_limit;
    named_value<uint32_t> twcs_max_window_count;
    named_value<unsigned> initial_sstable_loading_concurrency;
    named_value<bool> enable_3_1_0_compatibility_mode;
    named_value<bool> enable_user_defined_functions;
    named_value<unsigned> user_defined_function_time_limit_ms;
    named_value<unsigned> user_defined_function_allocation_limit_bytes;
    named_value<unsigned> user_defined_function_contiguous_allocation_limit_bytes;
    named_value<uint32_t> schema_registry_grace_period;
    named_value<uint32_t> max_concurrent_requests_per_shard;
    named_value<bool> cdc_dont_rewrite_streams;
    named_value<tri_mode_restriction> strict_allow_filtering;
    named_value<tri_mode_restriction> strict_is_not_null_in_views;
    named_value<bool> enable_cql_config_updates;
    named_value<bool> enable_parallelized_aggregation;

    named_value<uint16_t> alternator_port;
    named_value<uint16_t> alternator_https_port;
    named_value<sstring> alternator_address;
    named_value<bool> alternator_enforce_authorization;
    named_value<sstring> alternator_write_isolation;
    named_value<uint32_t> alternator_streams_time_window_s;
    named_value<uint32_t> alternator_timeout_in_ms;
    named_value<double> alternator_ttl_period_in_seconds;
    named_value<sstring> alternator_describe_endpoints;

    named_value<bool> abort_on_ebadf;

    named_value<uint16_t> redis_port;
    named_value<uint16_t> redis_ssl_port;
    named_value<sstring> redis_read_consistency_level;
    named_value<sstring> redis_write_consistency_level;
    named_value<uint16_t> redis_database_count;
    named_value<string_map> redis_keyspace_replication_strategy_options;

    named_value<bool> sanitizer_report_backtrace;
    named_value<bool> flush_schema_tables_after_modification;

    // Options to restrict (forbid, warn or somehow limit) certain operations
    // or options which non-expert users are more likely to regret than to
    // enjoy:
    named_value<tri_mode_restriction> restrict_replication_simplestrategy;
    named_value<tri_mode_restriction> restrict_dtcs;
    named_value<tri_mode_restriction> restrict_twcs_without_default_ttl;
    named_value<bool> restrict_future_timestamp;

    named_value<bool> ignore_truncation_record;
    named_value<bool> force_schema_commit_log;

    named_value<uint32_t> task_ttl_seconds;
    named_value<uint32_t> nodeops_watchdog_timeout_seconds;
    named_value<uint32_t> nodeops_heartbeat_interval_seconds;

    named_value<bool> cache_index_pages;
    named_value<double> index_cache_fraction;

    named_value<bool> consistent_cluster_management;
    named_value<bool> force_gossip_topology_changes;

    named_value<double> wasm_cache_memory_fraction;
    named_value<uint32_t> wasm_cache_timeout_in_ms;
    named_value<size_t> wasm_cache_instance_size_limit;
    named_value<uint64_t> wasm_udf_yield_fuel;
    named_value<uint64_t> wasm_udf_total_fuel;
    named_value<size_t> wasm_udf_memory_limit;
    named_value<sstring> relabel_config_file;
    named_value<sstring> object_storage_config_file;
    // wasm_udf_reserved_memory is static because the options in db::config
    // are parsed using seastar::app_template, while this option is used for
    // configuring the Seastar memory subsystem.
    static constexpr size_t wasm_udf_reserved_memory = 50 * 1024 * 1024;

    named_value<bool> live_updatable_config_params_changeable_via_cql;
    bool are_live_updatable_config_params_changeable_via_cql() const override {
        return live_updatable_config_params_changeable_via_cql();
    }

    // authenticator options
    named_value<std::string> auth_superuser_name;
    named_value<std::string> auth_superuser_salted_password;

    named_value<std::vector<std::unordered_map<sstring, sstring>>> auth_certificate_role_queries;

    named_value<int> minimum_replication_factor_fail_threshold;
    named_value<int> minimum_replication_factor_warn_threshold;
    named_value<int> maximum_replication_factor_warn_threshold;
    named_value<int> maximum_replication_factor_fail_threshold;

    named_value<int> tablets_initial_scale_factor;
    named_value<uint64_t> target_tablet_size_in_bytes;

    named_value<std::vector<enum_option<replication_strategy_restriction_t>>> replication_strategy_warn_list;
    named_value<std::vector<enum_option<replication_strategy_restriction_t>>> replication_strategy_fail_list;

    named_value<uint32_t> service_levels_interval;

    seastar::logging_settings logging_settings(const log_cli::options&) const;

    const db::extensions& extensions() const;

    utils::updateable_value_source<std::unordered_map<sstring, s3::endpoint_config>> object_storage_config;

    named_value<std::vector<error_injection_at_startup>> error_injections_at_startup;
    named_value<double> topology_barrier_stall_detector_threshold_seconds;
    named_value<bool> enable_tablets;

    static const sstring default_tls_priority;
private:
    template<typename T>
    struct log_legacy_value : public named_value<T> {
        using MyBase = named_value<T>;

        using MyBase::MyBase;

        T value_or(T&& t) const {
            return this->is_set() ? (*this)() : t;
        }
        // do not add to boost::options. We only care about yaml config
        void add_command_line_option(boost::program_options::options_description_easy_init&) override {}
    };

    log_legacy_value<seastar::log_level> default_log_level;
    log_legacy_value<std::unordered_map<sstring, seastar::log_level>> logger_log_level;
    log_legacy_value<bool> log_to_stdout;
    log_legacy_value<bool> log_to_syslog;

    void maybe_in_workdir(named_value<sstring>&, const char*);
    void maybe_in_workdir(named_value<string_list>&, const char*);

    std::shared_ptr<db::extensions> _extensions;
};

}

namespace utils {

template<typename K, typename V, typename... Args, typename K2, typename V2 = V>
V get_or_default(const std::unordered_map<K, V, Args...>& ss, const K2& key, const V2& def = V()) {
    const auto iter = ss.find(key);
    if (iter != ss.end()) {
        return iter->second;
    }
    return def;
}

inline bool is_true(sstring val) {
    std::transform(val.begin(), val.end(), val.begin(), ::tolower);
    return val == "true" || val == "1";
}

future<> configure_tls_creds_builder(seastar::tls::credentials_builder& creds, db::config::string_map options);
future<gms::inet_address> resolve(const config_file::named_value<sstring>&, gms::inet_address::opt_family family = {}, gms::inet_address::opt_family preferred = {});

/*!
 * \brief read the the relabel config from a file
 *
 * Will throw an exception if there is a conflict with the metrics names
 */
future<> update_relabel_config_from_file(const std::string& name);

std::vector<sstring> split_comma_separated_list(sstring comma_separated_list);

}
