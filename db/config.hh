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

#pragma once

#include <boost/program_options.hpp>
#include <unordered_map>

#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/program-options.hh>
#include <seastar/util/log.hh>

#include "seastarx.hh"
#include "utils/config_file.hh"
#include "utils/enum_option.hh"
#include "db/hints/host_filter.hh"

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
    bool operator==(const seed_provider_type& other) const {
        return class_name == other.class_name && parameters == other.parameters;
    }
    friend std::ostream& operator<<(std::ostream& os, const seed_provider_type&);
};

std::ostream& operator<<(std::ostream& os, const db::seed_provider_type& s);

}


namespace utils {

sstring config_value_as_json(const db::seed_provider_type& v);

sstring config_value_as_json(const log_level& v);

sstring config_value_as_json(const std::unordered_map<sstring, log_level>& v);

}

namespace db {

/// Enumeration of all valid values for the `experimental` config entry.
struct experimental_features_t {
    enum feature { UNUSED, UDF, UNUSED_CDC, ALTERNATOR_STREAMS };
    static std::unordered_map<sstring, feature> map(); // See enum_option.
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


class config : public utils::config_file {
public:
    config();
    config(std::shared_ptr<db::extensions>);
    ~config();

    // For testing only
    void add_cdc_extension();

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

    /*
     * All values and documentation taken from
     * http://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html
     */
    named_value<double> background_writer_scheduling_quota;
    named_value<bool> auto_adjust_flush_quota;
    named_value<float> memtable_flush_static_shares;
    named_value<float> compaction_static_shares;
    named_value<bool> compaction_enforce_min_threshold;
    named_value<sstring> cluster_name;
    named_value<sstring> listen_address;
    named_value<sstring> listen_interface;
    named_value<bool> listen_interface_prefer_ipv6;
    named_value<sstring> work_directory;
    named_value<sstring> commitlog_directory;
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
    named_value<uint32_t> memtable_total_space_in_mb;
    named_value<uint32_t> concurrent_reads;
    named_value<uint32_t> concurrent_writes;
    named_value<uint32_t> concurrent_counter_writes;
    named_value<bool> incremental_backups;
    named_value<bool> snapshot_before_compaction;
    named_value<uint32_t> phi_convict_threshold;
    named_value<uint32_t> failure_detector_timeout_in_ms;
    named_value<sstring> commitlog_sync;
    named_value<uint32_t> commitlog_segment_size_in_mb;
    named_value<uint32_t> commitlog_sync_period_in_ms;
    named_value<uint32_t> commitlog_sync_batch_window_in_ms;
    named_value<int64_t> commitlog_total_space_in_mb;
    named_value<bool> commitlog_reuse_segments;
    named_value<bool> commitlog_use_o_dsync;
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
    named_value<uint32_t> index_summary_capacity_in_mb;
    named_value<uint32_t> index_summary_resize_interval_in_minutes;
    named_value<double> reduce_cache_capacity_to;
    named_value<double> reduce_cache_sizes_at;
    named_value<uint32_t> stream_throughput_outbound_megabits_per_sec;
    named_value<uint32_t> inter_dc_stream_throughput_outbound_megabits_per_sec;
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
    named_value<uint16_t> native_transport_port_ssl;
    named_value<uint16_t> native_shard_aware_transport_port;
    named_value<uint16_t> native_shard_aware_transport_port_ssl;
    named_value<uint32_t> native_transport_max_threads;
    named_value<uint32_t> native_transport_max_frame_size_in_mb;
    named_value<sstring> broadcast_rpc_address;
    named_value<uint16_t> rpc_port;
    named_value<bool> start_rpc;
    named_value<bool> rpc_keepalive;
    named_value<uint32_t> rpc_max_threads;
    named_value<uint32_t> rpc_min_threads;
    named_value<uint32_t> rpc_recv_buff_size_in_bytes;
    named_value<uint32_t> rpc_send_buff_size_in_bytes;
    named_value<sstring> rpc_server_type;
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
    named_value<uint32_t> thrift_framed_transport_size_in_mb;
    named_value<uint32_t> thrift_max_message_length_in_mb;
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
    named_value<sstring> replace_node;
    named_value<sstring> replace_token;
    named_value<sstring> replace_address;
    named_value<sstring> replace_address_first_boot;
    named_value<bool> override_decommission;
    named_value<bool> enable_repair_based_node_ops;
    named_value<uint32_t> ring_delay_ms;
    named_value<uint32_t> shadow_round_ms;
    named_value<uint32_t> fd_max_interval_ms;
    named_value<uint32_t> fd_initial_value_ms;
    named_value<uint32_t> shutdown_announce_in_ms;
    named_value<bool> developer_mode;
    named_value<int32_t> skip_wait_for_gossip_to_settle;
    named_value<int32_t> force_gossip_generation;
    named_value<bool> experimental;
    named_value<std::vector<enum_option<experimental_features_t>>> experimental_features;
    named_value<size_t> lsa_reclamation_step;
    named_value<uint16_t> prometheus_port;
    named_value<sstring> prometheus_address;
    named_value<sstring> prometheus_prefix;
    named_value<bool> abort_on_lsa_bad_alloc;
    named_value<unsigned> murmur3_partitioner_ignore_msb_bits;
    named_value<double> virtual_dirty_soft_limit;
    named_value<double> sstable_summary_ratio;
    named_value<size_t> large_memory_allocation_warning_threshold;
    named_value<bool> enable_deprecated_partitioners;
    named_value<bool> enable_keyspace_column_family_metrics;
    named_value<bool> enable_sstable_data_integrity_check;
    named_value<bool> enable_sstable_key_validation;
    named_value<bool> cpu_scheduler;
    named_value<bool> view_building;
    named_value<bool> enable_sstables_mc_format;
    named_value<bool> enable_sstables_md_format;
    named_value<bool> enable_dangerous_direct_import_of_cassandra_counters;
    named_value<bool> enable_shard_aware_drivers;
    named_value<bool> enable_ipv6_dns_lookup;
    named_value<bool> abort_on_internal_error;
    named_value<uint32_t> max_partition_key_restrictions_per_query;
    named_value<uint32_t> max_clustering_key_restrictions_per_query;
    named_value<uint64_t> max_memory_for_unlimited_query_soft_limit;
    named_value<uint64_t> max_memory_for_unlimited_query_hard_limit;
    named_value<unsigned> initial_sstable_loading_concurrency;
    named_value<bool> enable_3_1_0_compatibility_mode;
    named_value<bool> enable_user_defined_functions;
    named_value<unsigned> user_defined_function_time_limit_ms;
    named_value<unsigned> user_defined_function_allocation_limit_bytes;
    named_value<unsigned> user_defined_function_contiguous_allocation_limit_bytes;
    named_value<uint32_t> schema_registry_grace_period;
    named_value<uint32_t> max_concurrent_requests_per_shard;
    named_value<bool> cdc_dont_rewrite_streams;

    named_value<uint16_t> alternator_port;
    named_value<uint16_t> alternator_https_port;
    named_value<sstring> alternator_address;
    named_value<bool> alternator_enforce_authorization;
    named_value<sstring> alternator_write_isolation;
    named_value<uint32_t> alternator_streams_time_window_s;
    named_value<uint32_t> alternator_timeout_in_ms;

    named_value<bool> abort_on_ebadf;

    named_value<uint16_t> redis_port;
    named_value<uint16_t> redis_ssl_port;
    named_value<sstring> redis_read_consistency_level;
    named_value<sstring> redis_write_consistency_level;
    named_value<uint16_t> redis_database_count;
    named_value<string_map> redis_keyspace_replication_strategy_options;

    named_value<bool> sanitizer_report_backtrace;

    // Options to restrict (forbid, warn or somehow limit) certain operations
    // or options which non-expert users are more likely to regret than to
    // enjoy:
    named_value<tri_mode_restriction> restrict_replication_simplestrategy;
    named_value<tri_mode_restriction> restrict_dtcs;

    seastar::logging_settings logging_settings(const boost::program_options::variables_map&) const;

    const db::extensions& extensions() const;

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
    log_legacy_value<bool> log_to_stdout, log_to_syslog;

    void maybe_in_workdir(named_value<sstring>&, const char*);
    void maybe_in_workdir(named_value<string_list>&, const char*);

    std::shared_ptr<db::extensions> _extensions;
};

}
