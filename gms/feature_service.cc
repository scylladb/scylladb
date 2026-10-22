/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#include <seastar/core/sstring.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include "db/schema_features.hh"
#include "utils/log.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "db/cluster_config_registry.hh"
#include "db/system_keyspace.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "gms/gossiper.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "service/storage_service.hh"

namespace gms {

static logging::logger logger("features");

// All the definitions, by their index.
static std::vector<const feature_definition*>& feature_definitions() {
    static std::vector<const feature_definition*> definitions;
    return definitions;
}

feature_definition::feature_definition(std::string_view name)
        : _name(name)
        , _index(feature_definitions().size()) {
    feature_definitions().push_back(this);
}

// The cluster features. Code that uses one of them declares it with
// "extern const gms::feature_definition" in namespace gms::features.
namespace features {

extern const feature_definition user_defined_functions{"UDF"};
extern const feature_definition alternator_streams{"ALTERNATOR_STREAMS"};
extern const feature_definition alternator_ttl{"ALTERNATOR_TTL"};
extern const feature_definition cql_row_ttl{"CQL_ROW_TTL"};
extern const feature_definition range_scan_data_variant{"RANGE_SCAN_DATA_VARIANT"};
extern const feature_definition user_defined_aggregates{"UDA"};
// Historically max_result_size contained only two fields: soft_limit and
// hard_limit. It was somehow obscure because for normal paged queries both
// fields were equal and meant page size. For unpaged queries and reversed
// queries soft_limit was used to warn when the size of the result exceeded
// the soft_limit and hard_limit was used to throw when the result was
// bigger than this hard_limit. To clean things up, we introduced the third
// field into max_result_size. It's name is page_size. Now page_size always
// means the size of the page while soft and hard limits are just what their
// names suggest. They are no longer interpreted as page size. This is not
// a backwards compatible change so this new cluster feature is used to make
// sure the whole cluster supports the new page_size field and we can safely
// send it to replicas.
extern const feature_definition separate_page_size_and_safety_limit{"SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT"};
// Replica is allowed to send back empty pages to coordinator on queries.
extern const feature_definition empty_replica_pages{"EMPTY_REPLICA_PAGES"};
extern const feature_definition empty_replica_mutation_pages{"EMPTY_REPLICA_MUTATION_PAGES"};
extern const feature_definition supports_raft_cluster_mgmt{"SUPPORTS_RAFT_CLUSTER_MANAGEMENT"};
extern const feature_definition tombstone_gc_options{"TOMBSTONE_GC_OPTIONS"};
extern const feature_definition parallelized_aggregation{"PARALLELIZED_AGGREGATION"};
extern const feature_definition keyspace_storage_options{"KEYSPACE_STORAGE_OPTIONS"};
extern const feature_definition typed_errors_in_read_rpc{"TYPED_ERRORS_IN_READ_RPC"};
extern const feature_definition uda_native_parallelized_aggregation{"UDA_NATIVE_PARALLELIZED_AGGREGATION"};
extern const feature_definition aggregate_storage_options{"AGGREGATE_STORAGE_OPTIONS"};
extern const feature_definition collection_indexing{"COLLECTION_INDEXING"};
extern const feature_definition large_collection_detection{"LARGE_COLLECTION_DETECTION"};
extern const feature_definition range_tombstone_and_dead_rows_detection{"RANGE_TOMBSTONE_AND_DEAD_ROWS_DETECTION"};
extern const feature_definition truncate_as_topology_operation{"TRUNCATE_AS_TOPOLOGY_OPERATION"};
extern const feature_definition snapshot_as_topology_operation{"SNAPSHOT_AS_TOPOLOGY_OPERATION"};
extern const feature_definition secondary_indexes_on_static_columns{"SECONDARY_INDEXES_ON_STATIC_COLUMNS"};
extern const feature_definition tablets{"TABLETS"};
extern const feature_definition table_digest_insensitive_to_expiry{"TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"};
extern const feature_definition supports_consistent_topology_changes{"SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES"};
extern const feature_definition topology_requests_type_column{"TOPOLOGY_REQUESTS_TYPE_COLUMN"};
extern const feature_definition topology_requests_target_host_column{"TOPOLOGY_REQUESTS_TARGET_HOST_COLUMN"};
extern const feature_definition native_reverse_queries{"NATIVE_REVERSE_QUERIES"};
extern const feature_definition zero_token_nodes{"ZERO_TOKEN_NODES"};
extern const feature_definition views_with_tablets{"VIEWS_WITH_TABLETS"};
extern const feature_definition group0_limited_voters{"GROUP0_LIMITED_VOTERS"};
extern const feature_definition compaction_history_upgrade{"COMPACTION_HISTORY_UPGRADE"};

// Whether to allow fragmented commitlog entries. While this is a node-local feature as such, hide
// behind a feature to ensure an upgrading cluster appears to be at least functional before using,
// to avoid data loss if rolling back in a dirty state, but also because it changes which/how mutations
// can be applied to a given node - i.e. with it on, a node can accept larger, say, schema mutations,
// whereas without it, it will fail the insert - i.e. for things like raft etc _all_ nodes should
// have it or none, otherwise we can get partial failures on writes.
extern const feature_definition fragmented_commitlog_entries{"FRAGMENTED_COMMITLOG_ENTRIES"};
extern const feature_definition maintenance_tenant{"MAINTENANCE_TENANT"};

extern const feature_definition tablet_incremental_repair{"TABLET_INCREMENTAL_REPAIR"};
extern const feature_definition tablet_rebuild_incremental_repair{"TABLET_REBUILD_INCREMENTAL_REPAIR"};
extern const feature_definition tablet_repair_scheduler{"TABLET_REPAIR_SCHEDULER"};
extern const feature_definition tablet_repair_tasks_table{"TABLET_REPAIR_TASKS_TABLE"};
extern const feature_definition tablet_merge{"TABLET_MERGE"};
extern const feature_definition tablet_rack_aware_view_pairing{"TABLET_RACK_AWARE_VIEW_PAIRING"};

extern const feature_definition tablet_migration_virtual_task{"TABLET_MIGRATION_VIRTUAL_TASK"};
extern const feature_definition tablet_resize_virtual_task{"TABLET_RESIZE_VIRTUAL_TASK"};

// A feature just for use in tests. It must not be advertised unless
// the "features_enable_test_feature" injection is enabled.
// This feature MUST NOT be advertised in release mode!
extern const feature_definition test_only_feature{"TEST_ONLY_FEATURE"};
extern const feature_definition address_nodes_by_host_ids{"ADDRESS_NODES_BY_HOST_IDS"};

extern const feature_definition in_memory_tables{"IN_MEMORY_TABLES"};
extern const feature_definition workload_prioritization{"WORKLOAD_PRIORITIZATION"};
extern const feature_definition colocated_tablets{"COLOCATED_TABLETS"};
extern const feature_definition cdc_with_tablets{"CDC_WITH_TABLETS"};
extern const feature_definition cdc_block_tablet_merges_for_alternator_streams{"CDC_BLOCK_TABLET_MERGES_FOR_ALTERNATOR_STREAMS"};
extern const feature_definition counters_with_tablets{"COUNTERS_WITH_TABLETS"};
extern const feature_definition file_stream{"FILE_STREAM"};
extern const feature_definition sstable_reference_sharing{"SSTABLE_REFERENCE_SHARING"};
extern const feature_definition compression_dicts{"COMPRESSION_DICTS"};
extern const feature_definition tablet_options{"TABLET_OPTIONS"};
extern const feature_definition tablet_load_stats_v2{"TABLET_LOAD_STATS_V2"};
extern const feature_definition sstable_compression_dicts{"SSTABLE_COMPRESSION_DICTS"};
extern const feature_definition repair_based_tablet_rebuild{"REPAIR_BASED_TABLET_REBUILD"};
extern const feature_definition enforced_raft_rpc_scheduling_group{"ENFORCED_RAFT_RPC_SCHEDULING_GROUP"};
extern const feature_definition load_and_stream_abort_rpc_message{"LOAD_AND_STREAM_ABORT_RPC_MESSAGE"};
extern const feature_definition topology_global_request_queue{"TOPOLOGY_GLOBAL_REQUEST_QUEUE"};
extern const feature_definition lwt_with_tablets{"LWT_WITH_TABLETS"};
extern const feature_definition repair_msg_split{"REPAIR_MSG_SPLIT"};
extern const feature_definition parallel_tablet_draining{"PARALLEL_TABLET_DRAINING"};
extern const feature_definition view_building_coordinator{"VIEW_BUILDING_COORDINATOR"};
extern const feature_definition ms_sstable{"MS_SSTABLE_FORMAT"};
extern const feature_definition mt_sstable{"MT_SSTABLE_FORMAT"};
extern const feature_definition rack_list_rf{"RACK_LIST_RF"};
extern const feature_definition driver_service_level{"DRIVER_SERVICE_LEVEL"};
extern const feature_definition strongly_consistent_tables{"STRONGLY_CONSISTENT_TABLES"};
extern const feature_definition logstor{"LOGSTOR"};
extern const feature_definition client_routes{"CLIENT_ROUTES"};
extern const feature_definition removenode_with_left_token_ring{"REMOVENODE_WITH_LEFT_TOKEN_RING"};
extern const feature_definition size_based_load_balancing{"SIZE_BASED_LOAD_BALANCING"};
extern const feature_definition topology_noop_request{"TOPOLOGY_NOOP_REQUEST"};
extern const feature_definition tablets_intermediate_fallback_cleanup{"TABLETS_INTERMEDIATE_FALLBACK_CLEANUP"};
extern const feature_definition batchlog_v2{"BATCHLOG_V2"};
extern const feature_definition vnodes_to_tablets_migrations{"VNODES_TO_TABLETS_MIGRATIONS"};
// Gates the `current_storage_mode` column of system.topology.
extern const feature_definition topology_current_storage_mode{"TOPOLOGY_CURRENT_STORAGE_MODE"};
extern const feature_definition writetime_ttl_individual_element{"WRITETIME_TTL_INDIVIDUAL_ELEMENT"};
extern const feature_definition arbitrary_tablet_boundaries{"ARBITRARY_TABLET_BOUNDARIES"};
extern const feature_definition large_data_virtual_tables{"LARGE_DATA_VIRTUAL_TABLES"};
extern const feature_definition large_data_guardrails{"LARGE_DATA_GUARDRAILS"};
extern const feature_definition keyspace_multi_rf_change{"KEYSPACE_MULTI_RF_CHANGE"};
extern const feature_definition view_building_tasks_min_task_id{"VIEW_BUILDING_TASKS_MIN_TASK_ID"};
extern const feature_definition quiesce_topology_enhanced{"QUIESCE_TOPOLOGY_ENHANCED"};
extern const feature_definition tablet_pow2_convergence{"TABLET_POW2_CONVERGENCE"};
extern const feature_definition fetch_column_mappings_on_tablet_migration{"FETCH_COLUMN_MAPPINGS_ON_TABLET_MIGRATION"};
// Gates the repair_get_table_size RPC verb used to auto-detect small user
// tables for the RBNO small table optimization. The coordinator only probes
// table sizes when the whole cluster supports this feature, avoiding doomed
// RPCs (and their warnings) to nodes that do not register the verb during a
// rolling upgrade.
extern const feature_definition small_table_optimization_size_probe{"SMALL_TABLE_OPTIMIZATION_SIZE_PROBE"};
extern const feature_definition alternator_composite_gsi_keys{"ALTERNATOR_COMPOSITE_GSI_KEYS"};
extern const feature_definition cluster_config{"CLUSTER_CONFIG"};
// Restore persists tablet transitions and a topology request that older nodes fail to parse.
extern const feature_definition tablet_aware_restore{"TABLET_AWARE_RESTORE"};

} // namespace features

static const char* enable_test_feature_error_injection_name = "features_enable_test_feature";
static const char* enable_test_feature_as_deprecated_error_injection_name = "features_enable_test_feature_as_deprecated";

static bool is_test_only_feature_deprecated() {
    return utils::get_local_injector().enter(enable_test_feature_as_deprecated_error_injection_name);
}

bool is_test_only_feature_enabled() {
    return utils::get_local_injector().enter(enable_test_feature_error_injection_name)
            || is_test_only_feature_deprecated();
}

feature_service::feature_service(feature_config cfg)
        : _config(cfg)
        , _defined_features(feature_definitions().size()) {
    for (const feature_definition* f : feature_definitions()) {
        _defined_features[f->index()].emplace(*this, f->name());
    }

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
    initialize_suppressed_features_set();
#endif

    if (is_test_only_feature_deprecated()) {
        // Assume it's enabled
        auto& f = (*this)[features::test_only_feature];
        f.enable();
        unregister_feature(f);
    }

    for (const auto& opt : db::cluster_config_registry::options()) {
        add_cluster_config_option_feature(opt.name);
    }
    // A retired name must not be reused by a live option or a declared feature, or the
    // feature would stand for two different things.
    for (const std::string_view name : db::cluster_config_registry::retired_feature_names()) {
        SCYLLA_ASSERT(!_registered_features.contains(sstring(name)));
    }
}

feature& feature_service::add_cluster_config_option_feature(std::string_view option_name) {
    if (const auto it = _cluster_config_option_features.find(option_name); it != _cluster_config_option_features.end()) {
        return it->second.get();
    }
    const auto name = db::cluster_config_registry::feature_name(option_name);
    // A clash with a declared feature would alias two unrelated things.
    SCYLLA_ASSERT(!_registered_features.contains(name));
    auto& f = add_feature(name);
    _cluster_config_option_features.emplace(option_name, f);
    return f;
}

const feature* feature_service::cluster_config_option_feature(std::string_view option_name) const {
    const auto it = _cluster_config_option_features.find(option_name);
    return it == _cluster_config_option_features.end() ? nullptr : &it->second.get();
}

feature& feature_service::add_feature(const sstring& name) {
    if (const auto it = _registered_features.find(name); it != _registered_features.end()) {
        return it->second.get();
    }
    const auto [it, inserted] = _dynamic_features.try_emplace(name, *this, std::string_view(name));
    return it->second;
}

future<> feature_service::stop() {
    return make_ready_future<>();
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
void feature_service::initialize_suppressed_features_set() {
    if (const auto features_list = utils::get_local_injector().inject_parameter<std::string_view>("suppress_features"); features_list) {
        boost::split(_suppressed_features, *features_list, boost::is_any_of(";"));
    }
}
#endif

void feature_service::register_feature(feature& f) {
    auto i = _registered_features.emplace(f.name(), f);
    SCYLLA_ASSERT(i.second);
}

void feature_service::unregister_feature(feature& f) {
    _registered_features.erase(f.name());
}


std::set<std::string_view> feature_service::supported_feature_set() const {
    std::set<std::string_view> features = {
        // Deprecated features - sent to other nodes via gossip, but assumed true in the code
        "RANGE_TOMBSTONES"sv,
        "LARGE_PARTITIONS"sv,
        "COUNTERS"sv,
        "DIGEST_MULTIPARTITION_READ"sv,
        "CORRECT_COUNTER_ORDER"sv,
        "SCHEMA_TABLES_V3"sv,
        "CORRECT_NON_COMPOUND_RANGE_TOMBSTONES"sv,
        "WRITE_FAILURE_REPLY"sv,
        "XXHASH"sv,
        "ROLES"sv,
        "LA_SSTABLE_FORMAT"sv,
        "STREAM_WITH_RPC_STREAM"sv,
        "MATERIALIZED_VIEWS"sv,
        "INDEXES"sv,
        "ROW_LEVEL_REPAIR"sv,
        "TRUNCATION_TABLE"sv,
        "CORRECT_STATIC_COMPACT_IN_MC"sv,
        "UNBOUNDED_RANGE_TOMBSTONES"sv,
        "MC_SSTABLE_FORMAT"sv,
        "COMPUTED_COLUMNS"sv,
        "SCHEMA_COMMITLOG"sv,
        "MD_SSTABLE_FORMAT"sv,
        "ME_SSTABLE_FORMAT"sv,
        "VIEW_VIRTUAL_COLUMNS"sv,
        "DIGEST_INSENSITIVE_TO_EXPIRY"sv,
        "CDC"sv,
        "NONFROZEN_UDTS"sv,
        "HINTED_HANDOFF_SEPARATE_CONNECTION"sv,
        "LWT"sv,
        "PER_TABLE_PARTITIONERS"sv,
        "PER_TABLE_CACHING"sv,
        "DIGEST_FOR_NULL_VALUES"sv,
        "CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX"sv,
        "UUID_SSTABLE_IDENTIFIERS"sv,
        "GROUP0_SCHEMA_VERSIONING"sv,
        "VIEW_BUILD_STATUS_ON_GROUP0"sv,
        "CDC_GENERATIONS_V2"sv,
        "HOST_ID_BASED_HINTED_HANDOFF"sv,
    };

    // Deprecated in the same sense: the features of cluster config options that were
    // removed, still advertised so that a cluster which enabled them stays joinable.
    for (const std::string_view name : db::cluster_config_registry::retired_feature_names()) {
        features.insert(name);
    }

    if (is_test_only_feature_deprecated()) {
        features.insert((*this)[features::test_only_feature].name());
    }

    for (auto& [name, f_ref] : _registered_features) {
        features.insert(name);
    }

    for (const sstring& s : _config.disabled_features) {
        features.erase(s);
    }

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
    for (auto& sf: _suppressed_features) {
        features.erase(sf);
    }
#endif

    return features;
}

const std::unordered_map<sstring, std::reference_wrapper<feature>>& feature_service::registered_features() const {
    return _registered_features;
}

feature::feature(feature_service& service, std::string_view name, bool enabled)
        : _service(&service)
        , _name(name)
        , _enabled(enabled) {
    _service->register_feature(*this);
}

feature::~feature() {
    if (_service) {
        _service->unregister_feature(*this);
    }
}

feature& feature::operator=(feature&& other) {
    _service->unregister_feature(*this);
    _service = std::exchange(other._service, nullptr);
    _name = other._name;
    _enabled = other._enabled;
    _s = std::move(other._s);
    _service->register_feature(*this);
    return *this;
}

void feature::enable() {
    if (!_enabled) {
        if (this_shard_id() == 0) {
            logger.info("Feature {} is enabled", name());
        }
        _enabled = true;
        _s();
    }
}

db::schema_features feature_service::cluster_schema_features() const {
    db::schema_features f;
    f.set<db::schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY>();
    f.set<db::schema_feature::COMPUTED_COLUMNS>();
    f.set_if<db::schema_feature::SCYLLA_KEYSPACES>((*this)[features::keyspace_storage_options]);
    f.set_if<db::schema_feature::SCYLLA_KEYSPACES>((*this)[features::tablets]);
    f.set_if<db::schema_feature::SCYLLA_AGGREGATES>((*this)[features::aggregate_storage_options]);
    f.set_if<db::schema_feature::TABLE_DIGEST_INSENSITIVE_TO_EXPIRY>((*this)[features::table_digest_insensitive_to_expiry]);
    f.set<db::schema_feature::GROUP0_SCHEMA_VERSIONING>();
    f.set<db::schema_feature::IN_MEMORY_TABLES>();
    f.set_if<db::schema_feature::TABLET_OPTIONS>(bool((*this)[features::tablet_options]));
    f.set_if<db::schema_feature::KEYSPACE_MULTI_RF_CHANGE>(bool((*this)[features::keyspace_multi_rf_change]));
    f.set_if<db::schema_feature::CLUSTER_CONFIG_TABLES>(bool((*this)[features::cluster_config]));
    return f;
}

std::set<sstring> feature_service::to_feature_set(sstring features_string) {
    std::set<sstring> features;
    boost::split(features, features_string, boost::is_any_of(","));
    features.erase("");
    return features;
}

future<> feature_service::on_system_tables_loaded(db::system_keyspace& sys_ks) {
    return enable_features_on_startup(sys_ks);
}

future<> feature_service::enable_features_on_startup(db::system_keyspace& sys_ks) {
    std::set<sstring> features_to_enable;
    std::set<sstring> persisted_features;
    std::set<sstring> persisted_unsafe_to_disable_features;

    auto topo_features = co_await sys_ks.load_topology_features_state();
    if (topo_features) {
        persisted_unsafe_to_disable_features = topo_features->calculate_not_yet_enabled_features();
        persisted_features = std::move(topo_features->enabled_features);
    } else {
        persisted_features = co_await sys_ks.load_local_enabled_features();
    }

    check_features(persisted_features, persisted_unsafe_to_disable_features);

    for (auto&& f : persisted_features) {
        logger.debug("Enabling persisted feature '{}'", f);
        if (_registered_features.contains(sstring(f))) {
            features_to_enable.insert(std::move(f));
        }
    }

    co_await container().invoke_on_all([&features_to_enable] (auto& srv) -> future<> {
        auto feat = features_to_enable | std::ranges::to<std::set<std::string_view>>();
        co_await srv.enable(std::move(feat));
    });
}

void feature_service::check_features(const std::set<sstring>& enabled_features,
            const std::set<sstring>& unsafe_to_disable_features) {

    if (enabled_features.empty() && unsafe_to_disable_features.empty()) {
        return;
    }

    const auto known_features = supported_feature_set();
    for (auto&& f : enabled_features) {
        const bool is_registered_feat = _registered_features.contains(sstring(f));
        if (!known_features.contains(f)) {
            if (is_registered_feat) {
                throw unsupported_feature_exception(format(
                    "Feature '{}' was previously enabled in the cluster but its support is disabled by this node. "
                    "Set the corresponding configuration option to enable the support for the feature.", f));
            } else {
                throw unsupported_feature_exception(format("Unknown feature '{}' was previously enabled in the cluster. "
                    " That means this node is performing a prohibited downgrade procedure"
                    " and should not be allowed to boot.", f));
            }
        }
        // If a feature is not in `registered_features` but still in `known_features` list
        // that means the feature name is used for backward compatibility and should be implicitly
        // enabled in the code by default, so just skip it.
    }

    // With raft cluster features, it is also unsafe to disable support for features
    // that are supported by everybody but not enabled yet. There is a possibility
    // that the feature became enabled and this node didn't notice it.
    for (auto&& f : unsafe_to_disable_features) {
        const bool is_registered_feat = _registered_features.contains(sstring(f));
        if (!known_features.contains(f)) {
            if (is_registered_feat) {
                throw unsupported_feature_exception(format(
                    "Feature '{}' was previously supported by all nodes in the cluster. It is unknown whether "
                    "the feature became enabled or not, therefore it is not safe for this node to boot. "
                    "Set the corresponding configuration option to enable the support for the feature.", f));
            } else {
                throw unsupported_feature_exception(format(
                    "Unknown feature '{}' was previously supported by all nodes in the cluster. "
                    "That means this node is performing a prohibited downgrade procedure "
                    "and should not be allowed to boot.", f));
            }
        }
    }
}

future<> feature_service::enable(std::set<std::string_view> list) {
    // `gms::feature::enable` should be run within a seastar thread context
    return seastar::async([this, list = std::move(list)] {
        for (gms::feature& f : _registered_features | std::views::values) {
            if (list.contains(f.name())) {
                f.enable();
            }
        }
    });
}

} // namespace gms
