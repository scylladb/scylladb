/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <unordered_map>
#include <functional>
#include <set>
#include <unordered_set>
#include "seastarx.hh"
#include "db/schema_features.hh"
#include "gms/feature.hh"

namespace db {
class config;
class system_keyspace;
}
namespace service { class storage_service; }

namespace gms {

class gossiper;
class feature_service;
class i_endpoint_state_change_subscriber;

struct feature_config {
private:
    std::set<sstring> _disabled_features;
    feature_config();

    friend class feature_service;
    friend feature_config feature_config_from_db_config(const db::config& cfg, std::set<sstring> disabled);
};

feature_config feature_config_from_db_config(const db::config& cfg, std::set<sstring> disabled = {});

class unsupported_feature_exception : public std::runtime_error {
public:
    unsupported_feature_exception(std::string what)
            : runtime_error(std::move(what))
    {}
};

using namespace std::literals;

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 *
 * A pointer to `cql3::query_processor` can be optionally supplied
 * if the instance needs to persist enabled features in a system table.
 */
class feature_service final : public peering_sharded_service<feature_service> {
    void register_feature(feature& f);
    void unregister_feature(feature& f);
    friend class feature;
    std::unordered_map<sstring, std::reference_wrapper<feature>> _registered_features;
    std::unordered_set<sstring> _suppressed_features;

    feature_config _config;

    future<> enable_features_on_startup(db::system_keyspace&);
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
    void initialize_suppressed_features_set();
#endif
public:
    explicit feature_service(feature_config cfg);
    ~feature_service() = default;
    future<> stop();
    future<> enable(std::set<std::string_view> list);
    db::schema_features cluster_schema_features() const;
    std::set<std::string_view> supported_feature_set() const;

    // Key in the 'system.scylla_local' table, that is used to
    // persist enabled features
    static constexpr const char* ENABLED_FEATURES_KEY = "enabled_features";

public:
    gms::feature user_defined_functions { *this, "UDF"sv };
    gms::feature alternator_streams { *this, "ALTERNATOR_STREAMS"sv };
    gms::feature alternator_ttl { *this, "ALTERNATOR_TTL"sv };
    gms::feature range_scan_data_variant { *this, "RANGE_SCAN_DATA_VARIANT"sv };
    gms::feature cdc_generations_v2 { *this, "CDC_GENERATIONS_V2"sv };
    gms::feature user_defined_aggregates { *this, "UDA"sv };
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
    gms::feature separate_page_size_and_safety_limit { *this, "SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT"sv };
    // Replica is allowed to send back empty pages to coordinator on queries.
    gms::feature empty_replica_pages { *this, "EMPTY_REPLICA_PAGES"sv };
    gms::feature empty_replica_mutation_pages { *this, "EMPTY_REPLICA_MUTATION_PAGES"sv };
    gms::feature supports_raft_cluster_mgmt { *this, "SUPPORTS_RAFT_CLUSTER_MANAGEMENT"sv };
    gms::feature tombstone_gc_options { *this, "TOMBSTONE_GC_OPTIONS"sv };
    gms::feature parallelized_aggregation { *this, "PARALLELIZED_AGGREGATION"sv };
    gms::feature keyspace_storage_options { *this, "KEYSPACE_STORAGE_OPTIONS"sv };
    gms::feature typed_errors_in_read_rpc { *this, "TYPED_ERRORS_IN_READ_RPC"sv };
    gms::feature uda_native_parallelized_aggregation { *this, "UDA_NATIVE_PARALLELIZED_AGGREGATION"sv };
    gms::feature aggregate_storage_options { *this, "AGGREGATE_STORAGE_OPTIONS"sv };
    gms::feature collection_indexing { *this, "COLLECTION_INDEXING"sv };
    gms::feature large_collection_detection { *this, "LARGE_COLLECTION_DETECTION"sv };
    gms::feature range_tombstone_and_dead_rows_detection { *this, "RANGE_TOMBSTONE_AND_DEAD_ROWS_DETECTION"sv };
    gms::feature truncate_as_topology_operation { *this, "TRUNCATE_AS_TOPOLOGY_OPERATION"sv };
    gms::feature secondary_indexes_on_static_columns { *this, "SECONDARY_INDEXES_ON_STATIC_COLUMNS"sv };
    gms::feature tablets { *this, "TABLETS"sv };
    gms::feature uuid_sstable_identifiers { *this, "UUID_SSTABLE_IDENTIFIERS"sv };
    gms::feature table_digest_insensitive_to_expiry { *this, "TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"sv };
    // If this feature is enabled, schema versions are persisted by the group 0 command
    // that modifies schema instead of being calculated as a digest (hash) by each node separately.
    // The feature controls both the 'global' schema version (the one gossiped as application_state::SCHEMA)
    // and the per-table schema versions (schema::version()).
    // The feature affects non-Raft mode as well (e.g. during RECOVERY), where we send additional
    // tombstones and flags to schema tables when performing schema changes, allowing us to
    // revert to the digest method when necessary (if we must perform a schema change during RECOVERY).
    gms::feature group0_schema_versioning { *this, "GROUP0_SCHEMA_VERSIONING"sv };
    gms::feature supports_consistent_topology_changes { *this, "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES"sv };
    gms::feature host_id_based_hinted_handoff { *this, "HOST_ID_BASED_HINTED_HANDOFF"sv };
    gms::feature topology_requests_type_column { *this, "TOPOLOGY_REQUESTS_TYPE_COLUMN"sv };
    gms::feature native_reverse_queries { *this, "NATIVE_REVERSE_QUERIES"sv };
    gms::feature zero_token_nodes { *this, "ZERO_TOKEN_NODES"sv };
    gms::feature view_build_status_on_group0 { *this, "VIEW_BUILD_STATUS_ON_GROUP0"sv };
    gms::feature views_with_tablets { *this, "VIEWS_WITH_TABLETS"sv };
    gms::feature group0_limited_voters { *this, "GROUP0_LIMITED_VOTERS"sv };
    gms::feature compaction_history_upgrade { *this, "COMPACTION_HISTORY_UPGRADE"};

    // Whether to allow fragmented commitlog entries. While this is a node-local feature as such, hide
    // behind a feature to ensure an upgrading cluster appears to be at least functional before using,
    // to avoid data loss if rolling back in a dirty state, but also because it changes which/how mutations
    // can be applied to a given node - i.e. with it on, a node can accept larger, say, schema mutations,
    // whereas without it, it will fail the insert - i.e. for things like raft etc _all_ nodes should
    // have it or none, otherwise we can get partial failures on writes.
    gms::feature fragmented_commitlog_entries { *this, "FRAGMENTED_COMMITLOG_ENTRIES"sv };
    gms::feature maintenance_tenant { *this, "MAINTENANCE_TENANT"sv };

    gms::feature tablet_repair_scheduler { *this, "TABLET_REPAIR_SCHEDULER"sv };
    gms::feature tablet_merge { *this, "TABLET_MERGE"sv };
    gms::feature tablet_rack_aware_view_pairing { *this, "TABLET_RACK_AWARE_VIEW_PAIRING"sv };

    gms::feature tablet_migration_virtual_task { *this, "TABLET_MIGRATION_VIRTUAL_TASK"sv };
    gms::feature tablet_resize_virtual_task { *this, "TABLET_RESIZE_VIRTUAL_TASK"sv };

    // A feature just for use in tests. It must not be advertised unless
    // the "features_enable_test_feature" injection is enabled.
    // This feature MUST NOT be advertised in release mode!
    gms::feature test_only_feature { *this, "TEST_ONLY_FEATURE"sv };
    gms::feature address_nodes_by_host_ids { *this, "ADDRESS_NODES_BY_HOST_IDS"sv };

    gms::feature in_memory_tables { *this, "IN_MEMORY_TABLES"sv };
    gms::feature workload_prioritization { *this, "WORKLOAD_PRIORITIZATION"sv };
    gms::feature colocated_tablets { *this, "COLOCATED_TABLETS"sv };
    gms::feature file_stream { *this, "FILE_STREAM"sv };
    gms::feature compression_dicts { *this, "COMPRESSION_DICTS"sv };
    gms::feature tablet_options { *this, "TABLET_OPTIONS"sv };
    gms::feature tablet_load_stats_v2 { *this, "TABLET_LOAD_STATS_V2"sv };
    gms::feature sstable_compression_dicts { *this, "SSTABLE_COMPRESSION_DICTS"sv };
    gms::feature repair_based_tablet_rebuild { *this, "REPAIR_BASED_TABLET_REBUILD"sv };
    gms::feature enforced_raft_rpc_scheduling_group { *this, "ENFORCED_RAFT_RPC_SCHEDULING_GROUP"sv };
    gms::feature load_and_stream_abort_rpc_message { *this, "LOAD_AND_STREAM_ABORT_RPC_MESSAGE"sv };
    gms::feature topology_global_request_queue { *this, "TOPOLOGY_GLOBAL_REQUEST_QUEUE"sv };
    gms::feature keyspace_multi_rf_change { *this, "KEYSPACE_MULTI_RF_CHANGE"sv };
public:

    const std::unordered_map<sstring, std::reference_wrapper<feature>>& registered_features() const;

    static std::set<sstring> to_feature_set(sstring features_string);
    future<> enable_features_on_join(gossiper&, db::system_keyspace&, service::storage_service&);
    future<> on_system_tables_loaded(db::system_keyspace& sys_ks);

    // Performs the feature check.
    // Throws an unsupported_feature_exception if there is a feature either
    // in `enabled_features` or `unsafe_to_disable_features` that is not being
    // currently supported by this node.
    void check_features(const std::set<sstring>& enabled_features, const std::set<sstring>& unsafe_to_disable_features);
};

} // namespace gms
