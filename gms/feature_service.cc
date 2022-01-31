/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#include <any>
#include <seastar/core/sstring.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include "log.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "db/system_keyspace.hh"
#include "db/query_context.hh"
#include "to_string.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace gms {

// Deprecated features - sent to other nodes via gossip, but assumed true in the code
constexpr std::string_view features::RANGE_TOMBSTONES = "RANGE_TOMBSTONES";
constexpr std::string_view features::LARGE_PARTITIONS = "LARGE_PARTITIONS";
constexpr std::string_view features::MATERIALIZED_VIEWS = "MATERIALIZED_VIEWS";
constexpr std::string_view features::COUNTERS = "COUNTERS";
constexpr std::string_view features::INDEXES = "INDEXES";
constexpr std::string_view features::DIGEST_MULTIPARTITION_READ = "DIGEST_MULTIPARTITION_READ";
constexpr std::string_view features::CORRECT_COUNTER_ORDER = "CORRECT_COUNTER_ORDER";
constexpr std::string_view features::SCHEMA_TABLES_V3 = "SCHEMA_TABLES_V3";
constexpr std::string_view features::CORRECT_NON_COMPOUND_RANGE_TOMBSTONES = "CORRECT_NON_COMPOUND_RANGE_TOMBSTONES";
constexpr std::string_view features::WRITE_FAILURE_REPLY = "WRITE_FAILURE_REPLY";
constexpr std::string_view features::XXHASH = "XXHASH";
constexpr std::string_view features::ROLES = "ROLES";
constexpr std::string_view features::LA_SSTABLE = "LA_SSTABLE_FORMAT";
constexpr std::string_view features::STREAM_WITH_RPC_STREAM = "STREAM_WITH_RPC_STREAM";
constexpr std::string_view features::ROW_LEVEL_REPAIR = "ROW_LEVEL_REPAIR";
constexpr std::string_view features::TRUNCATION_TABLE = "TRUNCATION_TABLE";
constexpr std::string_view features::CORRECT_STATIC_COMPACT_IN_MC = "CORRECT_STATIC_COMPACT_IN_MC";
constexpr std::string_view features::UNBOUNDED_RANGE_TOMBSTONES = "UNBOUNDED_RANGE_TOMBSTONES";
constexpr std::string_view features::MC_SSTABLE = "MC_SSTABLE_FORMAT";

// Up-to-date features
constexpr std::string_view features::UDF = "UDF";
constexpr std::string_view features::MD_SSTABLE = "MD_SSTABLE_FORMAT";
constexpr std::string_view features::VIEW_VIRTUAL_COLUMNS = "VIEW_VIRTUAL_COLUMNS";
constexpr std::string_view features::DIGEST_INSENSITIVE_TO_EXPIRY = "DIGEST_INSENSITIVE_TO_EXPIRY";
constexpr std::string_view features::COMPUTED_COLUMNS = "COMPUTED_COLUMNS";
constexpr std::string_view features::CDC = "CDC";
constexpr std::string_view features::NONFROZEN_UDTS = "NONFROZEN_UDTS";
constexpr std::string_view features::HINTED_HANDOFF_SEPARATE_CONNECTION = "HINTED_HANDOFF_SEPARATE_CONNECTION";
constexpr std::string_view features::LWT = "LWT";
constexpr std::string_view features::PER_TABLE_PARTITIONERS = "PER_TABLE_PARTITIONERS";
constexpr std::string_view features::PER_TABLE_CACHING = "PER_TABLE_CACHING";
constexpr std::string_view features::DIGEST_FOR_NULL_VALUES = "DIGEST_FOR_NULL_VALUES";
constexpr std::string_view features::CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX = "CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX";
constexpr std::string_view features::ALTERNATOR_STREAMS = "ALTERNATOR_STREAMS";
constexpr std::string_view features::ALTERNATOR_TTL = "ALTERNATOR_TTL";
constexpr std::string_view features::RANGE_SCAN_DATA_VARIANT = "RANGE_SCAN_DATA_VARIANT";
constexpr std::string_view features::CDC_GENERATIONS_V2 = "CDC_GENERATIONS_V2";
constexpr std::string_view features::UDA = "UDA";
constexpr std::string_view features::SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT = "SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT";
constexpr std::string_view features::SUPPORTS_RAFT_CLUSTER_MANAGEMENT = "SUPPORTS_RAFT_CLUSTER_MANAGEMENT";
constexpr std::string_view features::USES_RAFT_CLUSTER_MANAGEMENT = "USES_RAFT_CLUSTER_MANAGEMENT";
constexpr std::string_view features::TOMBSTONE_GC_OPTIONS = "TOMBSTONE_GC_OPTIONS";
constexpr std::string_view features::PARALLELIZED_AGGREGATION = "PARALLELIZED_AGGREGATION";

static logging::logger logger("features");

feature_config::feature_config() {
}

feature_service::feature_service(feature_config cfg) : _config(cfg)
        , _udf_feature(*this, features::UDF)
        , _md_sstable_feature(*this, features::MD_SSTABLE)
        , _view_virtual_columns(*this, features::VIEW_VIRTUAL_COLUMNS)
        , _digest_insensitive_to_expiry(*this, features::DIGEST_INSENSITIVE_TO_EXPIRY)
        , _computed_columns(*this, features::COMPUTED_COLUMNS)
        , _cdc_feature(*this, features::CDC)
        , _nonfrozen_udts(*this, features::NONFROZEN_UDTS)
        , _hinted_handoff_separate_connection(*this, features::HINTED_HANDOFF_SEPARATE_CONNECTION)
        , _lwt_feature(*this, features::LWT)
        , _per_table_partitioners_feature(*this, features::PER_TABLE_PARTITIONERS)
        , _per_table_caching_feature(*this, features::PER_TABLE_CACHING)
        , _digest_for_null_values_feature(*this, features::DIGEST_FOR_NULL_VALUES)
        , _correct_idx_token_in_secondary_index_feature(*this, features::CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX)
        , _alternator_streams_feature(*this, features::ALTERNATOR_STREAMS)
        , _alternator_ttl_feature(*this, features::ALTERNATOR_TTL)
        , _range_scan_data_variant(*this, features::RANGE_SCAN_DATA_VARIANT)
        , _cdc_generations_v2(*this, features::CDC_GENERATIONS_V2)
        , _uda(*this, features::UDA)
        , _separate_page_size_and_safety_limit(*this, features::SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT)
        , _supports_raft_cluster_mgmt(*this, features::SUPPORTS_RAFT_CLUSTER_MANAGEMENT)
        , _uses_raft_cluster_mgmt(*this, features::USES_RAFT_CLUSTER_MANAGEMENT)
        , _tombstone_gc_options(*this, features::TOMBSTONE_GC_OPTIONS)
        , _parallelized_aggregation(*this, features::PARALLELIZED_AGGREGATION)
        , _raft_support_listener(_supports_raft_cluster_mgmt.when_enabled([this] {
            // When the cluster fully supports raft-based cluster management,
            // we can re-enable support for the second gossip feature to trigger
            // actual use of raft-based cluster management procedures.
            support(features::USES_RAFT_CLUSTER_MANAGEMENT);
        }))
{}

feature_config feature_config_from_db_config(db::config& cfg, std::set<sstring> disabled) {
    feature_config fcfg;

    fcfg._disabled_features = std::move(disabled);

    switch (sstables::from_string(cfg.sstable_format())) {
    case sstables::sstable_version_types::ka:
    case sstables::sstable_version_types::la:
    case sstables::sstable_version_types::mc:
        fcfg._disabled_features.insert(sstring(gms::features::MD_SSTABLE));
        [[fallthrough]];
    case sstables::sstable_version_types::md:
    case sstables::sstable_version_types::me:
        break;
    }
    if (!cfg.enable_user_defined_functions()) {
        fcfg._disabled_features.insert(sstring(gms::features::UDF));
    } else {
        if (!cfg.check_experimental(db::experimental_features_t::UDF)) {
            throw std::runtime_error(
                    "You must use both enable_user_defined_functions and experimental_features:udf "
                    "to enable user-defined functions");
        }
    }

    if (!cfg.check_experimental(db::experimental_features_t::ALTERNATOR_STREAMS)) {
        fcfg._disabled_features.insert(sstring(gms::features::ALTERNATOR_STREAMS));
    }
    if (!cfg.check_experimental(db::experimental_features_t::ALTERNATOR_TTL)) {
        fcfg._disabled_features.insert(sstring(gms::features::ALTERNATOR_TTL));
    }
    if (!cfg.check_experimental(db::experimental_features_t::RAFT)) {
        fcfg._disabled_features.insert(sstring(gms::features::SUPPORTS_RAFT_CLUSTER_MANAGEMENT));
        fcfg._disabled_features.insert(sstring(gms::features::USES_RAFT_CLUSTER_MANAGEMENT));
    } else {
        // Disable support for using raft cluster management so that it cannot
        // be enabled by accident.
        // This prevents the `USES_RAFT_CLUSTER_MANAGEMENT` feature from being
        // advertised via gossip ahead of time.
        fcfg._masked_features.insert(sstring(gms::features::USES_RAFT_CLUSTER_MANAGEMENT));
    }

    return fcfg;
}

future<> feature_service::stop() {
    return make_ready_future<>();
}

void feature_service::register_feature(feature& f) {
    auto i = _registered_features.emplace(f.name(), f);
    assert(i.second);
}

void feature_service::unregister_feature(feature& f) {
    _registered_features.erase(f.name());
}


void feature_service::enable(const sstring& name) {
    if (auto it = _registered_features.find(name); it != _registered_features.end()) {
        auto&& f = it->second;
        auto& f_ref = f.get();
        if (db::qctx && !f_ref) {
            persist_enabled_feature_info(f_ref);
        }
        f_ref.enable();
    }
}

void feature_service::support(const std::string_view& name) {
    _config._masked_features.erase(sstring(name));
}

std::set<std::string_view> feature_service::known_feature_set() {
    // Add features known by this local node. When a new feature is
    // introduced in scylla, update it here, e.g.,
    // return sstring("FEATURE1,FEATURE2")
    std::set<std::string_view> features = {
        // Deprecated features - sent to other nodes via gossip, but assumed true in the code
        gms::features::RANGE_TOMBSTONES,
        gms::features::LARGE_PARTITIONS,
        gms::features::COUNTERS,
        gms::features::DIGEST_MULTIPARTITION_READ,
        gms::features::CORRECT_COUNTER_ORDER,
        gms::features::SCHEMA_TABLES_V3,
        gms::features::CORRECT_NON_COMPOUND_RANGE_TOMBSTONES,
        gms::features::WRITE_FAILURE_REPLY,
        gms::features::XXHASH,
        gms::features::ROLES,
        gms::features::LA_SSTABLE,
        gms::features::STREAM_WITH_RPC_STREAM,
        gms::features::MATERIALIZED_VIEWS,
        gms::features::INDEXES,
        gms::features::ROW_LEVEL_REPAIR,
        gms::features::TRUNCATION_TABLE,
        gms::features::CORRECT_STATIC_COMPACT_IN_MC,
        gms::features::UNBOUNDED_RANGE_TOMBSTONES,
        gms::features::MC_SSTABLE,

        // Up-to-date features
        gms::features::VIEW_VIRTUAL_COLUMNS,
        gms::features::DIGEST_INSENSITIVE_TO_EXPIRY,
        gms::features::COMPUTED_COLUMNS,
        gms::features::NONFROZEN_UDTS,
        gms::features::HINTED_HANDOFF_SEPARATE_CONNECTION,
        gms::features::PER_TABLE_PARTITIONERS,
        gms::features::PER_TABLE_CACHING,
        gms::features::LWT,
        gms::features::MD_SSTABLE,
        gms::features::UDF,
        gms::features::CDC,
        gms::features::DIGEST_FOR_NULL_VALUES,
        gms::features::CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX,
        gms::features::ALTERNATOR_STREAMS,
        gms::features::ALTERNATOR_TTL,
        gms::features::RANGE_SCAN_DATA_VARIANT,
        gms::features::CDC_GENERATIONS_V2,
        gms::features::UDA,
        gms::features::SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT,
        gms::features::SUPPORTS_RAFT_CLUSTER_MANAGEMENT,
        gms::features::USES_RAFT_CLUSTER_MANAGEMENT,
        gms::features::TOMBSTONE_GC_OPTIONS,
        gms::features::PARALLELIZED_AGGREGATION,
    };

    for (const sstring& s : _config._disabled_features) {
        features.erase(s);
    }
    return features;
}

const std::unordered_map<sstring, std::reference_wrapper<feature>>& feature_service::registered_features() const {
    return _registered_features;
}

std::set<std::string_view> feature_service::supported_feature_set() {
    auto features = known_feature_set();

    for (const sstring& s : _config._masked_features) {
        features.erase(s);
    }
    return features;
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
    f.set_if<db::schema_feature::VIEW_VIRTUAL_COLUMNS>(bool(_view_virtual_columns));
    f.set_if<db::schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY>(bool(_digest_insensitive_to_expiry));
    f.set_if<db::schema_feature::COMPUTED_COLUMNS>(bool(_computed_columns));
    f.set_if<db::schema_feature::CDC_OPTIONS>(bool(_cdc_feature));
    f.set_if<db::schema_feature::PER_TABLE_PARTITIONERS>(bool(_per_table_partitioners_feature));
    return f;
}

std::set<sstring> feature_service::to_feature_set(sstring features_string) {
    std::set<sstring> features;
    boost::split(features, features_string, boost::is_any_of(","));
    features.erase("");
    return features;
}

void feature_service::persist_enabled_feature_info(const gms::feature& f) const {
    // Executed in seastar::async context, because `gms::feature::enable`
    // is only allowed to run within a thread context

    std::optional<sstring> raw_old_value = db::system_keyspace::get_scylla_local_param(ENABLED_FEATURES_KEY).get0();
    if (!raw_old_value) {
        db::system_keyspace::set_scylla_local_param(ENABLED_FEATURES_KEY, f.name()).get0();
        return;
    }
    auto feats_set = to_feature_set(*raw_old_value);
    feats_set.emplace(f.name());
    db::system_keyspace::set_scylla_local_param(ENABLED_FEATURES_KEY, ::join(",", feats_set)).get0();
}

void feature_service::enable(const std::set<std::string_view>& list) {
    for (gms::feature& f : {
        std::ref(_udf_feature),
        std::ref(_md_sstable_feature),
        std::ref(_view_virtual_columns),
        std::ref(_digest_insensitive_to_expiry),
        std::ref(_computed_columns),
        std::ref(_cdc_feature),
        std::ref(_nonfrozen_udts),
        std::ref(_hinted_handoff_separate_connection),
        std::ref(_lwt_feature),
        std::ref(_per_table_partitioners_feature),
        std::ref(_per_table_caching_feature),
        std::ref(_digest_for_null_values_feature),
        std::ref(_correct_idx_token_in_secondary_index_feature),
        std::ref(_alternator_streams_feature),
        std::ref(_alternator_ttl_feature),
        std::ref(_range_scan_data_variant),
        std::ref(_cdc_generations_v2),
        std::ref(_uda),
        std::ref(_separate_page_size_and_safety_limit),
        std::ref(_supports_raft_cluster_mgmt),
        std::ref(_uses_raft_cluster_mgmt),
        std::ref(_tombstone_gc_options),
        std::ref(_parallelized_aggregation),
    })
    {
        if (list.contains(f.name())) {
            if (db::qctx && !f) {
                persist_enabled_feature_info(f);
            }
            f.enable();
        }
    }
}

} // namespace gms
