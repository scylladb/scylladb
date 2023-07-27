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
#include <boost/range/adaptor/map.hpp>

namespace gms {

static logging::logger logger("features");

feature_config::feature_config() {
}

feature_service::feature_service(feature_config cfg) : _config(cfg)
{}

feature_config feature_config_from_db_config(const db::config& cfg, std::set<sstring> disabled) {
    feature_config fcfg;

    fcfg._disabled_features = std::move(disabled);

    switch (sstables::from_string(cfg.sstable_format())) {
    case sstables::sstable_version_types::ka:
    case sstables::sstable_version_types::la:
    case sstables::sstable_version_types::mc:
        fcfg._disabled_features.insert("MD_SSTABLE_FORMAT"s);
        [[fallthrough]];
    case sstables::sstable_version_types::md:
        fcfg._disabled_features.insert("ME_SSTABLE_FORMAT"s);
        [[fallthrough]];
    case sstables::sstable_version_types::me:
        break;
    }

    if (!cfg.enable_user_defined_functions()) {
        fcfg._disabled_features.insert("UDF");
    } else {
        if (!cfg.check_experimental(db::experimental_features_t::feature::UDF)) {
            throw std::runtime_error(
                    "You must use both enable_user_defined_functions and experimental_features:udf "
                    "to enable user-defined functions");
        }
    }

    if (!cfg.check_experimental(db::experimental_features_t::feature::ALTERNATOR_STREAMS)) {
        fcfg._disabled_features.insert("ALTERNATOR_STREAMS"s);
    }
    if (!cfg.consistent_cluster_management()) {
        fcfg._disabled_features.insert("SUPPORTS_RAFT_CLUSTER_MANAGEMENT"s);
    }
    if (!cfg.check_experimental(db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS)) {
        fcfg._disabled_features.insert("KEYSPACE_STORAGE_OPTIONS"s);
    }
    if (!cfg.table_digest_insensitive_to_expiry()) {
        fcfg._disabled_features.insert("TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"s);
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

std::set<std::string_view> feature_service::supported_feature_set() {
    // Add features known by this local node. When a new feature is
    // introduced in scylla, update it here, e.g.,
    // return sstring("FEATURE1,FEATURE2")
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
        "LARGE_COLLECTION_DETECTION"sv,
    };

    for (auto& [name, f_ref] : _registered_features) {
        features.insert(name);
    }

    for (const sstring& s : _config._disabled_features) {
        features.erase(s);
    }
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
    f.set_if<db::schema_feature::VIEW_VIRTUAL_COLUMNS>(view_virtual_columns);
    f.set_if<db::schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY>(digest_insensitive_to_expiry);
    f.set_if<db::schema_feature::COMPUTED_COLUMNS>(computed_columns);
    f.set_if<db::schema_feature::CDC_OPTIONS>(cdc);
    f.set_if<db::schema_feature::PER_TABLE_PARTITIONERS>(per_table_partitioners);
    f.set_if<db::schema_feature::SCYLLA_KEYSPACES>(keyspace_storage_options);
    f.set_if<db::schema_feature::SCYLLA_AGGREGATES>(aggregate_storage_options);
    f.set_if<db::schema_feature::TABLE_DIGEST_INSENSITIVE_TO_EXPIRY>(table_digest_insensitive_to_expiry);
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
    for (gms::feature& f : _registered_features | boost::adaptors::map_values) {
        if (list.contains(f.name())) {
            if (db::qctx && !f) {
                persist_enabled_feature_info(f);
            }
            f.enable();
        }
    }
}

} // namespace gms
