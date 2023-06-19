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
#include "utils/to_string.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/map.hpp>
#include "gms/gossiper.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "utils/error_injection.hh"

namespace gms {

static logging::logger logger("features");

feature_config::feature_config() {
}

feature_service::feature_service(feature_config cfg) : _config(cfg)
{}

feature_config feature_config_from_db_config(const db::config& cfg, std::set<sstring> disabled) {
    feature_config fcfg;

    fcfg._disabled_features = std::move(disabled);

    switch (sstables::version_from_string(cfg.sstable_format())) {
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
    if (!cfg.check_experimental(db::experimental_features_t::feature::TABLETS)) {
        fcfg._disabled_features.insert("TABLETS"s);
    }
    if (!cfg.uuid_sstable_identifiers_enabled()) {
        fcfg._disabled_features.insert("UUID_SSTABLE_IDENTIFIERS"s);
    }

    if (!utils::get_local_injector().enter("features_enable_test_feature")) {
        fcfg._disabled_features.insert("TEST_ONLY_FEATURE"s);
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


std::set<std::string_view> feature_service::supported_feature_set() const {
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
    return f;
}

std::set<sstring> feature_service::to_feature_set(sstring features_string) {
    std::set<sstring> features;
    boost::split(features, features_string, boost::is_any_of(","));
    features.erase("");
    return features;
}

class persistent_feature_enabler : public i_endpoint_state_change_subscriber {
    gossiper& _g;
    feature_service& _feat;
    db::system_keyspace& _sys_ks;

public:
    persistent_feature_enabler(gossiper& g, feature_service& f, db::system_keyspace& s)
            : _g(g)
            , _feat(f)
            , _sys_ks(s)
    {
    }
    future<> on_join(inet_address ep, endpoint_state state) override {
        return enable_features();
    }
    future<> on_change(inet_address ep, application_state state, const versioned_value&) override {
        if (state == application_state::SUPPORTED_FEATURES) {
            return enable_features();
        }
        return make_ready_future();
    }
    future<> before_change(inet_address, endpoint_state, application_state, const versioned_value&) override { return make_ready_future(); }
    future<> on_alive(inet_address, endpoint_state) override { return make_ready_future(); }
    future<> on_dead(inet_address, endpoint_state) override { return make_ready_future(); }
    future<> on_remove(inet_address) override { return make_ready_future(); }
    future<> on_restart(inet_address, endpoint_state) override { return make_ready_future(); }

    future<> enable_features();
};

future<> feature_service::enable_features_on_join(gossiper& g, db::system_keyspace& sys_ks) {
    auto enabler = make_shared<persistent_feature_enabler>(g, *this, sys_ks);
    g.register_(enabler);
    return enabler->enable_features();
}

future<> feature_service::enable_features_on_startup(db::system_keyspace& sys_ks) {
    std::set<sstring> features_to_enable;
    const auto persisted_features = co_await sys_ks.load_local_enabled_features();
    if (persisted_features.empty()) {
        co_return;
    }

    const auto known_features = supported_feature_set();
    for (auto&& f : persisted_features) {
        logger.debug("Enabling persisted feature '{}'", f);
        const bool is_registered_feat = _registered_features.contains(sstring(f));
        if (!is_registered_feat || !known_features.contains(f)) {
            if (is_registered_feat) {
                throw std::runtime_error(format(
                    "Feature '{}' was previously enabled in the cluster but its support is disabled by this node. "
                    "Set the corresponding configuration option to enable the support for the feature.", f));
            } else {
                throw std::runtime_error(format("Unknown feature '{}' was previously enabled in the cluster. "
                    " That means this node is performing a prohibited downgrade procedure"
                    " and should not be allowed to boot.", f));
            }
        }
        if (is_registered_feat) {
            features_to_enable.insert(std::move(f));
        }
        // If a feature is not in `registered_features` but still in `known_features` list
        // that means the feature name is used for backward compatibility and should be implicitly
        // enabled in the code by default, so just skip it.
    }

    co_await container().invoke_on_all([&features_to_enable] (auto& srv) -> future<> {
        std::set<std::string_view> feat = boost::copy_range<std::set<std::string_view>>(features_to_enable);
        co_await srv.enable(std::move(feat));
    });
}

future<> persistent_feature_enabler::enable_features() {
    auto loaded_peer_features = co_await _sys_ks.load_peer_features();
    auto&& features = _g.get_supported_features(loaded_peer_features, gossiper::ignore_features_of_local_node::no);

    // Persist enabled feature in the `system.scylla_local` table under the "enabled_features" key.
    // The key itself is maintained as an `unordered_set<string>` and serialized via `to_string`
    // function to preserve readability.
    std::set<sstring> feats_set = co_await _sys_ks.load_local_enabled_features();
    for (feature& f : _feat.registered_features() | boost::adaptors::map_values) {
        if (!f && features.contains(f.name())) {
            feats_set.emplace(f.name());
        }
    }
    co_await _sys_ks.save_local_enabled_features(std::move(feats_set));

    co_await _feat.container().invoke_on_all([&features] (feature_service& fs) -> future<> {
        std::set<std::string_view> features_v = boost::copy_range<std::set<std::string_view>>(features);
        co_await fs.enable(std::move(features_v));
    });
}

future<> feature_service::enable(std::set<std::string_view> list) {
    // `gms::feature::enable` should be run within a seastar thread context
    return seastar::async([this, list = std::move(list)] {
        for (gms::feature& f : _registered_features | boost::adaptors::map_values) {
            if (list.contains(f.name())) {
                f.enable();
            }
        }
    });
}

} // namespace gms
