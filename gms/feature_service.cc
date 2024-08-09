/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Copyright (C) 2020-present ScyllaDB
 */

#include <seastar/core/sstring.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include "log.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "db/system_keyspace.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/map.hpp>
#include "gms/gossiper.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "service/storage_service.hh"

namespace gms {

static logging::logger logger("features");

static const char* enable_test_feature_error_injection_name = "features_enable_test_feature";
static const char* enable_test_feature_as_deprecated_error_injection_name = "features_enable_test_feature_as_deprecated";

static bool is_test_only_feature_deprecated() {
    return utils::get_local_injector().enter(enable_test_feature_as_deprecated_error_injection_name);
}

static bool is_test_only_feature_enabled() {
    return utils::get_local_injector().enter(enable_test_feature_error_injection_name)
            || is_test_only_feature_deprecated();
}

feature_config::feature_config() {
}

feature_service::feature_service(feature_config cfg) : _config(cfg) {
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
    initialize_suppressed_features_set();
#endif

    if (is_test_only_feature_deprecated()) {
        // Assume it's enabled
        test_only_feature.enable();
        unregister_feature(test_only_feature);
    }
}

feature_config feature_config_from_db_config(const db::config& cfg, std::set<sstring> disabled) {
    feature_config fcfg;

    fcfg._disabled_features = std::move(disabled);

    switch (sstables::version_from_string(cfg.sstable_format())) {
    case sstables::sstable_version_types::md:
        logger.warn("sstable_format must be 'me', '{}' is specified", cfg.sstable_format());
        break;
    case sstables::sstable_version_types::me:
        break;
    default:
        SCYLLA_ASSERT(false && "Invalid sstable_format");
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
    if (!cfg.check_experimental(db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS)) {
        fcfg._disabled_features.insert("KEYSPACE_STORAGE_OPTIONS"s);
    }
    if (!cfg.enable_tablets()) {
        fcfg._disabled_features.insert("TABLETS"s);
    }
    if (!cfg.uuid_sstable_identifiers_enabled()) {
        fcfg._disabled_features.insert("UUID_SSTABLE_IDENTIFIERS"s);
    }
    if (!cfg.table_digest_insensitive_to_expiry()) {
        fcfg._disabled_features.insert("TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"s);
    }

    if (!is_test_only_feature_enabled()) {
        fcfg._disabled_features.insert("TEST_ONLY_FEATURE"s);
    }

    return fcfg;
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
    };

    if (is_test_only_feature_deprecated()) {
        features.insert(test_only_feature.name());
    }

    for (auto& [name, f_ref] : _registered_features) {
        features.insert(name);
    }

    for (const sstring& s : _config._disabled_features) {
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
    f.set_if<db::schema_feature::SCYLLA_KEYSPACES>(keyspace_storage_options);
    f.set_if<db::schema_feature::SCYLLA_KEYSPACES>(tablets);
    f.set_if<db::schema_feature::SCYLLA_AGGREGATES>(aggregate_storage_options);
    f.set_if<db::schema_feature::TABLE_DIGEST_INSENSITIVE_TO_EXPIRY>(table_digest_insensitive_to_expiry);
    f.set_if<db::schema_feature::GROUP0_SCHEMA_VERSIONING>(group0_schema_versioning);
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
    service::storage_service& _ss;

public:
    persistent_feature_enabler(gossiper& g, feature_service& f, db::system_keyspace& s, service::storage_service& ss)
            : _g(g)
            , _feat(f)
            , _sys_ks(s)
            , _ss(ss)
    {
    }
    future<> on_join(inet_address ep, endpoint_state_ptr state, gms::permit_id) override {
        return enable_features();
    }
    future<> on_change(inet_address ep, const gms::application_state_map& states, gms::permit_id pid) override {
        if (states.contains(application_state::SUPPORTED_FEATURES)) {
            return enable_features();
        }
        return make_ready_future();
    }
    future<> on_alive(inet_address, endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }
    future<> on_dead(inet_address, endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }
    future<> on_remove(inet_address, gms::permit_id) override { return make_ready_future(); }
    future<> on_restart(inet_address, endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }

    future<> enable_features();
};

future<> feature_service::enable_features_on_join(gossiper& g, db::system_keyspace& sys_ks, service::storage_service& ss) {
    auto enabler = make_shared<persistent_feature_enabler>(g, *this, sys_ks, ss);
    g.register_(enabler);
    return enabler->enable_features();
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
        std::set<std::string_view> feat = boost::copy_range<std::set<std::string_view>>(features_to_enable);
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

future<> persistent_feature_enabler::enable_features() {
    if (_ss.raft_topology_change_enabled()) {
        co_return;
    }

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
    co_await _sys_ks.save_local_enabled_features(std::move(feats_set), true);

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
