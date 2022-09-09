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

static std::set<sstring> disabled_features_from_db_config(db::config& cfg) {
    std::set<sstring> result;

    switch (sstables::from_string(cfg.sstable_format())) {
    case sstables::sstable_version_types::ka:
    case sstables::sstable_version_types::la:
    case sstables::sstable_version_types::mc:
        result.insert("MD_SSTABLE_FORMAT"s);
        [[fallthrough]];
    case sstables::sstable_version_types::md:
        result.insert("ME_SSTABLE_FORMAT"s);
        [[fallthrough]];
    case sstables::sstable_version_types::me:
        break;
    }

    if (!cfg.enable_user_defined_functions()) {
        result.insert("UDF");
    } else {
        if (!cfg.check_experimental(db::experimental_features_t::feature::UDF)) {
            throw std::runtime_error(
                    "You must use both enable_user_defined_functions and experimental_features:udf "
                    "to enable user-defined functions");
        }
    }

    if (!cfg.check_experimental(db::experimental_features_t::feature::ALTERNATOR_STREAMS)) {
        result.insert("ALTERNATOR_STREAMS"s);
    }
    if (!cfg.check_experimental(db::experimental_features_t::feature::ALTERNATOR_TTL)) {
        result.insert("ALTERNATOR_TTL"s);
    }
    if (!cfg.check_experimental(db::experimental_features_t::feature::RAFT)) {
        result.insert("SUPPORTS_RAFT_CLUSTER_MANAGEMENT"s);
    }
    if (!cfg.check_experimental(db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS)) {
        result.insert("KEYSPACE_STORAGE_OPTIONS"s);
    }

    return result;
}

feature_service::feature_service(db::config& cfg, custom_feature_config_for_tests custom_cfg)
        : _disabled_features(disabled_features_from_db_config(cfg))
        // FIXME: we observe `cfg.experimental_features` because there is currently no way to observe the
        // entire config.  In theory, other things could change the set of supported features:
        // `cfg.enable_user_defined_functions` or `cfg.sstable_format` (at the moment of writing this
        // comment). But `recalculate` is only interested in a subset of features, the ones for which we
        // explicitly considered the possibility of supporting them in runtime. If additional config options
        // need to be observed in the future to handle more features changing, we can add observers here when
        // we need them. This has the drawback that the warn messages in `recalculate` won't be completely
        // precise (they won't 'catch' attempts to support features which cannot be supported in runtime if
        // the config options responsible for these features don't have corresponding observers). Fixing this
        // would require adding some way to watch the entire config, or add observers for every applicable
        // config option right now, but then it would be easy to miss changing the set of observers
        // appropriately when `recalculate` is updated. So we just ignored the problem for now.
        , _experimental_features_observer(cfg.experimental_features.observe([this, &cfg] (auto&) { recalculate(cfg); })) {
    _disabled_features.merge(std::move(custom_cfg.extra_disabled_features));
}

void feature_service::recalculate(db::config& cfg) {
    // The list of features which were verified to be safe to be supported
    // dynamically (i.e. through a runtime Scylla configuration reload).
    static const std::set<sstring> can_be_supported_dynamically {
        // TODO: currently empty.
    };

    auto disabled_from_new_cfg = disabled_features_from_db_config(cfg);

    {
        std::set<sstring> attempted_to_disable;
        std::set_difference(
                disabled_from_new_cfg.begin(), disabled_from_new_cfg.end(),
                _disabled_features.begin(), _disabled_features.end(),
                std::inserter(attempted_to_disable, attempted_to_disable.end()));

        if (!attempted_to_disable.empty()) {
            logger.warn(
                "The new Scylla configuration would cause this node to stop supporting the following features,"
                " which it previously supported: {}. It is not allowed to stop supporting features"
                " while the node is running. The node will keep supporting them until it is restarted."
                " WARNING: if the features are already enabled (because all nodes were supporting them),"
                " it is not possible to un-support them; in that case the node will refuse to boot when restarted.",
                attempted_to_disable);
        }
    }

    std::set<sstring> attempted_to_support;
    std::set_difference(
            _disabled_features.begin(), _disabled_features.end(),
            disabled_from_new_cfg.begin(), disabled_from_new_cfg.end(),
            std::inserter(attempted_to_support, attempted_to_support.end()));

    std::set<sstring> newly_supported;
    std::set_intersection(
            attempted_to_support.begin(), attempted_to_support.end(),
            can_be_supported_dynamically.begin(), can_be_supported_dynamically.end(),
            std::inserter(newly_supported, newly_supported.end()));

    if (attempted_to_support.size() > newly_supported.size()) {
        std::set<sstring> failed_to_support;
        std::set_difference(
            attempted_to_support.begin(), attempted_to_support.end(),
            newly_supported.begin(), newly_supported.end(),
            std::inserter(failed_to_support, failed_to_support.end()));

        logger.warn("The following features cannot be supported until this node restarts: {}", failed_to_support);
    }

    if (newly_supported.empty()) {
        return;
    }

    logger.info("New features supported by this node: {}", newly_supported);
    std::erase_if(_disabled_features, [&newly_supported] (const sstring& f) { return newly_supported.contains(f); });

    for (auto& f : _supported_features_change_callbacks) {
        f();
    }
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
    };

    for (auto& [name, f_ref] : _registered_features) {
        features.insert(name);
    }

    for (const sstring& s : _disabled_features) {
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

struct feature_service::supported_features_change_subscription::impl {
    feature_service& _service;
    std::list<features_changed_callback_t>::iterator _it;

    impl(feature_service& service, features_changed_callback_t cb)
        : _service(service)
        , _it(_service._supported_features_change_callbacks.insert(_service._supported_features_change_callbacks.begin(), std::move(cb)))
    {}

    ~impl() {
        _service._supported_features_change_callbacks.erase(_it);
    }
};

feature_service::supported_features_change_subscription::supported_features_change_subscription(std::unique_ptr<impl> impl) : _impl(std::move(impl)) {}
feature_service::supported_features_change_subscription::~supported_features_change_subscription() = default;

feature_service::supported_features_change_subscription
feature_service::on_supported_features_change(features_changed_callback_t cb) {
    return { std::make_unique<feature_service::supported_features_change_subscription::impl>(*this, std::move(cb)) };
}

} // namespace gms
