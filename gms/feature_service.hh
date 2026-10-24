/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <map>
#include <unordered_map>
#include <functional>
#include <set>
#include <unordered_set>
#include <vector>
#include "seastarx.hh"
#include "db/schema_features.hh"
#include "gms/feature.hh"

namespace db {
class system_keyspace;
}
namespace service { class storage_service; }

namespace gms {

class gossiper;
class feature_service;
class i_endpoint_state_change_subscriber;

struct feature_config {
    std::set<sstring> disabled_features;
};

class unsupported_feature_exception : public std::runtime_error {
public:
    unsupported_feature_exception(std::string what)
            : runtime_error(std::move(what))
    {}
};

bool is_test_only_feature_enabled();

using namespace std::literals;

/**
 * Holds the values of all the cluster features: a gms::feature for each
 * feature_definition, in an array indexed by feature_definition::index(),
 * and one for each feature created at run time. There is a feature_service
 * on every shard, each with its own values.
 */
class feature_service final : public peering_sharded_service<feature_service> {
    void register_feature(feature& f);
    void unregister_feature(feature& f);
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

    // The values of the features defined in feature_service.cc, by their
    // feature_definition::index().
    // A contiguous array keeps the lookup in operator[] cheap. Filled in the constructor
    // and never resized, because _registered_features and the callers of operator[]
    // refer to its elements.
    std::vector<feature> _defined_features;

public:
    // The value of the feature `f`: whether it is enabled, and its listeners.
    feature& operator[](const feature_definition& f) noexcept {
        return _defined_features[f.index()];
    }
    const feature& operator[](const feature_definition& f) const noexcept {
        return _defined_features[f.index()];
    }

    // Features created at run time rather than declared above: one per cluster config
    // option, from the registry at construction, plus any a test adds. Node-based, so a
    // listener's reference stays valid when another entry is added.
    std::map<sstring, feature> _dynamic_features;
    // The feature of each cluster config option, by option name. The name is a view of
    // the registry entry's, which has static storage.
    std::unordered_map<std::string_view, std::reference_wrapper<feature>> _cluster_config_option_features;
public:
    // Creates the feature of the cluster config option called `option_name` (a view with
    // static storage) and returns it, or returns the one created earlier. Called at
    // construction for every registered option, and by the test hook that injects one.
    feature& add_cluster_config_option_feature(std::string_view option_name);
    // The feature of the cluster config option called `option_name`, or nullptr when no
    // such option was added.
    const feature* cluster_config_option_feature(std::string_view option_name) const;
    // The feature called `name`, created if no feature of that name exists yet. A created
    // feature is advertised and enabled like a declared one. When `name` is already
    // registered, declared or created, that feature is returned as is.
    // Call it only at construction or from tests: a feature added after the node has
    // advertised its feature set to the cluster is not seen by the other nodes.
    feature& add_feature(const sstring& name);

    const std::unordered_map<sstring, std::reference_wrapper<feature>>& registered_features() const;

    static std::set<sstring> to_feature_set(sstring features_string);
    future<> on_system_tables_loaded(db::system_keyspace& sys_ks);

    // Performs the feature check.
    // Throws an unsupported_feature_exception if there is a feature either
    // in `enabled_features` or `unsafe_to_disable_features` that is not being
    // currently supported by this node.
    void check_features(const std::set<sstring>& enabled_features, const std::set<sstring>& unsafe_to_disable_features);
};

} // namespace gms
