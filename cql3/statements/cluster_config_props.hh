/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>
#include <ranges>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include <seastar/core/format.hh>
#include <seastar/core/sstring.hh>

#include <seastar/core/coroutine.hh>

#include "auth/resource.hh"
#include "cql3/statements/property_definitions.hh"
#include "service/client_state.hh"
#include "cql3/statements/request_validations.hh"
#include "db/cluster_config_registry.hh"
#include "exceptions/exceptions.hh"
#include "gms/feature_service.hh"

// Shared, scope-parameterized handling of registry-backed cluster-config
// properties in CQL statements. cf_prop_defs (scope::table) and ks_prop_defs
// (scope::keyspace) both funnel through these helpers so the feature-gating,
// keyword-collection, property-classification, value-validation and
// update-extraction logic lives in one place and cannot drift between the
// ALTER TABLE and ALTER KEYSPACE paths.
namespace cql3::statements::cluster_config_props {

using seastar::sstring;

// The cluster config scopes at or below the scope target that `target` stands for. An
// option can be set somewhere under `target` only if it supports one of them.
inline db::cluster_config_registry::scope_set scopes_within(const auth::resource& target) {
    using db::cluster_config_registry::scope;
    using db::cluster_config_registry::scope_set;
    if (target.kind() == auth::resource_kind::data) {
        if (auth::data_resource_view(target).table()) {
            return scope_set::of<scope::table>();
        }
        return scope_set::of<scope::keyspace, scope::table>();
    }
    const auth::cluster_resource_view v(target);
    if (v.host_id()) {
        return scope_set::of<scope::node>();
    }
    if (v.rack()) {
        return scope_set::of<scope::rack, scope::node>();
    }
    if (v.datacenter()) {
        return scope_set::of<scope::datacenter, scope::rack, scope::node>();
    }
    return scope_set::of<scope::cluster, scope::datacenter, scope::rack, scope::node, scope::keyspace, scope::table>();
}

// Authorizes setting or removing the cluster config options `names` on the keyspace or
// table `target`. A restricted option needs CONFIGURE. An owner-settable option needs
// CONFIGURE or the statement's own data permission, which `ensure_data_access` checks
// (it throws when the permission is missing).
//
// CONFIGURE cannot be granted before every node knows it (CLUSTER_CONFIG_RBAC). Until
// then the data permission authorizes every option, as it did before the permission existed.
template <typename Names, typename EnsureDataAccess>
seastar::future<> ensure_config_access(const service::client_state& state, const gms::feature_service& feat, const auth::resource& target,
        const Names& names, EnsureDataAccess ensure_data_access) {
    if (!feat.cluster_config_rbac) {
        if (!std::ranges::empty(names)) {
            co_await ensure_data_access();
        }
        co_return;
    }
    for (const auto& name : names) {
        const auto* opt = db::cluster_config_registry::find(name);
        if (opt && opt->access == db::cluster_config_registry::option_access::owner_settable) {
            if (!co_await state.check_has_config_permission(auth::permission::CONFIGURE, target, name)) {
                co_await ensure_data_access();
            }
            continue;
        }
        co_await state.ensure_has_config_permission(auth::permission::CONFIGURE, target, name);
    }
}

// Throw if the property set contains a registry option registered for scope `s`
// whose feature is not yet enabled cluster-wide.
inline void ensure_registry_supported(
        db::cluster_config_registry::scope s,
        const property_definitions& props,
        const gms::feature_service& feat) {
    for (const auto& [name, value] : props.properties()) {
        const auto* opt = db::cluster_config_registry::find(name);
        if (opt && db::cluster_config_registry::supports_scope(*opt, s) && !db::cluster_config_registry::is_supported(*opt, feat)) {
            throw request_validations::invalid_request(
                    "Cluster config option '{}' is not yet supported by this cluster. Upgrade all nodes to use it.", name);
        }
    }
}

// The registry option names that are valid keywords at scope `s` and supported by
// the cluster (used to extend a statement's allowed-keyword set).
inline std::set<sstring> supported_config_keywords(
        db::cluster_config_registry::scope s, const gms::feature_service& feat) {
    std::set<sstring> result;
    for (const auto& opt : db::cluster_config_registry::options()) {
        if (db::cluster_config_registry::supports_scope(opt, s) && db::cluster_config_registry::is_supported(opt, feat)) {
            result.emplace(opt.name);
        }
    }
    return result;
}

// The registry option called `name` when it is registered for scope `s` and supported by
// the cluster, else nullptr.
inline const db::cluster_config_registry::option* supported_config_option(
        db::cluster_config_registry::scope s, const sstring& name, const gms::feature_service& feat) {
    const auto* opt = db::cluster_config_registry::find(name);
    if (opt && db::cluster_config_registry::supports_scope(*opt, s) && db::cluster_config_registry::is_supported(*opt, feat)) {
        return opt;
    }
    return nullptr;
}

// True iff `name` is a registry option registered for scope `s` and supported by the cluster.
inline bool is_config_property(
        db::cluster_config_registry::scope s, const sstring& name, const gms::feature_service& feat) {
    return supported_config_option(s, name, feat) != nullptr;
}

// Validate every registry-backed property for scope `s`; throws
// configuration_exception on an out-of-range value. `= NULL` removes the override and so
// is accepted regardless of the option's value type; the string literal 'null' is an
// ordinary value and is validated like any other.
inline void validate_config_values(
        db::cluster_config_registry::scope s,
        const property_definitions& props,
        const gms::feature_service& feat) {
    for (const auto& [name, value] : props.properties()) {
        const auto* opt = supported_config_option(s, name, feat);
        if (!opt) {
            continue;
        }
        if (props.is_null_keyword(name)) {
            continue;
        }
        const auto raw_value = property_definitions::value_as_string(name, value);
        if (auto error = db::cluster_config_registry::validate_value(*opt, raw_value)) {
            throw exceptions::configuration_exception(
                    seastar::format("Invalid value for property '{}': {}", name, *error));
        }
    }
}

// The names of the registry-backed properties for scope `s`. Unlike config_updates(), this
// does not look at the values, so it can run before they are validated.
inline std::vector<sstring> config_names(
        db::cluster_config_registry::scope s,
        const property_definitions& props,
        const gms::feature_service& feat) {
    std::vector<sstring> names;
    for (const auto& [name, value] : props.properties()) {
        if (supported_config_option(s, name, feat)) {
            names.push_back(name);
        }
    }
    return names;
}

// Extract registry-backed property assignments for scope `s`, in property order.
// A std::nullopt value encodes `= NULL`, i.e. removal; a present value is canonicalized
// to its stored form.
inline std::vector<std::pair<sstring, std::optional<sstring>>> config_updates(
        db::cluster_config_registry::scope s,
        const property_definitions& props,
        const gms::feature_service& feat) {
    std::vector<std::pair<sstring, std::optional<sstring>>> updates;
    for (const auto& [name, value] : props.properties()) {
        const auto* opt = supported_config_option(s, name, feat);
        if (!opt) {
            continue;
        }
        if (props.is_null_keyword(name)) {
            updates.emplace_back(name, std::nullopt);
            continue;
        }
        const auto raw_value = property_definitions::value_as_string(name, value);
        updates.emplace_back(name, db::cluster_config_registry::canonicalize_value(*opt, raw_value));
    }
    return updates;
}

}
