/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include <seastar/core/format.hh>
#include <seastar/core/sstring.hh>

#include "cql3/statements/property_definitions.hh"
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
            // std::string_view, not the sstring itself: fmt renders an sstring argument as
            // the list of its characters here, so the reason would reach the user as
            // ['e', 'x', ...] rather than as words.
            throw exceptions::configuration_exception(
                    seastar::format("Invalid value for property '{}': {}", name, std::string_view(*error)));
        }
    }
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
