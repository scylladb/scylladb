/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
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
// (scope::keyspace) both funnel through these helpers so the version-gating,
// keyword-collection, property-classification, value-validation and
// update-extraction logic lives in one place and cannot drift between the
// ALTER TABLE and ALTER KEYSPACE paths.
namespace cql3::statements::cluster_config_props {

using seastar::sstring;

// Throw if the cluster-config feature is not yet enabled cluster-wide but the
// property set contains any registry option registered for scope `s`.
inline void ensure_registry_supported(
        db::cluster_config_registry::scope s,
        const property_definitions::properties_map_type& properties,
        const gms::feature_service& feat) {
    if (db::cluster_config_registry::current_version(feat)) {
        return;
    }
    for (const auto& [name, value] : properties) {
        const auto* opt = db::cluster_config_registry::find(name);
        if (opt && db::cluster_config_registry::supports_scope(*opt, s)) {
            throw request_validations::invalid_request(
                    "Cluster config registry v0 is not yet supported by this cluster. Upgrade all nodes to use it.");
        }
    }
}

// The registry option names that are valid keywords at scope `s` in the active
// registry version (used to extend a statement's allowed-keyword set).
inline std::set<sstring> supported_config_keywords(
        db::cluster_config_registry::scope s, const gms::feature_service& feat) {
    std::set<sstring> result;
    auto current_version = db::cluster_config_registry::current_version(feat);
    for (const auto& opt : db::cluster_config_registry::options()) {
        if (db::cluster_config_registry::find(opt.name, current_version)
                && db::cluster_config_registry::supports_scope(opt, s)) {
            result.emplace(opt.name);
        }
    }
    return result;
}

// True iff `name` is a registry option registered for scope `s` in the active version.
inline bool is_config_property(
        db::cluster_config_registry::scope s, const sstring& name, const gms::feature_service& feat) {
    auto current_version = db::cluster_config_registry::current_version(feat);
    const auto* opt = db::cluster_config_registry::find(name, current_version);
    return opt && db::cluster_config_registry::supports_scope(*opt, s);
}

// Validate every registry-backed property for scope `s`; throws
// configuration_exception on an out-of-range value. The "null" removal sentinel
// is accepted regardless of the option's value type.
inline void validate_config_values(
        db::cluster_config_registry::scope s,
        const property_definitions::properties_map_type& properties,
        const gms::feature_service& feat) {
    auto current_version = db::cluster_config_registry::current_version(feat);
    for (const auto& [name, value] : properties) {
        const auto* opt = db::cluster_config_registry::find(name, current_version);
        if (!opt || !db::cluster_config_registry::supports_scope(*opt, s)) {
            continue;
        }
        const auto raw_value = property_definitions::value_as_string(name, value);
        if (raw_value == "null") {
            continue;
        }
        if (auto error = db::cluster_config_registry::validate_value(*opt, raw_value)) {
            throw exceptions::configuration_exception(
                    seastar::format("Invalid value for property '{}': {}", name, *error));
        }
    }
}

// Extract registry-backed property assignments for scope `s`, in property order.
// A std::nullopt value encodes the "null" removal sentinel; a present value is
// canonicalized to its stored form.
inline std::vector<std::pair<sstring, std::optional<sstring>>> config_updates(
        db::cluster_config_registry::scope s,
        const property_definitions::properties_map_type& properties,
        const gms::feature_service& feat) {
    std::vector<std::pair<sstring, std::optional<sstring>>> updates;
    auto current_version = db::cluster_config_registry::current_version(feat);
    for (const auto& [name, value] : properties) {
        const auto* opt = db::cluster_config_registry::find(name, current_version);
        if (!opt || !db::cluster_config_registry::supports_scope(*opt, s)) {
            continue;
        }
        const auto raw_value = property_definitions::value_as_string(name, value);
        updates.emplace_back(name, raw_value == "null"
                ? std::nullopt
                : std::make_optional(db::cluster_config_registry::canonicalize_value(*opt, raw_value)));
    }
    return updates;
}

}
