/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <variant>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "enum_set.hh"

namespace gms {
class feature_service;
}

namespace db::cluster_config_registry {

enum class scope : uint8_t {
    cluster,
    datacenter,
    rack,
    node,
    keyspace,
    table,
};

using scope_set = enum_set<super_enum<scope,
        scope::cluster,
        scope::datacenter,
        scope::rack,
        scope::node,
        scope::keyspace,
        scope::table>>;

enum class value_type {
    text,
    integer,
    floating_point,
    boolean,
};

enum class version : uint8_t {
    v0,
};

// A config value in its native type. One alternative per value_type, in the same order;
// option::type() is the index of the active alternative.
using config_value = std::variant<std::string_view, int64_t, double, bool>;

struct option {
    std::string_view name;
    std::string_view description;
    scope_set scopes;
    version min_version;
    // The value a consumer gets when no scope stores an override. Its alternative also
    // defines the option's type. Read it through the to_* converters, not directly.
    config_value default_value;

    constexpr value_type type() const {
        return static_cast<value_type>(default_value.index());
    }
};

std::span<const option> options();
// The registered option called `name`, or nullptr. With a version, only an option whose
// min_version is at or below it; without one, any registered option. Whether the cluster
// has enabled cluster config at all (current_version() returning nullopt) is the caller's
// check, not this function's.
const option* find(std::string_view name, std::optional<version> max_version = std::nullopt);
bool supports_scope(const option& opt, scope s);
// True for an option resolved per table (KEYSPACE and/or TABLE scope), false for one
// resolved per node (DATACENTER/RACK/NODE) or at CLUSTER scope only.
bool is_table_oriented(const option& opt);
std::optional<version> current_version(const gms::feature_service& features);

// Returns nullopt on success. On failure, returns a human-readable reason.
std::optional<seastar::sstring> validate_value(const option& opt, std::string_view value);

// Converts a resolved value to the option's native type, applying the option's registered
// default when no scope stored an override (`value` is nullopt). Consumers should read an
// option through these rather than reaching for `default_value` directly, so that the stored
// text and the default are turned into a value in exactly one place and no call site can
// substitute a default of its own. Defaults take no part in scope resolution and are never
// written to a `configs` map: resolve_config() reports absence, and these fill it in.
//
// Each helper reports an internal error (utils::on_internal_error) when the option's type
// does not match, so a mismatched call is a programming error rather than a silent conversion.
// A present `value` is expected to have passed validate_value() (every override written
// through the CQL path has); text that fails to parse anyway - a row corrupted or written
// out-of-band - is logged and treated as absent, so a bad row degrades to the default
// instead of failing the read.
bool to_boolean(const option& opt, const std::optional<seastar::sstring>& value);
int64_t to_integer(const option& opt, const std::optional<seastar::sstring>& value);
double to_floating_point(const option& opt, const std::optional<seastar::sstring>& value);
seastar::sstring to_text(const option& opt, const std::optional<seastar::sstring>& value);

// Returns the canonical text to persist for a value that has already passed
// validate_value(). CQL literals are case-preserving but case-insensitive (the BOOLEAN
// token matches TRUE as readily as true), so the accepted spellings are folded to a single
// stored form here. Consumers can then compare the stored text without re-normalizing it.
seastar::sstring canonicalize_value(const option& opt, std::string_view value);

// Test-only hooks for injecting additional registry options at runtime, on every shard.
// Production code never calls these; they exist so tests can exercise scopes/types that no
// shipping option covers yet (e.g. node-oriented DATACENTER/RACK/NODE scopes). The injected
// option's `name` must have static storage duration (e.g. a string literal), since the
// registry stores it as a view. Clear the injected options when the test is done.
seastar::future<> add_test_only_option_on_all_shards(option opt);
seastar::future<> clear_test_only_options_on_all_shards();

}
