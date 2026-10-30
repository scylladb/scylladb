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

namespace seastar {
template <typename T> class sharded;
}

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

// A config value in its native type. One alternative per value_type, in the same order;
// option::type() is the index of the active alternative.
using config_value = std::variant<std::string_view, int64_t, double, bool>;

// The names of the registered options, for the subsystems that consume them. A consumer
// looks its option up by one of these rather than by a bare literal, so that the registry
// entry and its reader cannot drift apart without the compiler noticing.
namespace option_name {

inline constexpr std::string_view auto_repair_enabled = "auto_repair_enabled";
inline constexpr std::string_view auto_repair_threshold_in_seconds = "auto_repair_threshold_in_seconds";
inline constexpr std::string_view repair_hints_batchlog_flush_discard_unreplayed_hints = "repair_hints_batchlog_flush_discard_unreplayed_hints";
inline constexpr std::string_view auto_scrub_period_seconds = "auto_scrub_period_seconds";
inline constexpr std::string_view table_metrics = "table_metrics";

}

// The values of the table_metrics option.
namespace table_metrics_value {

inline constexpr std::string_view none = "none";
inline constexpr std::string_view per_node = "per_node";
inline constexpr std::string_view per_shard = "per_shard";

}

struct option {
    std::string_view name;
    std::string_view description;
    scope_set scopes;
    // The value a consumer gets when no scope stores an override. Its alternative also
    // defines the option's type. Read it through the to_* converters, not directly.
    config_value default_value;
    // Optional per-option custom parser.
    // When defined, it is used instead of the type-specific parser and validator.
    // The parser must return the declared type.
    //
    // If set, the custom_excpected_description will be used in validation errors.
    config_value (*custom_parser)(std::string_view) = nullptr;
    std::string_view custom_expected_description = "";

    constexpr value_type type() const {
        return static_cast<value_type>(default_value.index());
    }
};

std::span<const option> options();
// The registered option called `name`, or nullptr. Whether the cluster supports it is the
// caller's check (is_supported()), not this function's.
const option* find(std::string_view name);
bool supports_scope(const option& opt, scope s);
// True for an option resolved per table (KEYSPACE and/or TABLE scope), false for one
// resolved per node (DATACENTER/RACK/NODE) or at CLUSTER scope only.
bool is_table_oriented(const option& opt);
// The cluster feature that says every node knows the option called `option_name`:
// CLUSTER_CONFIG_<NAME>. gms::feature_service advertises one for each registered option.
seastar::sstring feature_name(std::string_view option_name);
// Feature names of options removed from the registry. A feature once enabled on a cluster
// must stay supported, or a node without it cannot join, so these are advertised forever.
std::span<const std::string_view> retired_feature_names();
// True once every node has cluster config at all: the base feature that creates the
// config tables and the CQL surface.
bool enabled(const gms::feature_service& features);
// True once every node has cluster config and knows `opt`, i.e. the base feature and the
// option's own feature are both enabled.
bool is_supported(const option& opt, const gms::feature_service& features);

// Defaults of auto_repair_enabled and auto_repair_threshold_in_seconds. Also the defaults of the
// deprecated yaml options auto_repair_enabled_default and
// auto_repair_threshold_default_in_seconds in db/config.cc, which the tablet repair
// scheduler falls back to when no scope stores an override, so the two cannot drift apart.
constexpr bool auto_repair_enabled_default = false;
constexpr int64_t auto_repair_threshold_default_seconds = 24 * 3600;

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
// registry stores it as a view. The option's feature is created and enabled on every shard,
// so the injected option counts as supported. Clear the injected options when the test is
// done; the features stay, which is harmless.
seastar::future<> add_test_only_option_on_all_shards(seastar::sharded<gms::feature_service>& features, option opt);
seastar::future<> clear_test_only_options_on_all_shards();

}
