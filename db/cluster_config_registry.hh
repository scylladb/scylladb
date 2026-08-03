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

#include <seastar/core/sstring.hh>

namespace gms {
class feature_service;
}

namespace db::cluster_config_registry {

enum class scope : uint32_t {
    cluster = 1u << 0,
    datacenter = 1u << 1,
    rack = 1u << 2,
    node = 1u << 3,
    keyspace = 1u << 4,
    table = 1u << 5,
};

enum class value_type {
    text,
    uint32,
    floating_point,
    boolean,
};

enum class version : uint8_t {
    v0,
};

// An option's built-in default, held in the option's native type. One alternative per
// value_type, in the same order: text, uint32, floating_point, boolean.
using default_value_type = std::variant<std::string_view, uint32_t, double, bool>;

struct option {
    std::string_view name;
    value_type type;
    uint32_t scope_mask;
    version min_version;
    // The value a consumer uses when no scope stores an override. Declared here so every
    // option's default has a single definition that consumers read instead of hard-coding
    // their own literal, and so a changed default is visible in a diff between two releases.
    //
    // Defaults take no part in scope resolution and are never written to a `configs` map:
    // resolve_config() still returns absence when no scope has an override, and the consumer
    // falls back to this value.
    //
    // The active alternative must match `type`. A static_assert in cluster_config_registry.cc
    // enforces that for every registered option; add_test_only_option() checks it for injected
    // ones.
    default_value_type default_value;
};

std::span<const option> options();
const option* find(std::string_view name);
const option* find(std::string_view name, std::optional<version> current_version);
bool supports_scope(const option& opt, scope s);
std::optional<version> current_version(const gms::feature_service& features);

// Returns nullopt on success. On failure, returns a human-readable reason.
std::optional<seastar::sstring> validate_value(const option& opt, std::string_view value);

// Converts a resolved value to the option's native type, applying the option's registered
// default when no scope stored an override (`value` is nullopt). Consumers should read an
// option through these rather than reaching for `default_value` directly, so that the stored
// text and the default are turned into a value in exactly one place and no call site can
// substitute a default of its own.
//
// Each helper reports an internal error (utils::on_internal_error) when the option's type
// does not match, so a mismatched call is a programming error rather than a silent conversion. A present `value` is expected to have
// passed validate_value() (every override written through the CQL path has); text that fails
// to parse anyway - a row corrupted or written out-of-band - is logged and treated as absent,
// so a bad row degrades to the default instead of failing the read.
bool to_boolean(const option& opt, const std::optional<seastar::sstring>& value);
uint32_t to_uint32(const option& opt, const std::optional<seastar::sstring>& value);
double to_floating_point(const option& opt, const std::optional<seastar::sstring>& value);
seastar::sstring to_text(const option& opt, const std::optional<seastar::sstring>& value);

// Returns the canonical text to persist for a value that has already passed
// validate_value(). CQL literals are case-preserving but case-insensitive (the BOOLEAN
// token matches TRUE as readily as true), so the accepted spellings are folded to a single
// stored form here. Consumers can then compare the stored text without re-normalizing it.
seastar::sstring canonicalize_value(const option& opt, std::string_view value);

// Test-only hooks for injecting additional registry options at runtime. Production code never
// calls these; they exist so tests can exercise scopes/types that no shipping option covers yet
// (e.g. node-oriented DATACENTER/RACK/NODE scopes). The injected option's `name` must have static
// storage duration (e.g. a string literal), since the registry stores it as a view. The injected
// options live in per-shard state, so register them on every shard (smp::invoke_on_all) before
// issuing statements, and clear them afterwards.
void add_test_only_option(option opt);
void clear_test_only_options();

}
