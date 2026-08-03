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

struct option {
    std::string_view name;
    value_type type;
    uint32_t scope_mask;
    version min_version;
};

std::span<const option> options();
const option* find(std::string_view name);
const option* find(std::string_view name, std::optional<version> current_version);
bool supports_scope(const option& opt, scope s);
std::optional<version> current_version(const gms::feature_service& features);

// Returns nullopt on success. On failure, returns a human-readable reason.
std::optional<seastar::sstring> validate_value(const option& opt, std::string_view value);

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
