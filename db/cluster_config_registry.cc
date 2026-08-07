/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/cluster_config_registry.hh"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cmath>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <fmt/format.h>
#include <boost/algorithm/string/predicate.hpp>

#include "gms/feature_service.hh"
#include "utils/assert.hh"

namespace db::cluster_config_registry {

namespace {

constexpr uint32_t to_mask(scope s) {
    return static_cast<uint32_t>(s);
}

constexpr uint32_t schema_scopes =
        to_mask(scope::cluster)
        | to_mask(scope::keyspace)
        | to_mask(scope::table);

constexpr uint32_t table_oriented_scopes =
        to_mask(scope::keyspace)
        | to_mask(scope::table);

constexpr uint32_t node_oriented_scopes =
        to_mask(scope::datacenter)
        | to_mask(scope::rack)
        | to_mask(scope::node);

// An option's scopes must belong to a single resolution domain. CLUSTER is the shared
// fallback for both domains, so it may combine with either, but table-oriented scopes
// (KEYSPACE, TABLE) must never be mixed with node-oriented ones (DATACENTER, RACK, NODE).
constexpr bool is_single_domain_scope_mask(uint32_t mask) {
    const bool has_table_oriented = (mask & table_oriented_scopes) != 0;
    const bool has_node_oriented = (mask & node_oriented_scopes) != 0;
    return !(has_table_oriented && has_node_oriented);
}

constexpr std::array registry_options = {
    option{
        .name = "auto_repair_enabled",
        .type = value_type::boolean,
        .scope_mask = schema_scopes,
        .min_version = version::v0,
    },
};

constexpr bool all_registry_options_are_single_domain() {
    for (const auto& opt : registry_options) {
        if (!is_single_domain_scope_mask(opt.scope_mask)) {
            return false;
        }
    }
    return true;
}

static_assert(all_registry_options_are_single_domain(),
        "A cluster config option must not mix table-oriented (KEYSPACE/TABLE) and "
        "node-oriented (DATACENTER/RACK/NODE) scopes in the same scope mask");

// Empty in production. When a test injects options via add_test_only_option(), this holds
// registry_options followed by the injected ones, and options()/find() consult it instead of
// registry_options. Per-shard state, mirroring how the registry is otherwise stateless.
//
// find() hands out pointers into this vector, so it must never reallocate while those
// pointers are live. Capacity is therefore reserved once, up front, and add_test_only_option()
// refuses to grow past it rather than silently invalidating outstanding pointers.
thread_local std::vector<option> g_options_with_test_overrides;

constexpr size_t max_test_only_options = 16;


std::optional<seastar::sstring> validate_uint32(std::string_view value) {
    uint32_t parsed = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec == std::errc() && result.ptr == end) {
        return std::nullopt;
    }
    return fmt::format("expected unsigned 32-bit integer, got '{}'", value);
}

std::optional<seastar::sstring> validate_floating_point(std::string_view value) {
    float parsed = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto result = std::from_chars(begin, end, parsed);
    // Reject leading whitespace, trailing junk, and non-finite (inf/nan) inputs, so this
    // validator is as strict as validate_uint32 rather than as lenient as strtof.
    if (result.ec == std::errc() && result.ptr == end && std::isfinite(parsed)) {
        return std::nullopt;
    }
    return fmt::format("expected floating-point number, got '{}'", value);
}

// The CQL BOOLEAN token is case-insensitive but case-preserving (Cql.g: `T R U E | F A L S E`),
// so `WITH opt = TRUE` reaches us as "TRUE". Accept any spelling and let canonicalize_value()
// fold it to the stored form.
bool is_boolean(std::string_view value) {
    return boost::algorithm::iequals(value, "true") || boost::algorithm::iequals(value, "false");
}

}

std::span<const option> options() {
    if (!g_options_with_test_overrides.empty()) {
        return g_options_with_test_overrides;
    }
    return registry_options;
}

const option* find(std::string_view name) {
    for (const auto& opt : options()) {
        if (opt.name == name) {
            return &opt;
        }
    }
    return nullptr;
}

const option* find(std::string_view name, std::optional<version> current_version) {
    const auto* opt = find(name);
    if (!opt) {
        return nullptr;
    }
    if (!current_version || *current_version < opt->min_version) {
        return nullptr;
    }
    return opt;
}

bool supports_scope(const option& opt, scope s) {
    return (opt.scope_mask & to_mask(s)) != 0;
}

std::optional<version> current_version(const gms::feature_service& features) {
    if (features.cluster_config_registry_v0) {
        return version::v0;
    }
    return std::nullopt;
}

std::optional<seastar::sstring> validate_value(const option& opt, std::string_view value) {
    switch (opt.type) {
    case value_type::text:
        return std::nullopt;
    case value_type::uint32:
        return validate_uint32(value);
    case value_type::floating_point:
        return validate_floating_point(value);
    case value_type::boolean:
        if (is_boolean(value)) {
            return std::nullopt;
        }
        return fmt::format("expected 'true' or 'false', got '{}'", value);
    }

    // Unreachable: the switch above is exhaustive over value_type. Matches the
    // __builtin_unreachable() idiom used elsewhere in the cluster-config statements.
    __builtin_unreachable();
}

seastar::sstring canonicalize_value(const option& opt, std::string_view value) {
    switch (opt.type) {
    case value_type::text:
    case value_type::uint32:
    case value_type::floating_point:
        return seastar::sstring(value);
    case value_type::boolean:
        return boost::algorithm::iequals(value, "true") ? "true" : "false";
    }

    __builtin_unreachable();
}

void add_test_only_option(option opt) {
    // Same invariant registry_options is static_asserted against. An injected option that
    // mixed the two domains would resolve through a precedence chain that does not exist,
    // so reject it here rather than let a test build a nonsensical registry.
    SCYLLA_ASSERT(is_single_domain_scope_mask(opt.scope_mask));
    if (g_options_with_test_overrides.empty()) {
        g_options_with_test_overrides.reserve(registry_options.size() + max_test_only_options);
        g_options_with_test_overrides.assign(registry_options.begin(), registry_options.end());
    }
    // Growing would reallocate and dangle every pointer previously returned by find().
    SCYLLA_ASSERT(g_options_with_test_overrides.size() < g_options_with_test_overrides.capacity());
    g_options_with_test_overrides.push_back(opt);
}

void clear_test_only_options() {
    // Frees the buffer, so any pointer from a previous find() is invalid past this point.
    // Tests must not cache option pointers across a clear.
    g_options_with_test_overrides.clear();
    g_options_with_test_overrides.shrink_to_fit();
}

}
