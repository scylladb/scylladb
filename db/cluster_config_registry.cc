/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/cluster_config_registry.hh"

#include <algorithm>
#include <array>
#include <charconv>
#include <string>
#include <string_view>
#include <system_error>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include "gms/feature_service.hh"
#include "types/types.hh"
#include "utils/chunked_string.hh"
#include "utils/log.hh"
#include "utils/on_internal_error.hh"

namespace db::cluster_config_registry {

namespace {

logging::logger cluster_config_registry_logger("cluster_config_registry");

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

// An option's default must be declared in the type the option accepts, so that a consumer
// reading it with std::get<T> cannot be handed a different type than validate_value() would
// have accepted for a stored override.
constexpr bool default_matches_type(const option& opt) {
    switch (opt.type) {
    case value_type::text:
        return std::holds_alternative<std::string_view>(opt.default_value);
    case value_type::uint32:
        return std::holds_alternative<uint32_t>(opt.default_value);
    case value_type::floating_point:
        return std::holds_alternative<double>(opt.default_value);
    case value_type::boolean:
        return std::holds_alternative<bool>(opt.default_value);
    }

    __builtin_unreachable();
}

constexpr std::array registry_options = {
    option{
        .name = "auto_repair_enabled",
        .type = value_type::boolean,
        .scope_mask = schema_scopes,
        .min_version = version::v0,
        .default_value = false,
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

constexpr bool all_registry_options_have_matching_default_type() {
    for (const auto& opt : registry_options) {
        if (!default_matches_type(opt)) {
            return false;
        }
    }
    return true;
}

static_assert(all_registry_options_have_matching_default_type(),
        "A cluster config option's default_value must hold the alternative matching its "
        "value_type (text -> std::string_view, uint32 -> uint32_t, "
        "floating_point -> double, boolean -> bool)");

// Empty in production. When a test injects options via add_test_only_option(), this holds
// registry_options followed by the injected ones, and options()/find() consult it instead of
// registry_options. Per-shard state, mirroring how the registry is otherwise stateless.
//
// find() hands out pointers into this vector, so it must never reallocate while those
// pointers are live. Capacity is therefore reserved once, up front, and add_test_only_option()
// refuses to grow past it rather than silently invalidating outstanding pointers.
thread_local std::vector<option> g_options_with_test_overrides;

constexpr size_t max_test_only_options = 16;


// The validators and the to_* converters must agree on what parses, so both go through these:
// validate_value() rejects what they reject, and the converters accept exactly what a stored
// override could have been validated as.
//
// Boolean and floating-point values delegate to the CQL type system
// (abstract_type::from_string), so a cluster-config value accepts exactly what the
// corresponding CQL column type accepts and there is a single definition of what parses.
// uint32 has no CQL counterpart (CQL `int` is signed), so it keeps a strict from_chars
// parser.
std::optional<uint32_t> parse_uint32(std::string_view value) {
    uint32_t parsed = 0;
    const auto result = std::from_chars(value.begin(), value.end(), parsed);
    if (result.ec == std::errc() && result.ptr == value.end()) {
        return parsed;
    }
    return std::nullopt;
}

// Parse `value` with a CQL type, yielding the deserialized value or nullopt on failure.
// The empty string is rejected up front: from_string("") means a null (or false) value in
// the CQL type system, but an empty override is never a valid stored form here - removing
// an override is spelled `= NULL` and never reaches value validation.
std::optional<data_value> parse_with_cql_type(const data_type& type, std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }
    try {
        return type->deserialize(type->from_string(value));
    } catch (const marshal_exception&) {
        return std::nullopt;
    }
}

std::optional<double> parse_floating_point(std::string_view value) {
    if (const auto parsed = parse_with_cql_type(double_type, value)) {
        return value_cast<double>(*parsed);
    }
    return std::nullopt;
}

// The CQL BOOLEAN token is case-insensitive but case-preserving (Cql.g: `T R U E | F A L S E`),
// so `WITH opt = TRUE` reaches us as "TRUE". boolean_type accepts any spelling;
// canonicalize_value() folds it to the stored form.
std::optional<bool> parse_boolean(std::string_view value) {
    if (const auto parsed = parse_with_cql_type(boolean_type, value)) {
        return value_cast<bool>(*parsed);
    }
    return std::nullopt;
}

std::optional<seastar::sstring> validate_uint32(std::string_view value) {
    if (parse_uint32(value)) {
        return std::nullopt;
    }
    return fmt::format("expected unsigned 32-bit integer, got '{}'", value);
}

std::optional<seastar::sstring> validate_floating_point(std::string_view value) {
    if (parse_floating_point(value)) {
        return std::nullopt;
    }
    return fmt::format("expected floating-point number, got '{}'", value);
}

std::optional<seastar::sstring> validate_boolean(std::string_view value) {
    if (parse_boolean(value)) {
        return std::nullopt;
    }
    return fmt::format("expected 'true' or 'false', got '{}'", value);
}

// Shared implementation of the typed to_*() accessors: checks that the option was registered
// with the type the accessor reads, applies the registered default when no override is stored,
// and degrades an unparsable stored value (a row corrupted or written out-of-band) to the
// default with a warning. One definition, so the per-type accessors cannot drift apart in how
// they report a mismatched call or a bad row.
template <typename T>
T to_native(const option& opt, value_type expected_type, std::string_view type_name,
        const std::optional<seastar::sstring>& value,
        std::optional<T> (*parse)(std::string_view)) {
    if (opt.type != expected_type) {
        utils::on_internal_error(fmt::format(
                "cluster config '{}' read through the {} accessor but registered with a different type",
                opt.name, type_name));
    }
    if (!value) {
        return std::get<T>(opt.default_value);
    }
    if (const auto parsed = parse(*value)) {
        return *parsed;
    }
    cluster_config_registry_logger.warn(
            "Ignoring unparsable stored value '{}' for {} config '{}', using default",
            *value, type_name, opt.name);
    return std::get<T>(opt.default_value);
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
        return validate_boolean(value);
    }

    // Unreachable: the switch above is exhaustive over value_type. Matches the
    // __builtin_unreachable() idiom used elsewhere in the cluster-config statements.
    __builtin_unreachable();
}

bool to_boolean(const option& opt, const std::optional<seastar::sstring>& value) {
    return to_native<bool>(opt, value_type::boolean, "boolean", value, parse_boolean);
}

uint32_t to_uint32(const option& opt, const std::optional<seastar::sstring>& value) {
    return to_native<uint32_t>(opt, value_type::uint32, "uint32", value, parse_uint32);
}

double to_floating_point(const option& opt, const std::optional<seastar::sstring>& value) {
    return to_native<double>(opt, value_type::floating_point, "floating-point", value, parse_floating_point);
}

seastar::sstring to_text(const option& opt, const std::optional<seastar::sstring>& value) {
    // Not routed through to_native(): any text parses, so there is no failure path, and the
    // default is held as std::string_view rather than the returned sstring.
    if (opt.type != value_type::text) {
        utils::on_internal_error(fmt::format(
                "cluster config '{}' read through the text accessor but registered with a different type",
                opt.name));
    }
    return value ? *value : seastar::sstring(std::get<std::string_view>(opt.default_value));
}

seastar::sstring canonicalize_value(const option& opt, std::string_view value) {
    switch (opt.type) {
    case value_type::text:
    case value_type::uint32:
    case value_type::floating_point:
        return seastar::sstring(value);
    case value_type::boolean: {
        // Only reached after validate_value() accepted the input, so the parse cannot fail.
        const auto parsed = parse_boolean(value);
        if (!parsed) {
            utils::on_internal_error(fmt::format(
                    "canonicalize_value() called for boolean config '{}' with a value that did not pass validation: '{}'",
                    opt.name, value));
        }
        return *parsed ? "true" : "false";
    }
    }

    __builtin_unreachable();
}

void add_test_only_option(option opt) {
    // Same invariant registry_options is static_asserted against. An injected option that
    // mixed the two domains would resolve through a precedence chain that does not exist,
    // so reject it here rather than let a test build a nonsensical registry.
    if (!is_single_domain_scope_mask(opt.scope_mask)) {
        utils::on_internal_error(fmt::format(
                "test-only cluster config option '{}' mixes table-oriented and node-oriented scopes", opt.name));
    }
    // registry_options gets this checked at compile time; injected options can only be checked
    // here. Without it, a test that omits .default_value would silently get the variant's first
    // alternative (an empty text value) and any consumer reading the default would throw.
    if (!default_matches_type(opt)) {
        utils::on_internal_error(fmt::format(
                "test-only cluster config option '{}' has a default_value that does not match its value_type", opt.name));
    }
    if (g_options_with_test_overrides.empty()) {
        g_options_with_test_overrides.reserve(registry_options.size() + max_test_only_options);
        g_options_with_test_overrides.assign(registry_options.begin(), registry_options.end());
    }
    // Growing would reallocate and dangle every pointer previously returned by find().
    if (g_options_with_test_overrides.size() >= g_options_with_test_overrides.capacity()) {
        utils::on_internal_error(fmt::format(
                "too many test-only cluster config options (max {}); growing would dangle pointers handed out by find()",
                max_test_only_options));
    }
    g_options_with_test_overrides.push_back(opt);
}

void clear_test_only_options() {
    // Frees the buffer, so any pointer from a previous find() is invalid past this point.
    // Tests must not cache option pointers across a clear.
    g_options_with_test_overrides.clear();
    g_options_with_test_overrides.shrink_to_fit();
}

}
