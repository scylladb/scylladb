/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/cluster_config_registry.hh"

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <seastar/core/smp.hh>

#include "gms/feature_service.hh"
#include "types/types.hh"
#include "utils/chunked_string.hh"
#include "utils/log.hh"
#include "utils/on_internal_error.hh"

namespace db::cluster_config_registry {

namespace {

logging::logger cluster_config_registry_logger("cluster_config_registry");

// option::type() is the index of the default's alternative, so the two must line up.
static_assert(std::is_same_v<std::variant_alternative_t<size_t(value_type::text), config_value>, std::string_view>);
static_assert(std::is_same_v<std::variant_alternative_t<size_t(value_type::integer), config_value>, int64_t>);
static_assert(std::is_same_v<std::variant_alternative_t<size_t(value_type::floating_point), config_value>, double>);
static_assert(std::is_same_v<std::variant_alternative_t<size_t(value_type::boolean), config_value>, bool>);
static_assert(std::variant_size_v<config_value> == 4);

// The two resolution domains. CLUSTER is the shared last step of both chains, so it belongs
// to both sets; an option that wants only part of a chain spells its scopes out.
constexpr scope_set table_oriented_scopes = scope_set::of<scope::cluster, scope::keyspace, scope::table>();
// Unused until the first node-oriented option ships.
[[maybe_unused]] constexpr scope_set node_oriented_scopes = scope_set::of<scope::cluster, scope::datacenter, scope::rack, scope::node>();

// The scopes that place an option in one domain or the other, i.e. each set without CLUSTER.
constexpr scope_set table_only_scopes = scope_set::of<scope::keyspace, scope::table>();
constexpr scope_set node_only_scopes = scope_set::of<scope::datacenter, scope::rack, scope::node>();

// An option's scopes must belong to a single resolution domain: CLUSTER may combine with
// either, but KEYSPACE/TABLE must never be mixed with DATACENTER/RACK/NODE.
constexpr bool is_single_domain(scope_set scopes) {
    return !(scopes.intersects(table_only_scopes) && scopes.intersects(node_only_scopes));
}

constexpr std::array registry_options = {
    option{
        .name = "auto_repair_enabled",
        .description = "Enable automatic repair for tablet-based tables",
        .scopes = table_oriented_scopes,
        .min_version = version::v0,
        .default_value = false,
    },
};

constexpr bool all_registry_options_are_single_domain() {
    for (const auto& opt : registry_options) {
        if (!is_single_domain(opt.scopes)) {
            return false;
        }
    }
    return true;
}

static_assert(all_registry_options_are_single_domain(),
        "A cluster config option must not mix table-oriented (KEYSPACE/TABLE) and "
        "node-oriented (DATACENTER/RACK/NODE) scopes");

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
// Values delegate to the CQL type system (abstract_type::from_string) - bigint, double and
// boolean - so a cluster-config value accepts exactly what the corresponding CQL column
// type accepts and there is a single definition of what parses.
//
// Parse `value` with a CQL type. Throws marshal_exception, with the CQL type's own reason,
// when the value does not parse. The empty string is rejected up front: from_string("")
// means a null (or false) value in the CQL type system, but an empty override is never a
// valid stored form here - removing an override is spelled `= NULL` and never reaches
// value validation.
data_value parse_with_cql_type(const data_type& type, std::string_view value) {
    if (value.empty()) {
        throw marshal_exception("empty value");
    }
    return type->deserialize(type->from_string(value));
}

int64_t parse_integer(std::string_view value) {
    return value_cast<int64_t>(parse_with_cql_type(long_type, value));
}

double parse_floating_point(std::string_view value) {
    return value_cast<double>(parse_with_cql_type(double_type, value));
}

// The CQL BOOLEAN token is case-insensitive but case-preserving (Cql.g: `T R U E | F A L S E`),
// so `WITH opt = TRUE` reaches us as "TRUE". boolean_type accepts any spelling;
// canonicalize_value() folds it to the stored form.
bool parse_boolean(std::string_view value) {
    return value_cast<bool>(parse_with_cql_type(boolean_type, value));
}

// Runs a parser on behalf of validate_value(): nullopt when the value parses, otherwise the
// expected form together with the parser's own reason.
template <typename T>
std::optional<seastar::sstring> validate_with(T (*parse)(std::string_view), std::string_view expected, std::string_view value) {
    try {
        parse(value);
        return std::nullopt;
    } catch (const marshal_exception& e) {
        return fmt::format("expected {}, got '{}': {}", expected, value, e.what());
    }
}

std::optional<seastar::sstring> validate_integer(std::string_view value) {
    return validate_with(parse_integer, "64-bit integer", value);
}

std::optional<seastar::sstring> validate_floating_point(std::string_view value) {
    return validate_with(parse_floating_point, "floating-point number", value);
}

std::optional<seastar::sstring> validate_boolean(std::string_view value) {
    return validate_with(parse_boolean, "'true' or 'false'", value);
}

// Nothing above the registry bounds a text value (it is a map cell in a schema table), and it
// is echoed by DESCRIBE, so cap it here.
constexpr size_t max_text_value_length = 4096;

// A text value is otherwise unconstrained, but it has to survive being echoed back as CQL.
// DESCRIBE renders every effective value both as a property right-hand side and inside a
// `-- ...` provenance comment, and a described CDC log or paxos table wraps the whole
// statement in a `/* ... */` block. A newline would end the line comment and turn the rest
// of the value into CQL; a `*/` would end the block comment. Neither can be escaped inside
// a CQL string literal, so they are rejected at the door instead - the alternative is a
// dump that silently stops being replayable.
std::optional<seastar::sstring> validate_text(std::string_view value) {
    if (value.size() > max_text_value_length) {
        return fmt::format("must not exceed {} bytes", max_text_value_length);
    }
    if (value.find('\n') != std::string_view::npos || value.find('\r') != std::string_view::npos) {
        return seastar::sstring("must not contain a line break");
    }
    if (value.find("*/") != std::string_view::npos) {
        return seastar::sstring("must not contain '*/'");
    }
    return std::nullopt;
}

// Shared implementation of the typed to_*() accessors: checks that the option was registered
// with the type the accessor reads, applies the registered default when no override is stored,
// and degrades an unparsable stored value (a row corrupted or written out-of-band) to the
// default with a warning. One definition, so the per-type accessors cannot drift apart in how
// they report a mismatched call or a bad row.
template <typename T>
T to_native(const option& opt, value_type expected_type, std::string_view type_name,
        const std::optional<seastar::sstring>& value,
        T (*parse)(std::string_view)) {
    if (opt.type() != expected_type) {
        utils::on_internal_error(fmt::format(
                "cluster config '{}' read through the {} accessor but registered with a different type",
                opt.name, type_name));
    }
    if (!value) {
        return std::get<T>(opt.default_value);
    }
    try {
        return parse(*value);
    } catch (const marshal_exception& e) {
        cluster_config_registry_logger.warn(
                "Ignoring unparsable stored value '{}' for {} config '{}', using default: {}",
                *value, type_name, opt.name, e.what());
        return std::get<T>(opt.default_value);
    }
}

}

std::span<const option> options() {
    if (!g_options_with_test_overrides.empty()) {
        return g_options_with_test_overrides;
    }
    return registry_options;
}

const option* find(std::string_view name, std::optional<version> max_version) {
    for (const auto& opt : options()) {
        if (opt.name == name) {
            return (max_version && *max_version < opt.min_version) ? nullptr : &opt;
        }
    }
    return nullptr;
}

bool supports_scope(const option& opt, scope s) {
    return opt.scopes.contains(s);
}

bool is_table_oriented(const option& opt) {
    return opt.scopes.intersects(table_only_scopes);
}

std::optional<version> current_version(const gms::feature_service& features) {
    if (features.cluster_config_registry_v0) {
        return version::v0;
    }
    return std::nullopt;
}

std::optional<seastar::sstring> validate_value(const option& opt, std::string_view value) {
    switch (opt.type()) {
    case value_type::text:
        return validate_text(value);
    case value_type::integer:
        return validate_integer(value);
    case value_type::floating_point:
        return validate_floating_point(value);
    case value_type::boolean:
        return validate_boolean(value);
    }

    __builtin_unreachable();
}

bool to_boolean(const option& opt, const std::optional<seastar::sstring>& value) {
    return to_native<bool>(opt, value_type::boolean, "boolean", value, parse_boolean);
}

int64_t to_integer(const option& opt, const std::optional<seastar::sstring>& value) {
    return to_native<int64_t>(opt, value_type::integer, "integer", value, parse_integer);
}

double to_floating_point(const option& opt, const std::optional<seastar::sstring>& value) {
    return to_native<double>(opt, value_type::floating_point, "floating-point", value, parse_floating_point);
}

seastar::sstring to_text(const option& opt, const std::optional<seastar::sstring>& value) {
    // Not routed through to_native(): any text parses, so there is no failure path, and the
    // default is held as std::string_view rather than the returned sstring.
    if (opt.type() != value_type::text) {
        utils::on_internal_error(fmt::format(
                "cluster config '{}' read through the text accessor but registered with a different type",
                opt.name));
    }
    return value ? *value : seastar::sstring(std::get<std::string_view>(opt.default_value));
}

seastar::sstring canonicalize_value(const option& opt, std::string_view value) {
    switch (opt.type()) {
    case value_type::text:
    case value_type::integer:
    case value_type::floating_point:
        return seastar::sstring(value);
    case value_type::boolean:
        // Only reached after validate_value() accepted the input, so the parse cannot fail.
        try {
            return parse_boolean(value) ? "true" : "false";
        } catch (const marshal_exception& e) {
            utils::on_internal_error(fmt::format(
                    "canonicalize_value() called for boolean config '{}' with a value that did not pass validation: '{}': {}",
                    opt.name, value, e.what()));
        }
    }

    __builtin_unreachable();
}

namespace {

// Per-shard half of add_test_only_option_on_all_shards().
void add_test_only_option(option opt) {
    // Same invariant registry_options is static_asserted against. An injected option that
    // mixed the two domains would resolve through a precedence chain that does not exist,
    // so reject it here rather than let a test build a nonsensical registry.
    if (!is_single_domain(opt.scopes)) {
        utils::on_internal_error(fmt::format(
                "test-only cluster config option '{}' mixes table-oriented and node-oriented scopes", opt.name));
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

}

seastar::future<> add_test_only_option_on_all_shards(option opt) {
    return seastar::smp::invoke_on_all([opt] {
        add_test_only_option(opt);
    });
}

seastar::future<> clear_test_only_options_on_all_shards() {
    return seastar::smp::invoke_on_all([] {
        // Frees the buffer, so any pointer from a previous find() is invalid past this point.
        // Tests must not cache option pointers across a clear.
        g_options_with_test_overrides.clear();
        g_options_with_test_overrides.shrink_to_fit();
    });
}

}
