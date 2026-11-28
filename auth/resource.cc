/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "auth/resource.hh"

#include <algorithm>
#include <cctype>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <iterator>
#include <ranges>
#include <string>
#include <unordered_map>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "cql3/functions/aggregate_function.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/util.hh"
#include "db/marshal/type_parser.hh"
#include "utils/log.hh"

namespace auth {

static logging::logger logger("auth_resource");

static const std::unordered_map<resource_kind, std::string_view> roots{
        {resource_kind::data, "data"},
        {resource_kind::role, "roles"},
        {resource_kind::service_level, "service_levels"},
        {resource_kind::functions, "functions"},
        {resource_kind::object_storage, "object_storage"},
        {resource_kind::cluster, "cluster"}};

static const std::unordered_map<resource_kind, std::size_t> max_parts{
        {resource_kind::data, 2},
        {resource_kind::role, 1},
        {resource_kind::service_level, 0},
        {resource_kind::functions, 2},
        {resource_kind::object_storage, 0},
        {resource_kind::cluster, 3}};

// The last part of the name of a config option sub-resource is this prefix followed by
// the option name. No keyspace or table name contains '@' (CQL names are word characters;
// Alternator adds '.', '-', and ':' and '!' in view names), and datacenter and rack names
// are stored encoded, so the part cannot be mistaken for one of those.
static constexpr std::string_view config_option_prefix = "@config:";

// Datacenter and rack names are free-form, while '/' separates the parts of a resource
// name. Everything outside [A-Za-z0-9_.-] is percent-encoded in the stored name.
static sstring encode_part(std::string_view s) {
    std::string out;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '_' || c == '.' || c == '-') {
            out.push_back(c);
        } else {
            fmt::format_to(std::back_inserter(out), "%{:02X}", unsigned(c));
        }
    }
    return sstring(out);
}

static sstring decode_part(std::string_view s, std::string_view name) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] != '%') {
            out.push_back(s[i]);
            continue;
        }
        if (i + 2 >= s.size() || !std::isxdigit(static_cast<unsigned char>(s[i + 1])) || !std::isxdigit(static_cast<unsigned char>(s[i + 2]))) {
            throw invalid_resource_name(name);
        }
        out.push_back(static_cast<char>(std::stoi(std::string(s.substr(i + 1, 2)), nullptr, 16)));
        i += 2;
    }
    return sstring(out);
}

static permission_set applicable_permissions(const data_resource_view& dv) {

    // We only support the VECTOR_SEARCH_INDEXING, TEXT_SEARCH_INDEXING and PATTERN_SEARCH_INDEXING permissions for ALL KEYSPACES.

    auto set = permission_set::of<
                permission::ALTER,
                permission::DROP,
                permission::SELECT,
                permission::MODIFY,
                permission::AUTHORIZE,
                permission::CONFIGURE>();

    if (!dv.table()) {
        set.add(permission_set::of<permission::CREATE>());
    }

    if (!dv.table() && !dv.keyspace()) {
        set.add(permission_set::of<permission::VECTOR_SEARCH_INDEXING, permission::TEXT_SEARCH_INDEXING, permission::PATTERN_SEARCH_INDEXING>());
    }

    return set;

}

static permission_set applicable_permissions(const role_resource_view& rv) {
    if (rv.role()) {
        return permission_set::of<permission::ALTER, permission::DROP, permission::AUTHORIZE>();
    }

    return permission_set::of<
            permission::CREATE,
            permission::ALTER,
            permission::DROP,
            permission::AUTHORIZE,
            permission::DESCRIBE>();
}

static permission_set applicable_permissions(const service_level_resource_view &rv) {
    return permission_set::of<
            permission::CREATE,
            permission::ALTER,
            permission::DROP,
            permission::DESCRIBE,
            permission::AUTHORIZE>();
}

static permission_set applicable_permissions(const functions_resource_view& fv) {
    if (fv.function_name() || fv.function_signature()) {
        return permission_set::of<
                permission::ALTER,
                permission::DROP,
                permission::AUTHORIZE,
                permission::EXECUTE>();
    }
    return permission_set::of<
            permission::CREATE,
            permission::ALTER,
            permission::DROP,
            permission::AUTHORIZE,
            permission::EXECUTE>();
}

static permission_set applicable_permissions(const object_storage_resource_view&) {
    return permission_set::of<
            permission::SELECT,
            permission::MODIFY,
            permission::AUTHORIZE>();
}

static permission_set applicable_permissions(const cluster_resource_view&) {
    return permission_set::of<permission::CONFIGURE, permission::AUTHORIZE>();
}

resource::resource(resource_kind kind) : _kind(kind) {
    _parts.emplace_back(roots.at(kind));
}

resource::resource(resource_kind kind, utils::small_vector<sstring, 3> parts) : resource(kind) {
    _parts.insert(_parts.end(), std::make_move_iterator(parts.begin()), std::make_move_iterator(parts.end()));
}

resource::resource(data_resource_t, std::string_view keyspace) : resource(resource_kind::data) {
    _parts.emplace_back(keyspace);
}

resource::resource(data_resource_t, std::string_view keyspace, std::string_view table) : resource(resource_kind::data) {
    _parts.emplace_back(keyspace);
    _parts.emplace_back(table);
}

resource::resource(role_resource_t, std::string_view role) : resource(resource_kind::role) {
    _parts.emplace_back(role);
}

resource::resource(service_level_resource_t): resource(resource_kind::service_level) {
}

resource::resource(functions_resource_t) : resource(resource_kind::functions) {
}

resource::resource(functions_resource_t, std::string_view keyspace) : resource(resource_kind::functions) {
    _parts.emplace_back(keyspace);
}

resource::resource(functions_resource_t, std::string_view keyspace, std::string_view function_signature) : resource(resource_kind::functions) {
    _parts.emplace_back(keyspace);
    _parts.emplace_back(function_signature);
}

resource::resource(functions_resource_t, std::string_view keyspace, std::string_view function_name, std::vector<::shared_ptr<cql3::cql3_type::raw>> function_args) : resource(resource_kind::functions) {
    _parts.emplace_back(keyspace);
    _parts.emplace_back(function_name);
    if (function_args.empty()) {
        _parts.emplace_back("");
        return;
    }
    for (auto& arg_type : function_args) {
        // We can't validate the UDTs here, so we just use the raw cql type names.
        _parts.emplace_back(arg_type->to_string());
    }
}

resource::resource(cluster_resource_t) : resource(resource_kind::cluster) {
}

resource::resource(cluster_resource_t, std::string_view dc) : resource(resource_kind::cluster) {
    _parts.emplace_back(dc);
}

resource::resource(cluster_resource_t, std::string_view dc, std::string_view rack) : resource(resource_kind::cluster) {
    _parts.emplace_back(dc);
    _parts.emplace_back(rack);
}

resource::resource(cluster_resource_t, std::string_view dc, std::string_view rack, std::string_view host_id) : resource(resource_kind::cluster) {
    _parts.emplace_back(dc);
    _parts.emplace_back(rack);
    _parts.emplace_back(host_id);
}

sstring resource::name() const {
    sstring base;
    if (_kind == resource_kind::cluster) {
        base = fmt::to_string(fmt::join(_parts | std::views::transform(encode_part), "/"));
    } else {
        base = fmt::to_string(fmt::join(_parts, "/"));
    }
    if (_config_option) {
        return seastar::format("{}/{}{}", base, config_option_prefix, *_config_option);
    }
    return base;
}

resource resource::with_config_option(std::string_view option) const {
    resource copy = *this;
    copy._config_option = sstring(option);
    return copy;
}

resource resource::without_config_option() const {
    resource copy = *this;
    copy._config_option.reset();
    return copy;
}

std::optional<resource> resource::parent() const {
    if (_config_option) {
        return without_config_option();
    }

    if (_parts.size() == 1) {
        return {};
    }

    resource copy = *this;
    copy._parts.pop_back();
    return copy;
}

permission_set resource::applicable_permissions() const {
    if (_config_option) {
        return permission_set::of<permission::CONFIGURE>();
    }

    permission_set ps;

    switch (_kind) {
        case resource_kind::data: ps = ::auth::applicable_permissions(data_resource_view(*this)); break;
        case resource_kind::role: ps = ::auth::applicable_permissions(role_resource_view(*this)); break;
        case resource_kind::service_level: ps = ::auth::applicable_permissions(service_level_resource_view(*this)); break;
        case resource_kind::functions: ps = ::auth::applicable_permissions(functions_resource_view(*this)); break;
        case resource_kind::object_storage: ps = ::auth::applicable_permissions(object_storage_resource_view(*this)); break;
        case resource_kind::cluster: ps = ::auth::applicable_permissions(cluster_resource_view(*this)); break;
    }

    return ps;
}

bool operator<(const resource& r1, const resource& r2) {
    if (r1._kind != r2._kind) {
        return r1._kind < r2._kind;
    }

    if (!std::ranges::equal(r1._parts, r2._parts)) {
        return std::lexicographical_compare(
                r1._parts.cbegin() + 1,
                r1._parts.cend(),
                r2._parts.cbegin() + 1,
                r2._parts.cend());
    }

    return r1._config_option < r2._config_option;
}

cluster_resource_view::cluster_resource_view(const resource& r) : _resource(r) {
    if (r._kind != resource_kind::cluster) {
        throw resource_kind_mismatch(resource_kind::cluster, r._kind);
    }
}

std::optional<std::string_view> cluster_resource_view::datacenter() const {
    if (_resource._parts.size() <= 1) {
        return {};
    }
    return _resource._parts[1];
}

std::optional<std::string_view> cluster_resource_view::rack() const {
    if (_resource._parts.size() <= 2) {
        return {};
    }
    return _resource._parts[2];
}

std::optional<std::string_view> cluster_resource_view::host_id() const {
    if (_resource._parts.size() <= 3) {
        return {};
    }
    return _resource._parts[3];
}

service_level_resource_view::service_level_resource_view(const resource &r) {
    if (r._kind != resource_kind::service_level) {
        throw resource_kind_mismatch(resource_kind::service_level, r._kind);
    }
}

sstring encode_signature(std::string_view name, std::vector<data_type> args) {
    return seastar::format("{}[{}]", name,
            fmt::join(args | std::views::transform(&abstract_type::name), "^"));
}

std::pair<sstring, std::vector<data_type>> decode_signature(std::string_view encoded_signature) {
    auto name_delim = encoded_signature.find_last_of('[');
    std::string_view function_name = encoded_signature.substr(0, name_delim);
    encoded_signature.remove_prefix(name_delim + 1);
    encoded_signature.remove_suffix(1);
    if (encoded_signature.empty()) {
        return {sstring(function_name), {}};
    }
    std::vector<std::string_view> raw_types;
    boost::split(raw_types, encoded_signature, boost::is_any_of("^"));
    std::vector<data_type> decoded_types =
        raw_types | std::views::transform([] (std::string_view raw_type) {
            return db::marshal::type_parser::parse(raw_type);
        }) | std::ranges::to<std::vector>();
    return {sstring(function_name), decoded_types};
}

// Purely for Cassandra compatibility, types in the function signature are
// decoded from their verbose form (org.apache.cassandra.db.marshal.Int32Type)
// to the short form (int)
static sstring decoded_signature_string(std::string_view encoded_signature) {
    auto [function_name, arg_types] = decode_signature(encoded_signature);
    return seastar::format("{}({})", cql3::util::maybe_quote(sstring(function_name)),
            fmt::join(arg_types | std::views::transform(&abstract_type::cql3_type_name), ", "));
}

resource make_functions_resource(const cql3::functions::function& f) {
    if (!dynamic_cast<const cql3::functions::user_function*>(&f) &&
            !dynamic_cast<const cql3::functions::aggregate_function*>(&f)) {
        on_internal_error(logger, "unsuppported function type");
    }
    auto&& sig = auth::encode_signature(f.name().name, f.arg_types());
    return make_functions_resource(f.name().keyspace, sig);
}

functions_resource_view::functions_resource_view(const resource& r) : _resource(r) {
    if (r._kind != resource_kind::functions) {
        throw resource_kind_mismatch(resource_kind::functions, r._kind);
    }
}

std::optional<std::string_view> functions_resource_view::keyspace() const {
    if (_resource._parts.size() == 1) {
        return {};
    }

    return _resource._parts[1];
}

std::optional<std::string_view> functions_resource_view::function_signature() const {
    if (_resource._parts.size() <= 2 || _resource._parts.size() > 3) {
        return {};
    }

    return _resource._parts[2];
}

std::optional<std::string_view> functions_resource_view::function_name() const {
    if (_resource._parts.size() <= 3) {
        return {};
    }

    return _resource._parts[2];
}

std::optional<std::vector<std::string_view>> functions_resource_view::function_args() const {
    if (_resource._parts.size() <= 3) {
        return {};
    }

    std::vector<std::string_view> parts;
    if (_resource._parts[3] == "") {
        return parts;
    }
    for (size_t i = 3; i < _resource._parts.size(); i++) {
        parts.push_back(_resource._parts[i]);
    }
    return parts;
}

object_storage_resource_view::object_storage_resource_view(const resource& r) {
    if (r._kind != resource_kind::object_storage) {
        throw resource_kind_mismatch(resource_kind::object_storage, r._kind);
    }
}

data_resource_view::data_resource_view(const resource& r) : _resource(r) {
    if (r._kind != resource_kind::data) {
        throw resource_kind_mismatch(resource_kind::data, r._kind);
    }
}

std::optional<std::string_view> data_resource_view::keyspace() const {
    if (_resource._parts.size() == 1) {
        return {};
    }

    return _resource._parts[1];
}

bool data_resource_view::is_keyspace() const {
    return _resource._parts.size() == 2;
}

std::optional<std::string_view> data_resource_view::table() const {
    if (_resource._parts.size() <= 2) {
        return {};
    }

    return _resource._parts[2];
}

role_resource_view::role_resource_view(const resource& r) : _resource(r) {
    if (r._kind != resource_kind::role) {
        throw resource_kind_mismatch(resource_kind::role, r._kind);
    }
}

std::optional<std::string_view> role_resource_view::role() const {
    if (_resource._parts.size() == 1) {
        return {};
    }

    return _resource._parts[1];
}

resource parse_resource(std::string_view name) {
    static const std::unordered_map<std::string_view, resource_kind> reverse_roots = [] {
        std::unordered_map<std::string_view, resource_kind> result;

        for (const auto& pair : roots) {
            result.emplace(pair.second, pair.first);
        }

        return result;
    }();

    utils::small_vector<sstring, 3> parts;
    boost::split(parts, name, [](char ch) { return ch == '/'; });

    if (parts.empty()) {
        throw invalid_resource_name(name);
    }

    const auto iter = reverse_roots.find(parts[0]);
    if (iter == reverse_roots.end()) {
        throw invalid_resource_name(name);
    }

    const auto kind = iter->second;
    parts.erase(parts.begin());

    // Other kinds have no config options; a role or a function may be called "@config:...".
    std::optional<sstring> config_option;
    if ((kind == resource_kind::data || kind == resource_kind::cluster)
            && !parts.empty() && parts.back().starts_with(config_option_prefix)) {
        config_option = parts.back().substr(config_option_prefix.size());
        parts.pop_back();
    }

    if (parts.size() > max_parts.at(kind)) {
        throw invalid_resource_name(name);
    }

    if (kind == resource_kind::cluster) {
        for (auto& part : parts) {
            part = decode_part(part, name);
        }
    }

    resource r(kind, std::move(parts));
    r._config_option = std::move(config_option);
    return r;
}

static const resource the_root_data_resource{resource_kind::data};

const resource& root_data_resource() {
    return the_root_data_resource;
}

static const resource the_root_role_resource{resource_kind::role};

const resource& root_role_resource() {
    return the_root_role_resource;
}

static const resource the_root_service_level_resource{resource_kind::service_level};

const resource &root_service_level_resource() {
    return the_root_service_level_resource;
}

static const resource the_root_object_storage_resource{resource_kind::object_storage};

const resource& root_object_storage_resource() {
    return the_root_object_storage_resource;
}

static const resource the_root_cluster_resource{resource_kind::cluster};

const resource& root_cluster_resource() {
    return the_root_cluster_resource;
}

std::optional<resource> config_scope_parent(const resource& r) {
    if (auto p = r.parent()) {
        return p;
    }
    if (r.kind() == resource_kind::data) {
        return the_root_cluster_resource;
    }
    return {};
}

resource_set expand_resource_family(const resource& rr) {
    resource r = rr;
    resource_set rs;

    while (true) {
        const auto pr = r.parent();
        rs.insert(std::move(r));

        if (!pr) {
            break;
        }

        r = std::move(*pr);
    }

    return rs;
}

}

template <> struct fmt::formatter<auth::role_resource_view> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const auth::role_resource_view& v, fmt::format_context& ctx) const {
        const auto role = v.role();
        if (role) {
            return fmt::format_to(ctx.out(), "<role {}>", *role);
        } else {
            return fmt::format_to(ctx.out(), "<all roles>");
        }
    }
};

template <> struct fmt::formatter<auth::service_level_resource_view> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const auth::service_level_resource_view& v, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "<all service levels>");
    }
};

template <> struct fmt::formatter<auth::functions_resource_view> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const auth::functions_resource_view& v, fmt::format_context& ctx) const {
        const auto keyspace = v.keyspace();
        if (!keyspace) {
            return fmt::format_to(ctx.out(), "<all functions>");
        }

        const auto name = v.function_name();
        if (name) {
            auto out = ctx.out();
            out = fmt::format_to(out, "<function {}.{}(", *keyspace, cql3::util::maybe_quote(sstring(*name)));
            const auto args = v.function_args();
            for (auto arg : *args) {
                out = fmt::format_to(out, "{},", arg);
            }
            return fmt::format_to(out, ")>");
        }

        const auto function_signature = v.function_signature();
        if (!function_signature) {
            return fmt::format_to(ctx.out(), "<all functions in {}>", *keyspace);
        }

        return fmt::format_to(ctx.out(), "<function {}.{}>", *keyspace, auth::decoded_signature_string(*function_signature));
    }
};

template <> struct fmt::formatter<auth::object_storage_resource_view> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const auth::object_storage_resource_view&, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "<all object storage>");
    }
};

template <> struct fmt::formatter<auth::cluster_resource_view> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const auth::cluster_resource_view& v, fmt::format_context& ctx) const {
        if (const auto host_id = v.host_id()) {
            return fmt::format_to(ctx.out(), "<node {}>", *host_id);
        }
        if (const auto rack = v.rack()) {
            return fmt::format_to(ctx.out(), "<rack {} {}>", *v.datacenter(), *rack);
        }
        if (const auto dc = v.datacenter()) {
            return fmt::format_to(ctx.out(), "<datacenter {}>", *dc);
        }
        return fmt::format_to(ctx.out(), "<all scopes>");
    }
};

auto fmt::formatter<auth::data_resource_view>::format(const auth::data_resource_view& v,
                                                      fmt::format_context& ctx) const -> decltype(ctx.out()) {
    const auto keyspace = v.keyspace();
    if (!keyspace) {
        return fmt::format_to(ctx.out(), "<all keyspaces>");
    }

    const auto table = v.table();
    if (!table) {
        return fmt::format_to(ctx.out(), "<keyspace {}>", *keyspace);
    }
    return fmt::format_to(ctx.out(), "<table {}.{}>", *keyspace, *table);
}

auto fmt::formatter<auth::resource>::format(const auth::resource& r,
                                            fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto out = ctx.out();
    switch (r.kind()) {
    using enum auth::resource_kind;
    case data: return fmt::format_to(out, "{}", auth::data_resource_view(r));
    case role: return fmt::format_to(out, "{}", auth::role_resource_view(r));
    case service_level: return fmt::format_to(out, "{}", auth::service_level_resource_view(r));
    case functions: return fmt::format_to(out, "{}", auth::functions_resource_view(r));
    case object_storage: return fmt::format_to(out, "{}", auth::object_storage_resource_view(r));
    case cluster: return fmt::format_to(out, "{}", auth::cluster_resource_view(r));
    }
    return out;
}

