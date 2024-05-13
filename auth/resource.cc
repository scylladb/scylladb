/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/resource.hh"

#include <algorithm>
#include <fmt/core.h>
#include <iterator>
#include <unordered_map>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "cql3/functions/aggregate_function.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/util.hh"
#include "db/marshal/type_parser.hh"
#include "log.hh"

namespace auth {

static logging::logger logger("auth_resource");

static const std::unordered_map<resource_kind, std::string_view> roots{
        {resource_kind::data, "data"},
        {resource_kind::role, "roles"},
        {resource_kind::service_level, "service_levels"},
        {resource_kind::functions, "functions"}};

static const std::unordered_map<resource_kind, std::size_t> max_parts{
        {resource_kind::data, 2},
        {resource_kind::role, 1},
        {resource_kind::service_level, 0},
        {resource_kind::functions, 2}};

static permission_set applicable_permissions(const data_resource_view& dv) {
    if (dv.table()) {
        return permission_set::of<
                permission::ALTER,
                permission::DROP,
                permission::SELECT,
                permission::MODIFY,
                permission::AUTHORIZE>();
    }

    return permission_set::of<
            permission::CREATE,
            permission::ALTER,
            permission::DROP,
            permission::SELECT,
            permission::MODIFY,
            permission::AUTHORIZE>();
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

sstring resource::name() const {
    return boost::algorithm::join(_parts, "/");
}

std::optional<resource> resource::parent() const {
    if (_parts.size() == 1) {
        return {};
    }

    resource copy = *this;
    copy._parts.pop_back();
    return copy;
}

permission_set resource::applicable_permissions() const {
    permission_set ps;

    switch (_kind) {
        case resource_kind::data: ps = ::auth::applicable_permissions(data_resource_view(*this)); break;
        case resource_kind::role: ps = ::auth::applicable_permissions(role_resource_view(*this)); break;
        case resource_kind::service_level: ps = ::auth::applicable_permissions(service_level_resource_view(*this)); break;
        case resource_kind::functions: ps = ::auth::applicable_permissions(functions_resource_view(*this)); break;
    }

    return ps;
}

bool operator<(const resource& r1, const resource& r2) {
    if (r1._kind != r2._kind) {
        return r1._kind < r2._kind;
    }

    return std::lexicographical_compare(
            r1._parts.cbegin() + 1,
            r1._parts.cend(),
            r2._parts.cbegin() + 1,
            r2._parts.cend());
}

service_level_resource_view::service_level_resource_view(const resource &r) {
    if (r._kind != resource_kind::service_level) {
        throw resource_kind_mismatch(resource_kind::service_level, r._kind);
    }
}

sstring encode_signature(std::string_view name, std::vector<data_type> args) {
    return format("{}[{}]", name,
            fmt::join(args | boost::adaptors::transformed([] (const data_type t) {
                return t->name();
            }), "^"));
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
    std::vector<data_type> decoded_types = boost::copy_range<std::vector<data_type>>(
        raw_types | boost::adaptors::transformed([] (std::string_view raw_type) {
            return db::marshal::type_parser::parse(raw_type);
        })
    );
    return {sstring(function_name), decoded_types};
}

// Purely for Cassandra compatibility, types in the function signature are
// decoded from their verbose form (org.apache.cassandra.db.marshal.Int32Type)
// to the short form (int)
static sstring decoded_signature_string(std::string_view encoded_signature) {
    auto [function_name, arg_types] = decode_signature(encoded_signature);
    return format("{}({})", cql3::util::maybe_quote(sstring(function_name)),
            boost::algorithm::join(arg_types | boost::adaptors::transformed([] (data_type t) {
                return t->cql3_type_name();
            }), ", "));
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

    if (parts.size() > max_parts.at(kind)) {
        throw invalid_resource_name(name);
    }

    return resource(kind, std::move(parts));
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
    }
    return out;
}

