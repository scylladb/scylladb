/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <string_view>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <vector>
#include <unordered_set>

#include <fmt/core.h>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include "auth/permission.hh"
#include "cql3/functions/function.hh"
#include "seastarx.hh"
#include "utils/hash.hh"
#include "utils/small_vector.hh"
#include "cql3/cql3_type.hh"

namespace auth {

class invalid_resource_name : public std::invalid_argument {
public:
    explicit invalid_resource_name(std::string_view name)
            : std::invalid_argument(format("The resource name '{}' is invalid.", name)) {
    }
};

enum class resource_kind {
    data, role, service_level, functions
};

}

template <>
struct fmt::formatter<auth::resource_kind> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const auth::resource_kind kind, FormatContext& ctx) const {
        using enum auth::resource_kind;
        switch (kind) {
        case data:
            return formatter<string_view>::format("data", ctx);
        case role:
            return formatter<string_view>::format("role", ctx);
        case service_level:
            return formatter<string_view>::format("service_level", ctx);
        case functions:
            return formatter<string_view>::format("functions", ctx);
        }
        std::abort();
    }
};

namespace auth {
///
/// Type tag for constructing data resources.
///
struct data_resource_t final {};

///
/// Type tag for constructing role resources.
///
struct role_resource_t final {};

///
/// Type tag for constructing service_level resources.
///
struct service_level_resource_t final {};

///
/// Type tag for constructing function resources.
///
struct functions_resource_t final {};

///
/// Resources are entities that users can be granted permissions on.
///
/// There are data (keyspaces and tables), role and function resources. There may be other kinds of resources in the future.
///
/// When they are stored as system metadata, resources have the form `root/part_0/part_1/.../part_n`. Each kind of
/// resource has a specific root prefix, followed by a maximum of `n` parts (where `n` is distinct for each kind of
/// resource as well). In this code, this form is called the "name".
///
/// Since all resources have this same structure, all the different kinds are stored in instances of the same class:
/// \ref resource. When we wish to query a resource for kind-specific data (like the table of a "data" resource), we
/// create a kind-specific "view" of the resource.
///
class resource final {
    resource_kind _kind;

    utils::small_vector<sstring, 3> _parts;

public:
    ///
    /// A root resource of a particular kind.
    ///
    explicit resource(resource_kind);
    resource(data_resource_t, std::string_view keyspace);
    resource(data_resource_t, std::string_view keyspace, std::string_view table);
    resource(role_resource_t, std::string_view role);
    resource(service_level_resource_t);
    explicit resource(functions_resource_t);
    resource(functions_resource_t, std::string_view keyspace);
    resource(functions_resource_t, std::string_view keyspace, std::string_view function_signature);
    resource(functions_resource_t, std::string_view keyspace, std::string_view function_name,
            std::vector<::shared_ptr<cql3::cql3_type::raw>> function_args);

    resource_kind kind() const noexcept {
        return _kind;
    }

    ///
    /// A machine-friendly identifier unique to each resource.
    ///
    sstring name() const;

    std::optional<resource> parent() const;

    permission_set applicable_permissions() const;

private:
    resource(resource_kind, utils::small_vector<sstring, 3> parts);

    friend class std::hash<resource>;
    friend class data_resource_view;
    friend class role_resource_view;
    friend class service_level_resource_view;
    friend class functions_resource_view;

    friend bool operator<(const resource&, const resource&);
    friend bool operator==(const resource&, const resource&) = default;
    friend resource parse_resource(std::string_view);
};

bool operator<(const resource&, const resource&);

class resource_kind_mismatch : public std::invalid_argument {
public:
    explicit resource_kind_mismatch(resource_kind expected, resource_kind actual)
        : std::invalid_argument(
            format("This resource has kind '{}', but was expected to have kind '{}'.", actual, expected)) {
    }
};

/// A "data" view of \ref resource.
///
/// If neither `keyspace` nor `table` is present, this is the root resource.
class data_resource_view final {
    const resource& _resource;

public:
    ///
    /// \throws `resource_kind_mismatch` if the argument is not a `data` resource.
    ///
    explicit data_resource_view(const resource& r);

    std::optional<std::string_view> keyspace() const;
    bool is_keyspace() const;

    std::optional<std::string_view> table() const;
};

///
/// A "role" view of \ref resource.
///
/// If `role` is not present, this is the root resource.
///
class role_resource_view final {
    const resource& _resource;

public:
    ///
    /// \throws \ref resource_kind_mismatch if the argument is not a "role" resource.
    ///
    explicit role_resource_view(const resource&);

    std::optional<std::string_view> role() const;
};

///
/// A "service_level" view of \ref resource.
///
class service_level_resource_view final {
public:
    ///
    /// \throws \ref resource_kind_mismatch if the argument is not a "service_level" resource.
    ///
    explicit service_level_resource_view(const resource&);

};

///
/// A "function" view of \ref resource.
///
class functions_resource_view final {
    const resource& _resource;
public:
    ///
    /// \throws \ref resource_kind_mismatch if the argument is not a "function" resource.
    ///
    explicit functions_resource_view(const resource&);

    std::optional<std::string_view> keyspace() const;
    std::optional<std::string_view> function_signature() const;
    std::optional<std::string_view> function_name() const;
    std::optional<std::vector<std::string_view>> function_args() const;
};

///
/// Parse a resource from its name.
///
/// \throws \ref invalid_resource_name when the name is malformed.
///
resource parse_resource(std::string_view name);

const resource& root_data_resource();

inline resource make_data_resource(std::string_view keyspace) {
    return resource(data_resource_t{}, keyspace);
}
inline resource make_data_resource(std::string_view keyspace, std::string_view table) {
    return resource(data_resource_t{}, keyspace, table);
}

const resource& root_role_resource();

inline resource make_role_resource(std::string_view role) {
    return resource(role_resource_t{}, role);
}

const resource& root_service_level_resource();

inline resource make_service_level_resource() {
    return resource(service_level_resource_t{});
}

const resource& root_function_resource();

inline resource make_functions_resource() {
    return resource(functions_resource_t{});
}

inline resource make_functions_resource(std::string_view keyspace) {
    return resource(functions_resource_t{}, keyspace);
}

inline resource make_functions_resource(std::string_view keyspace, std::string_view function_signature) {
    return resource(functions_resource_t{}, keyspace, function_signature);
}

inline resource make_functions_resource(std::string_view keyspace, std::string_view function_name, std::vector<::shared_ptr<cql3::cql3_type::raw>> function_signature) {
    return resource(functions_resource_t{}, keyspace, function_name, function_signature);
}

resource make_functions_resource(const cql3::functions::function& f);

sstring encode_signature(std::string_view name, std::vector<data_type> args);

std::pair<sstring, std::vector<data_type>> decode_signature(std::string_view encoded_signature);

}

namespace std {

template <>
struct hash<auth::resource> {
    static size_t hash_data(const auth::data_resource_view& dv) {
        return utils::tuple_hash()(std::make_tuple(auth::resource_kind::data, dv.keyspace(), dv.table()));
    }

    static size_t hash_role(const auth::role_resource_view& rv) {
        return utils::tuple_hash()(std::make_tuple(auth::resource_kind::role, rv.role()));
    }

    static size_t hash_service_level(const auth::service_level_resource_view& rv) {
            return utils::tuple_hash()(std::make_tuple(auth::resource_kind::service_level));
    }

    static size_t hash_function(const auth::functions_resource_view& fv) {
        return utils::tuple_hash()(std::make_tuple(auth::resource_kind::functions, fv.keyspace(), fv.function_signature()));
    }

    size_t operator()(const auth::resource& r) const {
        std::size_t value;

        switch (r._kind) {
        case auth::resource_kind::data: value = hash_data(auth::data_resource_view(r)); break;
        case auth::resource_kind::role: value = hash_role(auth::role_resource_view(r)); break;
        case auth::resource_kind::service_level: value = hash_service_level(auth::service_level_resource_view(r)); break;
        case auth::resource_kind::functions: value = hash_function(auth::functions_resource_view(r)); break;
        }

        return value;
    }
};

}

namespace auth {

using resource_set = std::unordered_set<resource>;

//
// A resource and all of its parents.
//
resource_set expand_resource_family(const resource&);

}

template <> struct fmt::formatter<auth::data_resource_view> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const auth::data_resource_view&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<auth::resource> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const auth::resource&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
