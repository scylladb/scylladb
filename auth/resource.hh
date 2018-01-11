/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <experimental/string_view>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <vector>
#include <unordered_set>

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include "auth/permission.hh"
#include "seastarx.hh"
#include "stdx.hh"
#include "utils/hash.hh"

namespace auth {

class invalid_resource_name : public std::invalid_argument {
    std::shared_ptr<sstring> _name;

public:
    explicit invalid_resource_name(stdx::string_view name)
            : std::invalid_argument(sprint("The resource name '%s' is invalid.", name))
            , _name(std::make_shared<sstring>(name)) {
    }

    stdx::string_view name() const noexcept {
        return *_name;
    }
};

enum class resource_kind {
    data, role
};

std::ostream& operator<<(std::ostream&, resource_kind);

///
/// Resources are entities that users can be granted permissions on.
///
/// There are data (keyspaces and tables) and role resources. There may be other kinds of resources in the future.
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

    std::vector<sstring> _parts;

public:
    static resource root_of(resource_kind);

    static resource data(stdx::string_view keyspace);
    static resource data(stdx::string_view keyspace, stdx::string_view table);

    static resource role(stdx::string_view role);

    ///
    /// Parse a resource name.
    ///
    /// \throws \ref invalid_resource_name when the name is malformed.
    ///
    static resource from_name(stdx::string_view);

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
    // A root resource.
    explicit resource(resource_kind kind);

    resource(resource_kind, std::vector<sstring> parts);

    friend class std::hash<resource>;
    friend class data_resource_view;
    friend class role_resource_view;

    friend bool operator<(const resource&, const resource&);
    friend bool operator==(const resource&, const resource&);
};

bool operator<(const resource&, const resource&);

inline bool operator==(const resource& r1, const resource& r2) {
    return (r1._kind == r2._kind) && (r1._parts == r2._parts);
}

inline bool operator!=(const resource& r1, const resource& r2) {
    return !(r1 == r2);
}

std::ostream& operator<<(std::ostream&, const resource&);

class resource_kind_mismatch : public std::invalid_argument {
public:
    explicit resource_kind_mismatch(resource_kind expected, resource_kind actual)
        : std::invalid_argument(
            sprint("This resource has kind '%s', but was expected to have kind '%s'.", actual, expected)) {
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

    std::optional<stdx::string_view> keyspace() const;

    std::optional<stdx::string_view> table() const;
};

std::ostream& operator<<(std::ostream&, const data_resource_view&);

bool resource_exists(const data_resource_view&);

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

    std::optional<stdx::string_view> role() const;
};

std::ostream& operator<<(std::ostream&, const role_resource_view&);

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

    size_t operator()(const auth::resource& r) const {
        std::size_t value;

        switch (r._kind) {
        case auth::resource_kind::data: value = hash_data(auth::data_resource_view(r)); break;
        case auth::resource_kind::role: value = hash_role(auth::role_resource_view(r)); break;
        }

        return value;
    }
};

}

namespace auth {

using resource_set = std::unordered_set<resource>;

}
