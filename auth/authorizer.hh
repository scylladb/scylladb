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
#include <functional>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "auth/permission.hh"
#include "auth/resource.hh"
#include "seastarx.hh"
#include "stdx.hh"

namespace auth {

class role_or_anonymous;

struct permission_details {
    sstring role_name;
    ::auth::resource resource;
    permission_set permissions;
};

inline bool operator==(const permission_details& pd1, const permission_details& pd2) {
    return std::forward_as_tuple(pd1.role_name, pd1.resource, pd1.permissions.mask())
            == std::forward_as_tuple(pd2.role_name, pd2.resource, pd2.permissions.mask());
}

inline bool operator!=(const permission_details& pd1, const permission_details& pd2) {
    return !(pd1 == pd2);
}

inline bool operator<(const permission_details& pd1, const permission_details& pd2) {
    return std::forward_as_tuple(pd1.role_name, pd1.resource, pd1.permissions)
            < std::forward_as_tuple(pd2.role_name, pd2.resource, pd2.permissions);
}

class unsupported_authorization_operation : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// Abstract client for authorizing roles to access resources.
///
/// All state necessary to authorize a role is stored externally to the client instance.
///
class authorizer {
public:
    virtual ~authorizer() = default;

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    ///
    /// A fully-qualified (class with package) Java-like name for this implementation.
    ///
    virtual const sstring& qualified_java_name() const = 0;

    ///
    /// Query for the permissions granted directly to a role for a particular \ref resource (and not any of its
    /// parents).
    ///
    /// The optional role name is empty when an anonymous user is authorized. Some implementations may still wish to
    /// grant default permissions in this case.
    ///
    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const = 0;

    ///
    /// Grant a set of permissions to a role for a particular \ref resource.
    ///
    /// \throws \ref unsupported_authorization_operation if granting permissions is not supported.
    ///
    virtual future<> grant(stdx::string_view role_name, permission_set, const resource&) const = 0;

    ///
    /// Revoke a set of permissions from a role for a particular \ref resource.
    ///
    /// \throws \ref unsupported_authorization_operation if revoking permissions is not supported.
    ///
    virtual future<> revoke(stdx::string_view role_name, permission_set, const resource&) const = 0;

    ///
    /// Query for all directly granted permissions.
    ///
    /// \throws \ref unsupported_authorization_operation if listing permissions is not supported.
    ///
    virtual future<std::vector<permission_details>> list_all() const = 0;

    ///
    /// Revoke all permissions granted directly to a particular role.
    ///
    /// \throws \ref unsupported_authorization_operation if revoking permissions is not supported.
    ///
    virtual future<> revoke_all(stdx::string_view role_name) const = 0;

    ///
    /// Revoke all permissions granted to any role for a particular resource.
    ///
    /// \throws \ref unsupported_authorization_operation if revoking permissions is not supported.
    ///
    virtual future<> revoke_all(const resource&) const = 0;

    ///
    /// System resources used internally as part of the implementation. These are made inaccessible to users.
    ///
    virtual const resource_set& protected_resources() const = 0;
};

}
