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

#include <vector>
#include <tuple>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "auth/permission.hh"
#include "auth/resource.hh"
#include "seastarx.hh"

namespace auth {

class service;

class authenticated_user;

struct permission_details {
    sstring user;
    ::auth::resource resource;
    permission_set permissions;

    bool operator<(const permission_details& v) const {
        return std::tie(user, resource, permissions) < std::tie(v.user, v.resource, v.permissions);
    }
};

using std::experimental::optional;

///
/// Abstract interface for authorizing users to access resources.
///
class authorizer {
public:
    virtual ~authorizer() {
    }

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    ///
    /// A fully-qualified (class with package) Java-like name for this implementation.
    ///
    virtual const sstring& qualified_java_name() const = 0;

    ///
    /// Query for the permissions granted to a role for a particular \ref resource.
    ///
    /// The resulting permission set includes permissions obtained transitively through roles granted to this role.
    ///
    virtual future<permission_set> authorize(service&, sstring role_name, resource) const = 0;

    ///
    /// Grant a set of permissions to a user for a particular \ref resource.
    ///
    virtual future<> grant(const authenticated_user& performer, permission_set, resource, sstring to) = 0;

    ///
    /// Revoke a set of permissions from a user for a particular \ref resource.
    ///
    virtual future<> revoke(const authenticated_user& performer, permission_set, resource, sstring from) = 0;

    ///
    /// Query for granted permissions.
    ///
    /// Only information for permissions in `matching` is included.
    ///
    /// If `resource` is empty, then query for permissions on all resources.
    ///
    /// If `user` is empty, query for permissions of all users. Otherwise, query for permissions specific to that user.
    ///
    /// \returns an exceptional future with \ref exceptions::unauthorized_exception if the performer does not have
    /// permission to view the user's permissions.
    ///
    virtual future<std::vector<permission_details>>
    list(
            service&,
            const authenticated_user& performer,
            permission_set matching,
            optional<resource> resource,
            optional<sstring> user) const = 0;

    ///
    /// Revoke all permissions granted to a particular user.
    ///
    virtual future<> revoke_all(sstring dropped_user) = 0;

    ///
    /// Revoke all permissions granted to any user for a particular resource.
    ///
    virtual future<> revoke_all(resource) = 0;

    ///
    /// System resources used internally as part of the implementation. These are made inaccessible to users.
    ///
    virtual const resource_set& protected_resources() = 0;

    ///
    /// Validate configuration, if applicable.
    ///
    virtual future<> validate_configuration() const = 0;
};

}
