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

#include <experimental/optional>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "permission.hh"
#include "data_resource.hh"

#include "seastarx.hh"

namespace auth {

class service;

class authenticated_user;

struct permission_details {
    sstring user;
    data_resource resource;
    permission_set permissions;

    bool operator<(const permission_details& v) const {
        return std::tie(user, resource, permissions) < std::tie(v.user, v.resource, v.permissions);
    }
};

using std::experimental::optional;

class authorizer {
public:
    virtual ~authorizer() {}

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    virtual const sstring& qualified_java_name() const = 0;

    /**
     * The primary Authorizer method. Returns a set of permissions of a user on a resource.
     *
     * @param user Authenticated user requesting authorization.
     * @param resource Resource for which the authorization is being requested. @see DataResource.
     * @return Set of permissions of the user on the resource. Should never return empty. Use permission.NONE instead.
     */
    virtual future<permission_set> authorize(service&, ::shared_ptr<authenticated_user>, data_resource) const = 0;

    /**
     * Grants a set of permissions on a resource to a user.
     * The opposite of revoke().
     *
     * @param performer User who grants the permissions.
     * @param permissions Set of permissions to grant.
     * @param to Grantee of the permissions.
     * @param resource Resource on which to grant the permissions.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    virtual future<> grant(::shared_ptr<authenticated_user> performer, permission_set, data_resource, sstring to) = 0;

    /**
     * Revokes a set of permissions on a resource from a user.
     * The opposite of grant().
     *
     * @param performer User who revokes the permissions.
     * @param permissions Set of permissions to revoke.
     * @param from Revokee of the permissions.
     * @param resource Resource on which to revoke the permissions.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    virtual future<> revoke(::shared_ptr<authenticated_user> performer, permission_set, data_resource, sstring from) = 0;

    /**
     * Returns a list of permissions on a resource of a user.
     *
     * @param performer User who wants to see the permissions.
     * @param permissions Set of Permission values the user is interested in. The result should only include the matching ones.
     * @param resource The resource on which permissions are requested. Can be null, in which case permissions on all resources
     *                 should be returned.
     * @param of The user whose permissions are requested. Can be null, in which case permissions of every user should be returned.
     *
     * @return All of the matching permission that the requesting user is authorized to know about.
     *
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    virtual future<std::vector<permission_details>> list(service&, ::shared_ptr<authenticated_user> performer, permission_set, optional<data_resource>, optional<sstring>) const = 0;

    /**
     * This method is called before deleting a user with DROP USER query so that a new user with the same
     * name wouldn't inherit permissions of the deleted user in the future.
     *
     * @param droppedUser The user to revoke all permissions from.
     */
    virtual future<> revoke_all(sstring dropped_user) = 0;

    /**
     * This method is called after a resource is removed (i.e. keyspace or a table is dropped).
     *
     * @param droppedResource The resource to revoke all permissions on.
     */
    virtual future<> revoke_all(data_resource) = 0;

    /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces, column families that will be unmodifiable by users; other resources.
     */
    virtual const resource_ids& protected_resources() = 0;

    /**
     * Validates configuration of IAuthorizer implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    virtual future<> validate_configuration() const = 0;
};

}
