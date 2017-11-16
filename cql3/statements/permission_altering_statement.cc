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
 * Copyright 2016 ScyllaDB
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

#include <seastar/core/thread.hh>

#include "auth/service.hh"
#include "permission_altering_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/selection/selection.hh"

cql3::statements::permission_altering_statement::permission_altering_statement(
                auth::permission_set permissions, auth::data_resource resource,
                sstring username)
                : _permissions(permissions), _resource(std::move(resource)), _username(
                                std::move(username)) {
}

void cql3::statements::permission_altering_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) {
    // a check to ensure the existence of the user isn't being leaked by user existence check.
    state.ensure_not_anonymous();
}

future<> cql3::statements::permission_altering_statement::check_access(const service::client_state& state) {
    return state.get_auth_service()->is_existing_user(_username).then([this, &state](bool exists) {
        if (!exists) {
            throw exceptions::invalid_request_exception(sprint("User %s doesn't exist", _username));
        }
        mayme_correct_resource(_resource, state);
        if (!_resource.exists()) {
            throw exceptions::invalid_request_exception(sprint("%s doesn't exist", _resource));
        }

        // check that the user has AUTHORIZE permission on the resource or its parents, otherwise reject GRANT/REVOKE.
        return state.ensure_has_permission(auth::permission::AUTHORIZE, _resource).then([this, &state] {
            static auto perm_list = {
                auth::permission::READ,
                auth::permission::WRITE,
                auth::permission::CREATE,
                auth::permission::ALTER,
                auth::permission::DROP,
                auth::permission::SELECT,
                auth::permission::MODIFY,
            };
            return do_for_each(perm_list, [this, &state](auth::permission p) {
                // TODO: how about we re-write the access check to check a set
                // right away. Might need some tweaking of enum_set to make it
                // neat/transparent, but still...
                // This is not critical code however.
                if (_permissions.contains(p)) {
                    return state.ensure_has_permission(p, _resource);
                }
                return make_ready_future();
            });
        });
    });
}

