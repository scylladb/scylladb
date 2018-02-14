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

#include <vector>
#include <seastar/core/future-util.hh>

#include "list_permissions_statement.hh"
#include "auth/authorizer.hh"
#include "auth/common.hh"
#include "cql3/result_set.hh"
#include "transport/messages/result_message.hh"

cql3::statements::list_permissions_statement::list_permissions_statement(
                auth::permission_set permissions,
                std::experimental::optional<auth::resource> resource,
                std::experimental::optional<sstring> username, bool recursive)
                : _permissions(permissions), _resource(std::move(resource)), _username(
                                std::move(username)), _recursive(recursive) {
}

void cql3::statements::list_permissions_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) {
    // a check to ensure the existence of the user isn't being leaked by user existence check.
    state.ensure_not_anonymous();
}

future<> cql3::statements::list_permissions_statement::check_access(const service::client_state& state) {
    auto f = make_ready_future();
    if (_username) {
        f = state.get_auth_service()->underlying_role_manager().exists(*_username).then([this](bool exists) {
            if (!exists) {
                throw exceptions::invalid_request_exception(sprint("User %s doesn't exist", *_username));
            }
        });
    }
    return f.then([this, &state] {
        if (_resource) {
            maybe_correct_resource(*_resource, state);

            if ((_resource->kind() == auth::resource_kind::data)
                    && !auth::resource_exists(auth::data_resource_view(*_resource))) {
                throw exceptions::invalid_request_exception(sprint("%s doesn't exist", *_resource));
            }
        }
    });
}


future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::list_permissions_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    static auto make_column = [](sstring name) {
        return ::make_shared<column_specification>(auth::meta::AUTH_KS, "permissions", ::make_shared<column_identifier>(std::move(name), true), utf8_type);
    };
    static thread_local const std::vector<::shared_ptr<column_specification>> metadata({
        make_column("role"), make_column("resource"), make_column("permission")
    });

    typedef std::experimental::optional<auth::resource> opt_resource;

    std::vector<opt_resource> resources;

    auto r = _resource;
    for (;;) {
        resources.emplace_back(r);
        if (!r || !_recursive) {
            break;
        }

        auto parent = r->parent();
        if (!parent) {
            break;
        }

        r = std::move(parent);
    }

    return map_reduce(resources, [&state, this](opt_resource r) {
        auto& auth_service = *state.get_client_state().get_auth_service();
        return auth_service.underlying_authorizer().list(auth_service, state.get_client_state().user(), _permissions, std::move(r), _username);
    }, std::vector<auth::permission_details>(), [](std::vector<auth::permission_details> details, std::vector<auth::permission_details> pd) {
        details.insert(details.end(), pd.begin(), pd.end());
        return std::move(details);
    }).then([this](std::vector<auth::permission_details> details) {
        std::sort(details.begin(), details.end());

        auto rs = std::make_unique<result_set>(metadata);

        for (auto& v : details) {
            // Make sure names are sorted.
            auto names = auth::permissions::to_strings(v.permissions);
            for (auto& p : std::set<sstring>(names.begin(), names.end())) {
                rs->add_row(
                                std::vector<bytes_opt> { utf8_type->decompose(
                                                v.user), utf8_type->decompose(
                                                sstring(sprint("%s", v.resource))),
                                                utf8_type->decompose(p), });
            }
        }

        auto rows = ::make_shared<cql_transport::messages::result_message::rows>(std::move(rs));
        return ::shared_ptr<cql_transport::messages::result_message>(std::move(rows));
    });
}
