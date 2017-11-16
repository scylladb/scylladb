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

#include "create_user_statement.hh"
#include "auth/authenticator.hh"
#include "auth/service.hh"

cql3::statements::create_user_statement::create_user_statement(sstring username, ::shared_ptr<user_options> opts, bool superuser, bool if_not_exists)
    : _username(std::move(username))
    , _opts(std::move(opts))
    , _superuser(superuser)
    , _if_not_exists(if_not_exists)
{}

void cql3::statements::create_user_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) {
    if (_username.empty()) {
        throw exceptions::invalid_request_exception("Username can't be an empty string");
    }

    _opts->validate(state.get_auth_service()->underlying_authenticator());

    // validate login here before checkAccess to avoid leaking user existence to anonymous users.
    state.ensure_not_anonymous();

    // cannot validate user existence compliant with _if_not_exists here, because
    // we need to query -> continuation, and this is not a continuation method
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::create_user_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    auto& client_state = state.get_client_state();
    auto& auth_service = *client_state.get_auth_service();

    return auth::is_super_user(auth_service, *client_state.user()).then([this, &auth_service](bool is_super) {
        if (!is_super) {
            throw exceptions::unauthorized_exception("Only superusers are allowed to perform CREATE USER queries");
        }
        return auth_service.is_existing_user(_username).then([this, &auth_service](bool exists) {
            if (exists && !_if_not_exists) {
                throw exceptions::invalid_request_exception(sprint("User %s already exists", _username));
            }
            if (exists && _if_not_exists) {
                make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
            }
            return auth_service.underlying_authenticator().create(_username, _opts->options()).then([this, &auth_service] {
                return auth_service.insert_user(_username, _superuser).then([] {
                    return  make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
                });
            });
        });
    });
}
