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

#include <boost/range/adaptor/map.hpp>

#include "drop_user_statement.hh"
#include "auth/authenticator.hh"
#include "auth/authorizer.hh"
#include "auth/service.hh"

cql3::statements::drop_user_statement::drop_user_statement(sstring username, bool if_exists)
    : _username(std::move(username))
    , _if_exists(if_exists)
{}

void cql3::statements::drop_user_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) {
    // validate login here before checkAccess to avoid leaking user existence to anonymous users.
    state.ensure_not_anonymous();

    // cannot validate user existence here, because
    // we need to query -> continuation, and this is not a continuation method

    if (state.user()->name() == _username) {
        throw exceptions::invalid_request_exception("Users aren't allowed to DROP themselves");
    }
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::drop_user_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    auto& client_state = state.get_client_state();
    auto& auth_service = *client_state.get_auth_service();

    return auth::is_super_user(auth_service, *client_state.user()).then([this, &auth_service](bool is_super) {
        if (!is_super) {
            throw exceptions::unauthorized_exception("Only superusers are allowed to perform DROP USER queries");
        }

        return auth_service.is_existing_user(_username).then([this, &auth_service](bool exists) {
            if (!_if_exists && !exists) {
                throw exceptions::invalid_request_exception(sprint("User %s doesn't exist", _username));
            }
            if (_if_exists && !exists) {
                return  make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
            }

            // clean up permissions after the dropped user.
            return auth_service.underlying_authorizer().revoke_all(_username).then([this, &auth_service] {
                return auth_service.delete_user(_username).then([this, &auth_service] {
                    return auth_service.underlying_authenticator().drop(_username);
                });
            }).then([] {
                return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
            });
        });
    });
}

