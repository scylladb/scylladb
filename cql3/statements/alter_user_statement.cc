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

#include "alter_user_statement.hh"
#include "auth/authenticator.hh"
#include "auth/service.hh"

cql3::statements::alter_user_statement::alter_user_statement(sstring username, ::shared_ptr<user_options> opts, std::experimental::optional<bool> superuser)
    : _username(std::move(username))
    , _opts(std::move(opts))
    , _superuser(std::move(superuser))
{}

void cql3::statements::alter_user_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) {
    _opts->validate(state.get_auth_service()->underlying_authenticator());

    if (!_superuser && _opts->empty()) {
        throw exceptions::invalid_request_exception("ALTER USER can't be empty");
    }

    // validate login here before checkAccess to avoid leaking user existence to anonymous users.
    state.ensure_not_anonymous();

    // cannot validate user existence here, because
    // we need to query -> continuation, and this is not a continuation method
}

future<> cql3::statements::alter_user_statement::check_access(const service::client_state& state) {
    auto user = state.user();
    if (_superuser && user->name() == _username) {
        // using contractions in error messages is the ultimate sign of lowbrowness.
        // however, dtests depend on matching the exception messages. So we keep them despite
        // my disgust.
        throw exceptions::unauthorized_exception("You aren't allowed to alter your own superuser status");
    }

    const auto& auth_service = *state.get_auth_service();

    return auth::is_super_user(auth_service, *user).then([this, user, &auth_service](bool is_super) {
        if (_superuser && !is_super) {
            throw exceptions::unauthorized_exception("Only superusers are allowed to alter superuser status");
        }

        if (!is_super && user->name() != _username) {
            throw exceptions::unauthorized_exception("You aren't allowed to alter this user");
        }

        if (!is_super) {
            for (auto o : _opts->options() | boost::adaptors::map_keys) {
                if (!auth_service.underlying_authenticator().alterable_options().contains(o)) {
                    throw exceptions::unauthorized_exception(sprint("You aren't allowed to alter {} option", o));
                }
            }
        }
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::alter_user_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    auto& client_state = state.get_client_state();
    auto& auth_service = *client_state.get_auth_service();

    return auth_service.is_existing_user(_username).then([this, &auth_service](bool exists) {
        if (!exists) {
            throw exceptions::invalid_request_exception(sprint("User %s doesn't exist", _username));
        }
        auto f = _opts->options().empty() ? make_ready_future() : auth_service.underlying_authenticator().alter(_username, _opts->options());
        if (_superuser) {
            f = f.then([this, &auth_service] {
                return auth_service.insert_user(_username, *_superuser);
            });
        }
        return f.then([] { return  make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(); });
    });
}

