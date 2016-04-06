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

#include "client_state.hh"
#include "auth/auth.hh"
#include "exceptions/exceptions.hh"

future<> service::client_state::login(::shared_ptr<auth::authenticated_user> user) {
    if (user == nullptr) {
        throw std::invalid_argument("Must provide user");
    }
    if (user->is_anonymous()) {
        _user = std::move(user);
        return make_ready_future();
    }
    auto f = auth::auth::is_existing_user(user->name());
    return f.then([this, user = std::move(user)](bool exists) mutable {
        if (!exists) {
            throw exceptions::authentication_exception(
                            sprint("User %s doesn't exist - create it with CREATE USER query first",
                                            user->name()));
        }
        _user = std::move(user);
        return make_ready_future();
    });
}

void service::client_state::validate_login() const {
    if (!_user) {
        throw exceptions::unauthorized_exception("You have not logged in");
    }
}

void service::client_state::ensure_not_anonymous() const throw(exceptions::unauthorized_exception) {
    validate_login();
    if (_user->is_anonymous()) {
        throw exceptions::unauthorized_exception("You have to be logged in and not anonymous to perform this request");
    }
}

void service::client_state::merge(const client_state& other) {
    if (other._dirty) {
        _keyspace = other._keyspace;
    }
    if (_user == nullptr) {
        _user = other._user;
    }
    _last_timestamp_micros = std::max(_last_timestamp_micros, other._last_timestamp_micros);
}
