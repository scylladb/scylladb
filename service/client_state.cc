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
#include "auth/authorizer.hh"
#include "auth/authenticator.hh"
#include "exceptions/exceptions.hh"
#include "validation.hh"
#include "db/system_keyspace.hh"
#include "db/schema_tables.hh"

void service::client_state::set_login(::shared_ptr<auth::authenticated_user> user) {
    if (user == nullptr) {
        throw std::invalid_argument("Must provide user");
    }
    _user = std::move(user);
}

future<> service::client_state::check_user_exists() {
    if (_user->is_anonymous()) {
        return make_ready_future();
    }

    return auth::auth::is_existing_user(_user->name()).then([user = _user](bool exists) mutable {
        if (!exists) {
            throw exceptions::authentication_exception(
                            sprint("User %s doesn't exist - create it with CREATE USER query first",
                                            user->name()));
        }
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

future<> service::client_state::has_all_keyspaces_access(
                auth::permission p) const {
    if (_is_internal) {
        return make_ready_future();
    }
    validate_login();
    return ensure_has_permission(p, auth::data_resource());
}

future<> service::client_state::has_keyspace_access(const sstring& ks,
                auth::permission p) const {
    validation::validate_keyspace(ks);
    return has_access(ks, p, auth::data_resource(ks));
}

future<> service::client_state::has_column_family_access(const sstring& ks,
                const sstring& cf, auth::permission p) const {
    validation::validate_column_family(ks, cf);
    return has_access(ks, p, auth::data_resource(ks, cf));
}

future<> service::client_state::has_access(const sstring& ks, auth::permission p, auth::data_resource resource) const {
    if (_is_internal) {
        return make_ready_future();
    }

    validate_login();

    // we only care about schema modification.
    if (auth::permissions::ALTERATIONS.contains(p)) {
        // prevent system keyspace modification
        auto name = ks;
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);
        if (name == db::system_keyspace::NAME) {
            throw exceptions::unauthorized_exception(ks + " keyspace is not user-modifiable.");
        }

        // we want to allow altering AUTH_KS and TRACING_KS.
        for (auto& n : { auth::auth::AUTH_KS /* TODO: tracing */ }) {
            if (name == n && !resource.is_keyspace_level() && p != auth::permission::ALTER) {
                throw exceptions::unauthorized_exception(sprint("Cannot %s %s", auth::permissions::to_string(p), resource));
            }
        }
    }

    if (p == auth::permission::SELECT && resource.is_column_family_level() && resource.keyspace() == db::system_keyspace::NAME) {
        for (auto& n : { db::system_keyspace::LOCAL, db::system_keyspace::PEERS }) {
            if (resource.column_family() == n) {
                return make_ready_future();
            }
        }
        for (auto& n : db::schema_tables::ALL) {
            if (resource.column_family() == n) {
                return make_ready_future();
            }
        }
    }
    if (auth::permissions::ALTERATIONS.contains(p)) {
        for (auto& s : { auth::authorizer::get().protected_resources(),
                        auth::authenticator::get().protected_resources() }) {
            if (s.count(resource)) {
                throw exceptions::unauthorized_exception(
                                sprint("%s schema is protected",
                                                resource));
            }
        }
    }

    return ensure_has_permission(p, std::move(resource));
}

future<bool> service::client_state::check_has_permission(auth::permission p, auth::data_resource resource) const {
    std::experimental::optional<auth::data_resource> parent;
    if (resource.has_parent()) {
        parent = resource.get_parent();
    }

    return auth::auth::get_permissions(_user, resource).then([this, p, parent = std::move(parent)](auth::permission_set set) {
        if (set.contains(p)) {
            return make_ready_future<bool>(true);
        }
        if (parent) {
            return check_has_permission(p, std::move(*parent));
        }
        return make_ready_future<bool>(false);
    });
}

future<> service::client_state::ensure_has_permission(auth::permission p, auth::data_resource resource) const {
    return check_has_permission(p, resource).then([this, p, resource](bool ok) {
        if (!ok) {
            throw exceptions::unauthorized_exception(sprint("User %s has no %s permission on %s or any of its parents",
                                            _user->name(),
                                            auth::permissions::to_string(p),
                                            resource));
        }
    });
}

