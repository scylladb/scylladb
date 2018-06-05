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
#include "auth/authorizer.hh"
#include "auth/authenticator.hh"
#include "auth/common.hh"
#include "auth/resource.hh"
#include "exceptions/exceptions.hh"
#include "validation.hh"
#include "db/system_keyspace.hh"
#include "db/schema_tables.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "storage_service.hh"

void service::client_state::set_login(::shared_ptr<auth::authenticated_user> user) {
    if (user == nullptr) {
        throw std::invalid_argument("Must provide user");
    }
    _user = std::move(user);
    if (_is_request_copy) {
        _user_is_dirty = true;
    }
}

future<> service::client_state::check_user_exists() {
    if (auth::is_anonymous(*_user)) {
        return make_ready_future();
    }

    return _auth_service->underlying_role_manager().exists(*_user->name).then([user = _user](bool exists) mutable {
        if (!exists) {
            throw exceptions::authentication_exception(
                            sprint("User %s doesn't exist - create it with CREATE USER query first",
                                            *user->name));
        }
        return make_ready_future();
    });
}

void service::client_state::validate_login() const {
    if (!_user) {
        throw exceptions::unauthorized_exception("You have not logged in");
    }
}

void service::client_state::ensure_not_anonymous() const {
    validate_login();
    if (auth::is_anonymous(*_user)) {
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
    if (_auth_state != other._auth_state) {
        _auth_state = other._auth_state;
    }
}

future<> service::client_state::has_all_keyspaces_access(
                auth::permission p) const {
    if (_is_internal) {
        return make_ready_future();
    }
    validate_login();

    return do_with(auth::resource(auth::resource_kind::data), [this, p](const auto& r) {
        return ensure_has_permission(p, r);
    });
}

future<> service::client_state::has_keyspace_access(const sstring& ks,
                auth::permission p) const {
    return do_with(ks, auth::make_data_resource(ks), [this, p](auto const& ks, auto const& r) {
        return has_access(ks, p, r);
    });
}

future<> service::client_state::has_column_family_access(const sstring& ks,
                const sstring& cf, auth::permission p) const {
    validation::validate_column_family(ks, cf);

    return do_with(ks, auth::make_data_resource(ks, cf), [this, p](const auto& ks, const auto& r) {
        return has_access(ks, p, r);
    });
}

future<> service::client_state::has_schema_access(const schema& s, auth::permission p) const {
    return do_with(
            s.ks_name(),
            auth::make_data_resource(s.ks_name(),s.cf_name()),
            [this, p](auto const& ks, auto const& r) {
        return has_access(ks, p, r);
    });
}

future<> service::client_state::has_access(const sstring& ks, auth::permission p, const auth::resource& resource) const {
    if (ks.empty()) {
        throw exceptions::invalid_request_exception("You have not set a keyspace for this session");
    }
    if (_is_internal) {
        return make_ready_future();
    }

    validate_login();

    static const auto alteration_permissions = auth::permission_set::of<
            auth::permission::CREATE, auth::permission::ALTER, auth::permission::DROP>();

    // we only care about schema modification.
    if (alteration_permissions.contains(p)) {
        // prevent system keyspace modification
        auto name = ks;
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);
        if (is_system_keyspace(name)) {
            throw exceptions::unauthorized_exception(ks + " keyspace is not user-modifiable.");
        }

        //
        // we want to disallow dropping any contents of TRACING_KS and disallow dropping the `auth::meta::AUTH_KS`
        // keyspace.
        //

        const bool dropping_anything_in_tracing = (name == tracing::trace_keyspace_helper::KEYSPACE_NAME)
                && (p == auth::permission::DROP);

        const bool dropping_auth_keyspace = (resource == auth::make_data_resource(auth::meta::AUTH_KS))
                && (p == auth::permission::DROP);

        if (dropping_anything_in_tracing || dropping_auth_keyspace) {
            throw exceptions::unauthorized_exception(sprint("Cannot %s %s", auth::permissions::to_string(p), resource));
        }
    }

    static thread_local std::unordered_set<auth::resource> readable_system_resources = [] {
        std::unordered_set<auth::resource> tmp;
        for (auto cf : { db::system_keyspace::LOCAL, db::system_keyspace::PEERS }) {
            tmp.insert(auth::make_data_resource(db::system_keyspace::NAME, cf));
        }
        for (auto cf : db::schema_tables::ALL) {
            tmp.insert(auth::make_data_resource(db::schema_tables::NAME, cf));
        }
        return tmp;
    }();

    if (p == auth::permission::SELECT && readable_system_resources.count(resource) != 0) {
        return make_ready_future();
    }
    if (alteration_permissions.contains(p)) {
        if (auth::is_protected(*_auth_service, resource)) {
            throw exceptions::unauthorized_exception(sprint("%s is protected", resource));
        }
    }

    return ensure_has_permission(p, resource);
}

future<bool> service::client_state::check_has_permission(auth::permission p, const auth::resource& r) const {
    if (_is_internal) {
        return make_ready_future<bool>(true);
    }

    return do_with(r.parent(), [this, p, &r](const auto& parent_r) {
        return auth::get_permissions(*_auth_service, *_user, r).then([this, p, &parent_r](auth::permission_set set) {
            if (set.contains(p)) {
                return make_ready_future<bool>(true);
            }
            if (parent_r) {
                return check_has_permission(p, *parent_r);
            }
            return make_ready_future<bool>(false);
        });
    });
}

future<> service::client_state::ensure_has_permission(auth::permission p, const auth::resource& r) const {
    return check_has_permission(p, r).then([this, p, &r](bool ok) {
        if (!ok) {
            throw exceptions::unauthorized_exception(
                sprint(
                        "User %s has no %s permission on %s or any of its parents",
                        *_user,
                        auth::permissions::to_string(p),
                        r));
        }
    });
}

auth::service* service::client_state::local_auth_service_copy(const service::client_state& orig) const {
    if (orig._auth_service && _cpu_of_origin != orig._cpu_of_origin) {
        // if moved to a different shard - return a pointer to the local auth_service instance
        return &service::get_local_storage_service().get_local_auth_service();
    }
    return orig._auth_service;
}

::shared_ptr<auth::authenticated_user> service::client_state::local_user_copy(const service::client_state& orig) const {
    if (orig._user) {
        if (_cpu_of_origin != orig._cpu_of_origin) {
            // if we moved to another shard create a local copy of authenticated_user
            return ::make_shared<auth::authenticated_user>(*orig._user);
        } else {
            return orig._user;
        }
    }
    return nullptr;
}

service::client_state::client_state(service::client_state::request_copy_tag, const service::client_state& orig, api::timestamp_type ts)
        : _keyspace(orig._keyspace)
        , _cpu_of_origin(engine().cpu_id())
        , _user(local_user_copy(orig))
        , _auth_state(orig._auth_state)
        , _is_internal(orig._is_internal)
        , _is_thrift(orig._is_thrift)
        , _is_request_copy(true)
        , _remote_address(orig._remote_address)
        , _auth_service(local_auth_service_copy(orig))
        , _request_ts(ts)
{
    assert(!orig._trace_state_ptr);
}

future<> service::client_state::ensure_exists(const auth::resource& r) const {
    return _auth_service->exists(r).then([&r](bool exists) {
        if (!exists) {
            throw exceptions::invalid_request_exception(sprint("%s doesn't exist.", r));
        }

        return make_ready_future<>();
    });
}
