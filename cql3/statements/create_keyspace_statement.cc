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
 * Copyright 2015 ScyllaDB
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

#include "cql3/statements/create_keyspace_statement.hh"
#include "prepared_statement.hh"

#include "service/migration_manager.hh"

#include <regex>

bool is_system_keyspace(const sstring& keyspace);

namespace cql3 {

namespace statements {

create_keyspace_statement::create_keyspace_statement(const sstring& name, shared_ptr<ks_prop_defs> attrs, bool if_not_exists)
    : _name{name}
    , _attrs{attrs}
    , _if_not_exists{if_not_exists}
{
}

const sstring& create_keyspace_statement::keyspace() const
{
    return _name;
}

future<> create_keyspace_statement::check_access(const service::client_state& state)
{
    return state.has_all_keyspaces_access(auth::permission::CREATE);
}

void create_keyspace_statement::validate(service::storage_proxy&, const service::client_state& state)
{
    std::string name;
    name.resize(_name.length());
    std::transform(_name.begin(), _name.end(), name.begin(), ::tolower);
    if (is_system_keyspace(name)) {
        throw exceptions::invalid_request_exception("system keyspace is not user-modifiable");
    }
    // keyspace name
    std::regex name_regex("\\w+");
    if (!std::regex_match(name, name_regex)) {
        throw exceptions::invalid_request_exception(sprint("\"%s\" is not a valid keyspace name", _name.c_str()));
    }
    if (name.length() > schema::NAME_LENGTH) {
        throw exceptions::invalid_request_exception(sprint("Keyspace names shouldn't be more than %d characters long (got \"%s\")", schema::NAME_LENGTH, _name.c_str()));
    }

    _attrs->validate();

    if (!bool(_attrs->get_replication_strategy_class())) {
        throw exceptions::configuration_exception("Missing mandatory replication strategy class");
    }
#if 0
    // The strategy is validated through KSMetaData.validate() in announceNewKeyspace below.
    // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
    // so doing proper validation here.
    AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                            AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                            StorageService.instance.getTokenMetadata(),
                                                            DatabaseDescriptor.getEndpointSnitch(),
                                                            attrs.getReplicationOptions());
#endif
}

future<shared_ptr<cql_transport::event::schema_change>> create_keyspace_statement::announce_migration(service::storage_proxy& proxy, bool is_local_only)
{
    return make_ready_future<>().then([this, is_local_only] {
        return service::get_local_migration_manager().announce_new_keyspace(_attrs->as_ks_metadata(_name), is_local_only);
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            using namespace cql_transport;
            return make_shared<event::schema_change>(
                    event::schema_change::change_type::CREATED,
                    this->keyspace());
        } catch (const exceptions::already_exists_exception& e) {
            if (_if_not_exists) {
                return ::shared_ptr<cql_transport::event::schema_change>();
            }
            throw e;
        }
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::create_keyspace_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_keyspace_statement>(*this));
}

future<> cql3::statements::create_keyspace_statement::grant_permissions_to_creator(const service::client_state& cs) {
    return do_with(auth::make_data_resource(keyspace()), [&cs](const auth::resource& r) {
        return auth::grant_applicable_permissions(
                *cs.get_auth_service(),
                *cs.user(),
                r).handle_exception_type([](const auth::unsupported_authorization_operation&) {
            // Nothing.
        });
    });
}

}

}
