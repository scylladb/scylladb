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

#include "cql3/statements/drop_type_statement.hh"
#include "cql3/statements/prepared_statement.hh"

#include "boost/range/adaptor/map.hpp"

#include "service/migration_manager.hh"

namespace cql3 {

namespace statements {

drop_type_statement::drop_type_statement(const ut_name& name, bool if_exists)
    : _name{name}
    , _if_exists{if_exists}
{
}

void drop_type_statement::prepare_keyspace(const service::client_state& state)
{
    if (!_name.has_keyspace()) {
        _name.set_keyspace(state.get_keyspace());
    }
}

future<> drop_type_statement::check_access(const service::client_state& state)
{
    return state.has_keyspace_access(keyspace(), auth::permission::DROP);
}

void drop_type_statement::validate(service::storage_proxy& proxy, const service::client_state& state)
{
    try {
        auto&& ks = proxy.get_db().local().find_keyspace(keyspace());
        auto&& all_types = ks.metadata()->user_types()->get_all_types();
        auto old = all_types.find(_name.get_user_type_name());
        if (old == all_types.end()) {
            if (_if_exists) {
                return;
            } else {
                throw exceptions::invalid_request_exception(sprint("No user type named %s exists.", _name.to_string()));
            }
        }

        // We don't want to drop a type unless it's not used anymore (mainly because
        // if someone drops a type and recreates one with the same name but different
        // definition with the previous name still in use, things can get messy).
        // We have two places to check: 1) other user type that can nest the one
        // we drop and 2) existing tables referencing the type (maybe in a nested
        // way).

        // This code is moved from schema_keyspace (akin to origin) because we cannot
        // delay this check to until after we've applied the mutations. If a type or
        // table references the type we're dropping, we will a.) get exceptions parsing
        // (can be translated to invalid_request, but...) and more importantly b.)
        // we will leave those types/tables in a broken state.
        // We managed to get through this before because we neither enforced hard
        // cross reference between types when loading them, nor did we in fact
        // probably ever run the scenario of dropping a referenced type and then
        // actually using the referee.
        //
        // Now, this has a giant flaw. We are succeptible to race conditions here,
        // since we could have a drop at the same time as a create type that references
        // the dropped one, but we complete the check before the create is done,
        // yet apply the drop mutations after -> inconsistent data!
        // This problem is the same in origin, and I see no good way around it
        // as long as the atomicity of schema modifications are based on
        // actual appy of mutations, because unlike other drops, this one isn't
        // benevolent.
        // I guess this is one case where user need beware, and don't mess with types
        // concurrently!

        auto&& type = old->second;
        auto&& keyspace = type->_keyspace;
        auto&& name = type->_name;

        for (auto&& ut : all_types | boost::adaptors::map_values) {
            if (ut->_keyspace == keyspace && ut->_name == name) {
                continue;
            }

            if (ut->references_user_type(keyspace, name)) {
                throw exceptions::invalid_request_exception(sprint("Cannot drop user type %s.%s as it is still used by user type %s", keyspace, type->get_name_as_string(), ut->get_name_as_string()));
            }
        }

        for (auto&& cfm : ks.metadata()->cf_meta_data() | boost::adaptors::map_values) {
            for (auto&& col : cfm->all_columns()) {
                if (col.type->references_user_type(keyspace, name)) {
                    throw exceptions::invalid_request_exception(sprint("Cannot drop user type %s.%s as it is still used by table %s.%s", keyspace, type->get_name_as_string(), cfm->ks_name(), cfm->cf_name()));
                }
            }
        }

    } catch (no_such_keyspace& e) {
        throw exceptions::invalid_request_exception(sprint("Cannot drop type in unknown keyspace %s", keyspace()));
    }
}

const sstring& drop_type_statement::keyspace() const
{
    return _name.get_keyspace();
}

future<shared_ptr<cql_transport::event::schema_change>> drop_type_statement::announce_migration(service::storage_proxy& proxy, bool is_local_only)
{
    auto&& db = proxy.get_db().local();

    // Keyspace exists or we wouldn't have validated otherwise
    auto&& ks = db.find_keyspace(keyspace());

    auto&& all_types = ks.metadata()->user_types()->get_all_types();
    auto to_drop = all_types.find(_name.get_user_type_name());

    // Can happen with if_exists
    if (to_drop == all_types.end()) {
        return make_ready_future<::shared_ptr<cql_transport::event::schema_change>>();
    }

    return service::get_local_migration_manager().announce_type_drop(to_drop->second, is_local_only).then([this] {
        using namespace cql_transport;

        return make_shared<event::schema_change>(
                event::schema_change::change_type::DROPPED,
                event::schema_change::target_type::TYPE,
                keyspace(),
                _name.get_string_type_name());
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_type_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_type_statement>(*this));
}

}

}
