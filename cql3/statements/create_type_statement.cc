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
 * Copyright 2016-present ScyllaDB
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

#include "cql3/statements/create_type_statement.hh"
#include "prepared_statement.hh"
#include "database.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "user_types_metadata.hh"
#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {

create_type_statement::create_type_statement(const ut_name& name, bool if_not_exists)
    : _name{name}
    , _if_not_exists{if_not_exists}
{
}

void create_type_statement::prepare_keyspace(const service::client_state& state)
{
    if (!_name.has_keyspace()) {
        _name.set_keyspace(state.get_keyspace());
    }
}

void create_type_statement::add_definition(::shared_ptr<column_identifier> name, ::shared_ptr<cql3_type::raw> type)
{
    _column_names.emplace_back(name);
    _column_types.emplace_back(type);
}

future<> create_type_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    return state.has_keyspace_access(keyspace(), auth::permission::CREATE);
}

inline bool create_type_statement::type_exists_in(::keyspace& ks) const
{
    auto&& keyspace_types = ks.metadata()->user_types().get_all_types();
    return keyspace_types.contains(_name.get_user_type_name());
}

void create_type_statement::validate(service::storage_proxy& proxy, const service::client_state& state) const
{
    try {
        auto&& ks = proxy.get_db().local().find_keyspace(keyspace());
        if (type_exists_in(ks) && !_if_not_exists) {
            throw exceptions::invalid_request_exception(format("A user type of name {} already exists", _name.to_string()));
        }
    } catch (no_such_keyspace& e) {
        throw exceptions::invalid_request_exception(format("Cannot add type in unknown keyspace {}", keyspace()));
    }

    if (_column_types.size() > max_udt_fields) {
        throw exceptions::invalid_request_exception(format("A user type cannot have more than {} fields", max_udt_fields));
    }

    for (auto&& type : _column_types) {
        if (type->is_counter()) {
            throw exceptions::invalid_request_exception("A user type cannot contain counters");
        }
        if (type->is_user_type() && !type->is_frozen()) {
            throw exceptions::invalid_request_exception("A user type cannot contain non-frozen user type fields");
        }
    }
}

void create_type_statement::check_for_duplicate_names(user_type type)
{
    auto names = type->field_names();
    for (auto i = names.cbegin(); i < names.cend() - 1; ++i) {
        for (auto j = i +  1; j < names.cend(); ++j) {
            if (*i == *j) {
                throw exceptions::invalid_request_exception(
                        format("Duplicate field name {} in type {}", to_hex(*i), type->get_name_as_string()));
            }
        }
    }
}

const sstring& create_type_statement::keyspace() const
{
    return _name.get_keyspace();
}

inline user_type create_type_statement::create_type(database& db) const
{
    std::vector<bytes> field_names;
    std::vector<data_type> field_types;

    for (auto&& column_name : _column_names) {
        field_names.push_back(column_name->name());
    }

    for (auto&& column_type : _column_types) {
        field_types.push_back(column_type->prepare(db, keyspace()).get_type());
    }

    // When a table is created with a UDT column, the column will be non-frozen (multi cell) by default.
    return user_type_impl::get_instance(keyspace(), _name.get_user_type_name(),
        std::move(field_names), std::move(field_types), true /* multi cell */);
}

future<shared_ptr<cql_transport::event::schema_change>> create_type_statement::announce_migration(query_processor& qp) const
{
    database& db = qp.db();

    // Keyspace exists or we wouldn't have validated otherwise
    auto&& ks = db.find_keyspace(keyspace());

    // Can happen with if_not_exists
    if (type_exists_in(ks)) {
        return make_ready_future<::shared_ptr<cql_transport::event::schema_change>>();
    }

    auto type = create_type(db);
    check_for_duplicate_names(type);
    return qp.get_migration_manager().announce_new_type(type).then([this] {
        using namespace cql_transport;

        return ::make_shared<event::schema_change>(
                event::schema_change::change_type::CREATED,
                event::schema_change::target_type::TYPE,
                keyspace(),
                _name.get_string_type_name());
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
create_type_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_type_statement>(*this));
}

}

}
