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

#include "cql3/statements/create_type_statement.hh"

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

future<> create_type_statement::check_access(const service::client_state& state)
{
    return state.has_keyspace_access(keyspace(), auth::permission::CREATE);
}

void create_type_statement::validate(distributed<service::storage_proxy>&, const service::client_state& state)
{
#if 0
    KSMetaData ksm = Schema.instance.getKSMetaData(name.getKeyspace());
    if (ksm == null)
        throw new InvalidRequestException(String.format("Cannot add type in unknown keyspace %s", name.getKeyspace()));

    if (ksm.userTypes.getType(name.getUserTypeName()) != null && !ifNotExists)
        throw new InvalidRequestException(String.format("A user type of name %s already exists", name));

    for (CQL3Type.Raw type : columnTypes)
        if (type.isCounter())
            throw new InvalidRequestException("A user type cannot contain counters");
#endif
}

#if 0
public static void checkForDuplicateNames(UserType type) throws InvalidRequestException
{
    for (int i = 0; i < type.size() - 1; i++)
    {
        ByteBuffer fieldName = type.fieldName(i);
        for (int j = i+1; j < type.size(); j++)
        {
            if (fieldName.equals(type.fieldName(j)))
                throw new InvalidRequestException(String.format("Duplicate field name %s in type %s",
                                                                UTF8Type.instance.getString(fieldName),
                                                                UTF8Type.instance.getString(type.name)));
        }
    }
}
#endif

shared_ptr<transport::event::schema_change> create_type_statement::change_event()
{
    using namespace transport;

    return make_shared<transport::event::schema_change>(event::schema_change::change_type::CREATED,
                                                        event::schema_change::target_type::TYPE,
                                                        keyspace(),
                                                        _name.get_string_type_name());
}

const sstring& create_type_statement::keyspace() const
{
    return _name.get_keyspace();
}

#if 0
private UserType createType() throws InvalidRequestException
{
    List<ByteBuffer> names = new ArrayList<>(columnNames.size());
    for (ColumnIdentifier name : columnNames)
        names.add(name.bytes);

    List<AbstractType<?>> types = new ArrayList<>(columnTypes.size());
    for (CQL3Type.Raw type : columnTypes)
        types.add(type.prepare(keyspace()).getType());

    return new UserType(name.getKeyspace(), name.getUserTypeName(), names, types);
}
#endif

future<bool> create_type_statement::announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only)
{
    throw std::runtime_error("User-defined types are not supported yet");
#if 0
   KSMetaData ksm = Schema.instance.getKSMetaData(name.getKeyspace());
   assert ksm != null; // should haven't validate otherwise

   // Can happen with ifNotExists
   if (ksm.userTypes.getType(name.getUserTypeName()) != null)
       return false;

   UserType type = createType();
   checkForDuplicateNames(type);
   MigrationManager.announceNewType(type, isLocalOnly);
   return true;
#endif
}

}

}
