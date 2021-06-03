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
 * Copyright 2014-present-2015 ScyllaDB
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

#include "raw/cf_statement.hh"
#include "service/client_state.hh"

namespace cql3 {

namespace statements {

namespace raw {

cf_statement::cf_statement(std::optional<cf_name> cf_name)
    : _cf_name(std::move(cf_name))
{
}

void cf_statement::prepare_keyspace(const service::client_state& state)
{
    if (!_cf_name->has_keyspace()) {
        // XXX: We explicitely only want to call state.getKeyspace() in this case, as we don't want to throw
        // if not logged in any keyspace but a keyspace is explicitely set on the statement. So don't move
        // the call outside the 'if' or replace the method by 'prepareKeyspace(state.getKeyspace())'
        _cf_name->set_keyspace(state.get_keyspace(), true);
    }
}

void cf_statement::prepare_keyspace(std::string_view keyspace)
{
    if (!_cf_name->has_keyspace()) {
        _cf_name->set_keyspace(keyspace, true);
    }
}

const sstring& cf_statement::keyspace() const
{
    assert(_cf_name->has_keyspace()); // "The statement hasn't be prepared correctly";
    return _cf_name->get_keyspace();
}

const sstring& cf_statement::column_family() const
{
    return _cf_name->get_column_family();
}

}

}

}
