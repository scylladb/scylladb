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
 * Copyright (C) 2015 ScyllaDB
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

#include "cql3/statements/use_statement.hh"
#include "cql3/statements/raw/use_statement.hh"

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

use_statement::use_statement(sstring keyspace)
        : cql_statement_no_metadata(&timeout_config::other_timeout)
        , _keyspace(keyspace)
{
}

uint32_t use_statement::get_bound_terms()
{
    return 0;
}

namespace raw {

use_statement::use_statement(sstring keyspace)
    : _keyspace(keyspace)
{
}

std::unique_ptr<prepared_statement> use_statement::prepare(database& db, cql_stats& stats)
{
    return std::make_unique<prepared>(make_shared<cql3::statements::use_statement>(_keyspace));
}

}

bool use_statement::uses_function(const sstring& ks_name, const sstring& function_name) const
{
    return false;
}

bool use_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool use_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

future<> use_statement::check_access(const service::client_state& state)
{
    state.validate_login();
    return make_ready_future<>();
}

void use_statement::validate(service::storage_proxy&, const service::client_state& state)
{
}

future<::shared_ptr<cql_transport::messages::result_message>>
use_statement::execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) {
    state.get_client_state().set_keyspace(proxy.get_db(), _keyspace);
    auto result =::make_shared<cql_transport::messages::result_message::set_keyspace>(_keyspace);
    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(result);
}

}

}
