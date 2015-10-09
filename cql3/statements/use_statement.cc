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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

bool use_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool use_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

future<::shared_ptr<transport::messages::result_message>>
use_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    state.get_client_state().set_keyspace(proxy.local().get_db(), _keyspace);
    auto result =::make_shared<transport::messages::result_message::set_keyspace>(_keyspace);
    return make_ready_future<::shared_ptr<transport::messages::result_message>>(result);
}

future<::shared_ptr<transport::messages::result_message>>
use_statement::execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    // Internal queries are exclusively on the system keyspace and 'use' is thus useless
    throw std::runtime_error("unsupported operation");
}

}

}
