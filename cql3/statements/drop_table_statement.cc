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
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_table_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "mutation.hh"

namespace cql3 {

namespace statements {

drop_table_statement::drop_table_statement(cf_name cf_name, bool if_exists)
    : schema_altering_statement{std::move(cf_name), &timeout_config::truncate_timeout}
    , _if_exists{if_exists}
{
}

future<> drop_table_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    // invalid_request_exception is only thrown synchronously.
    try {
        return state.has_column_family_access(proxy.local_db(), keyspace(), column_family(), auth::permission::DROP);
    } catch (exceptions::invalid_request_exception&) {
        if (!_if_exists) {
            throw;
        }
        return make_ready_future();
    }
}

void drop_table_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    // validated in prepare_schema_mutations()
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_table_statement::prepare_schema_mutations(query_processor& qp) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    try {
        m = co_await qp.get_migration_manager().prepare_column_family_drop_announcement(keyspace(), column_family());

        using namespace cql_transport;
        ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::DROPPED,
                event::schema_change::target_type::TABLE,
                this->keyspace(),
                this->column_family());
    } catch (const exceptions::configuration_exception& e) {
        if (!_if_exists) {
            co_return coroutine::exception(std::current_exception());
        }
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_table_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_table_statement>(*this));
}

}

}
