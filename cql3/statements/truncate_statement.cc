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
 * Copyright (C) 2014-present ScyllaDB
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

#include "cql3/statements/truncate_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include <optional>

namespace cql3 {

namespace statements {

truncate_statement::truncate_statement(cf_name name)
    : cf_statement{std::move(name)}
    , cql_statement_no_metadata(&timeout_config::truncate_timeout)
{
}

uint32_t truncate_statement::get_bound_terms() const
{
    return 0;
}

std::unique_ptr<prepared_statement> truncate_statement::prepare(database& db,cql_stats& stats)
{
    return std::make_unique<prepared_statement>(::make_shared<truncate_statement>(*this));
}

bool truncate_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool truncate_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

future<> truncate_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    return state.has_column_family_access(proxy.local_db(), keyspace(), column_family(), auth::permission::MODIFY);
}

void truncate_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    warn(unimplemented::cause::VALIDATION);
#if 0
    ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
#endif
}

future<::shared_ptr<cql_transport::messages::result_message>>
truncate_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const
{
    if (qp.db().find_schema(keyspace(), column_family())->is_view()) {
        throw exceptions::invalid_request_exception("Cannot TRUNCATE materialized view directly; must truncate base table instead");
    }
    return qp.proxy().truncate_blocking(keyspace(), column_family()).handle_exception([](auto ep) {
        throw exceptions::truncate_exception(ep);
    }).then([] {
        return ::shared_ptr<cql_transport::messages::result_message>{};
    });
}

}

}
