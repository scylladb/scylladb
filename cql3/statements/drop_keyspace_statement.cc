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

#include "cql3/statements/drop_keyspace_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"
#include "transport/event.hh"

namespace cql3 {

namespace statements {

drop_keyspace_statement::drop_keyspace_statement(const sstring& keyspace, bool if_exists)
    : schema_altering_statement(&timeout_config::truncate_timeout)
    , _keyspace{keyspace}
    , _if_exists{if_exists}
{
}

future<> drop_keyspace_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    return state.has_keyspace_access(keyspace(), auth::permission::DROP);
}

void drop_keyspace_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    warn(unimplemented::cause::VALIDATION);
#if 0
    ThriftValidation.validateKeyspaceNotSystem(keyspace);
#endif
}

const sstring& drop_keyspace_statement::keyspace() const
{
    return _keyspace;
}

future<shared_ptr<cql_transport::event::schema_change>> drop_keyspace_statement::announce_migration(query_processor& qp) const
{
    return make_ready_future<>().then([this, &mm = qp.get_migration_manager()] {
        return mm.announce_keyspace_drop(_keyspace);
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            using namespace cql_transport;
            return ::make_shared<event::schema_change>(
                    event::schema_change::change_type::DROPPED,
                    event::schema_change::target_type::KEYSPACE,
                    this->keyspace());
        } catch (const exceptions::configuration_exception& e) {
            if (_if_exists) {
                return ::shared_ptr<cql_transport::event::schema_change>();
            }
            throw e;
        }
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_keyspace_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_keyspace_statement>(*this));
}

}

}
