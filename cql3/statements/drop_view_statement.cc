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

#include "cql3/statements/drop_view_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/migration_manager.hh"
#include "view_info.hh"

namespace cql3 {

namespace statements {

drop_view_statement::drop_view_statement(::shared_ptr<cf_name> view_name, bool if_exists)
    : schema_altering_statement{std::move(view_name), &timeout_config::truncate_timeout}
    , _if_exists{if_exists}
{
}

future<> drop_view_statement::check_access(const service::client_state& state)
{
    try {
        auto&& s = service::get_local_storage_proxy().get_db().local().find_schema(keyspace(), column_family());
        if (s->is_view()) {
            return state.has_column_family_access(keyspace(), s->view_info()->base_name(), auth::permission::ALTER);
        }
    } catch (const no_such_column_family& e) {
        // Will be validated afterwards.
    }
    return make_ready_future<>();
}

void drop_view_statement::validate(service::storage_proxy&, const service::client_state& state)
{
    // validated in migration_manager::announce_view_drop()
}

future<shared_ptr<cql_transport::event::schema_change>> drop_view_statement::announce_migration(service::storage_proxy& proxy, bool is_local_only)
{
    return make_ready_future<>().then([this, is_local_only] {
        return service::get_local_migration_manager().announce_view_drop(keyspace(), column_family(), is_local_only);
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            using namespace cql_transport;

            return make_shared<event::schema_change>(event::schema_change::change_type::DROPPED,
                                                     event::schema_change::target_type::TABLE,
                                                     this->keyspace(),
                                                     this->column_family());
        } catch (const exceptions::configuration_exception& e) {
            if (_if_exists) {
                return ::shared_ptr<cql_transport::event::schema_change>();
            }
            throw e;
        }
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_view_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_view_statement>(*this));
}

}

}
