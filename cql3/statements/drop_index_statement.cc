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
 * Copyright (C) 2017 ScyllaDB
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

#include "cql3/statements/drop_index_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "schema_builder.hh"

namespace cql3 {

namespace statements {

drop_index_statement::drop_index_statement(::shared_ptr<index_name> index_name, bool if_exists)
    : schema_altering_statement{index_name->get_cf_name(), &timeout_config::truncate_timeout}
    , _index_name{index_name->get_idx()}
    , _if_exists{if_exists}
{
}

const sstring& drop_index_statement::column_family() const
{
    auto cfm = lookup_indexed_table();
    assert(cfm);
    return cfm->cf_name();
}

future<> drop_index_statement::check_access(const service::client_state& state)
{
    auto cfm = lookup_indexed_table();
    if (!cfm) {
        return make_ready_future<>();
    }
    return state.has_column_family_access(cfm->ks_name(), cfm->cf_name(), auth::permission::ALTER);
}

void drop_index_statement::validate(service::storage_proxy&, const service::client_state& state)
{
    // validated in lookup_indexed_table()
}

future<shared_ptr<cql_transport::event::schema_change>> drop_index_statement::announce_migration(service::storage_proxy& proxy, bool is_local_only)
{
    if (!service::get_local_storage_service().cluster_supports_indexes()) {
        throw exceptions::invalid_request_exception("Index support is not enabled");
    }
    auto cfm = lookup_indexed_table();
    if (!cfm) {
        return make_ready_future<::shared_ptr<cql_transport::event::schema_change>>(nullptr);
    }
    ++_cql_stats->secondary_index_drops;
    auto builder = schema_builder(cfm);
    builder.without_index(_index_name);
    return service::get_local_migration_manager().announce_column_family_update(builder.build(), false, {}, is_local_only).then([cfm] {
        // Dropping an index is akin to updating the CF
        // Note that we shouldn't call columnFamily() at this point because the index has been dropped and the call to lookupIndexedTable()
        // in that method would now throw.
        using namespace cql_transport;
        return make_shared<event::schema_change>(event::schema_change::change_type::UPDATED,
                                                 event::schema_change::target_type::TABLE,
                                                 cfm->ks_name(),
                                                 cfm->cf_name());
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_index_statement::prepare(database& db, cql_stats& stats) {
    _cql_stats = &stats;
    return std::make_unique<prepared_statement>(make_shared<drop_index_statement>(*this));
}

schema_ptr drop_index_statement::lookup_indexed_table() const
{
    auto& db = service::get_local_storage_proxy().get_db().local();
    if (!db.has_keyspace(keyspace())) {
        throw exceptions::keyspace_not_defined_exception(sprint("Keyspace %s does not exist", keyspace()));
    }
    auto cfm = db.find_indexed_table(keyspace(), _index_name);
    if (cfm) {
        return cfm;
    }
    if (_if_exists) {
        return nullptr;
    }
    throw exceptions::invalid_request_exception(
            sprint("Index '%s' could not be found in any of the tables of keyspace '%s'", _index_name, keyspace()));
}

}

}
