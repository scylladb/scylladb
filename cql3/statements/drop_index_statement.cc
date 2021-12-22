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
 * Copyright (C) 2017-present ScyllaDB
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
#include "cql3/statements/drop_index_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "schema_builder.hh"
#include "data_dictionary/data_dictionary.hh"
#include "mutation.hh"
#include "gms/feature_service.hh"
#include "cql3/query_processor.hh"
#include "cql3/index_name.hh"

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
    return _cf_name ? *_cf_name :
            // otherwise -- the empty name stored by the superclass
            cf_statement::column_family();
}

future<> drop_index_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    auto cfm = lookup_indexed_table(qp.proxy());
    if (!cfm) {
        return make_ready_future<>();
    }
    return state.has_column_family_access(qp.proxy().local_db(), cfm->ks_name(), cfm->cf_name(), auth::permission::ALTER);
}

void drop_index_statement::validate(query_processor& qp, const service::client_state& state) const
{
    // validated in lookup_indexed_table()

    auto db = qp.proxy().data_dictionary();
    if (db.has_keyspace(keyspace())) {
        auto schema = db.find_indexed_table(keyspace(), _index_name);
        if (schema) {
            _cf_name = schema->cf_name();
        }
    }
}

schema_ptr drop_index_statement::make_drop_idex_schema(query_processor& qp) const {
    auto cfm = lookup_indexed_table(qp.proxy());
    if (!cfm) {
        return nullptr;
    }
    ++_cql_stats->secondary_index_drops;
    auto builder = schema_builder(cfm);
    builder.without_index(_index_name);

    return builder.build();
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_index_statement::prepare_schema_mutations(query_processor& qp) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;
    auto cfm = make_drop_idex_schema(qp);

    if (cfm) {
        m = co_await qp.get_migration_manager().prepare_column_family_update_announcement(cfm, false, {}, std::nullopt);

        using namespace cql_transport;
        ret = ::make_shared<event::schema_change>(event::schema_change::change_type::UPDATED,
                                                 event::schema_change::target_type::TABLE,
                                                 cfm->ks_name(),
                                                 cfm->cf_name());
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_index_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    _cql_stats = &stats;
    return std::make_unique<prepared_statement>(make_shared<drop_index_statement>(*this));
}

schema_ptr drop_index_statement::lookup_indexed_table(service::storage_proxy& proxy) const
{
    auto& db = proxy.data_dictionary();
    if (!db.has_keyspace(keyspace())) {
        throw exceptions::keyspace_not_defined_exception(format("Keyspace {} does not exist", keyspace()));
    }
    auto cfm = db.find_indexed_table(keyspace(), _index_name);
    if (cfm) {
        return cfm;
    }
    if (_if_exists) {
        return nullptr;
    }
    throw exceptions::invalid_request_exception(
            format("Index '{}' could not be found in any of the tables of keyspace '{}'", _index_name, keyspace()));
}

}

}
