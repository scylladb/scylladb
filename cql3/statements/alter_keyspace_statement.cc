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

#include "alter_keyspace_statement.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "database.hh"
#include "cql3/query_processor.hh"

bool is_system_keyspace(std::string_view keyspace);

cql3::statements::alter_keyspace_statement::alter_keyspace_statement(sstring name, ::shared_ptr<ks_prop_defs> attrs)
    : _name(name)
    , _attrs(std::move(attrs))
{}

const sstring& cql3::statements::alter_keyspace_statement::keyspace() const {
    return _name;
}

future<> cql3::statements::alter_keyspace_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const {
    return state.has_keyspace_access(_name, auth::permission::ALTER);
}

void cql3::statements::alter_keyspace_statement::validate(service::storage_proxy& proxy, const service::client_state& state) const {
    try {
        proxy.get_db().local().find_keyspace(_name); // throws on failure
        auto tmp = _name;
        std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
        if (is_system_keyspace(tmp)) {
            throw exceptions::invalid_request_exception("Cannot alter system keyspace");
        }

        _attrs->validate();

        if (!bool(_attrs->get_replication_strategy_class()) && !_attrs->get_replication_options().empty()) {
            throw exceptions::configuration_exception("Missing replication strategy class");
        }
#if 0
        // The strategy is validated through KSMetaData.validate() in announceKeyspaceUpdate below.
        // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
        // so doing proper validation here.
        AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                                AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                                StorageService.instance.getTokenMetadata(),
                                                                DatabaseDescriptor.getEndpointSnitch(),
                                                                attrs.getReplicationOptions());
#endif


    } catch (no_such_keyspace& e) {
        std::throw_with_nested(exceptions::invalid_request_exception("Unknown keyspace " + _name));
    }
}

future<shared_ptr<cql_transport::event::schema_change>> cql3::statements::alter_keyspace_statement::announce_migration(query_processor& qp) const {
    service::storage_proxy& proxy = qp.proxy();
    auto old_ksm = proxy.get_db().local().find_keyspace(_name).metadata();
    const auto& tm = *proxy.get_token_metadata_ptr();
    return qp.get_migration_manager().announce_keyspace_update(_attrs->as_ks_metadata_update(old_ksm, tm)).then([this] {
        using namespace cql_transport;
        return ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::KEYSPACE,
                keyspace());
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_keyspace_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_keyspace_statement>(*this));
}

