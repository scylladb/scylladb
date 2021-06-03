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
 * Copyright 2015-present ScyllaDB
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

#include "cql3/statements/create_keyspace_statement.hh"
#include "prepared_statement.hh"
#include "database.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"

#include <regex>

bool is_system_keyspace(std::string_view keyspace);

namespace cql3 {

namespace statements {

logging::logger create_keyspace_statement::_logger("create_keyspace");

create_keyspace_statement::create_keyspace_statement(const sstring& name, shared_ptr<ks_prop_defs> attrs, bool if_not_exists)
    : _name{name}
    , _attrs{attrs}
    , _if_not_exists{if_not_exists}
{
}

const sstring& create_keyspace_statement::keyspace() const
{
    return _name;
}

future<> create_keyspace_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    return state.has_all_keyspaces_access(auth::permission::CREATE);
}

void create_keyspace_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    std::string name;
    name.resize(_name.length());
    std::transform(_name.begin(), _name.end(), name.begin(), ::tolower);
    if (is_system_keyspace(name)) {
        throw exceptions::invalid_request_exception("system keyspace is not user-modifiable");
    }
    // keyspace name
    std::regex name_regex("\\w+");
    if (!std::regex_match(name, name_regex)) {
        throw exceptions::invalid_request_exception(format("\"{}\" is not a valid keyspace name", _name.c_str()));
    }
    if (name.length() > schema::NAME_LENGTH) {
        throw exceptions::invalid_request_exception(format("Keyspace names shouldn't be more than {:d} characters long (got \"{}\")", schema::NAME_LENGTH, _name.c_str()));
    }

    _attrs->validate();

    if (!bool(_attrs->get_replication_strategy_class())) {
        throw exceptions::configuration_exception("Missing mandatory replication strategy class");
    }
#if 0
    // The strategy is validated through KSMetaData.validate() in announceNewKeyspace below.
    // However, for backward compatibility with thrift, this doesn't validate unexpected options yet,
    // so doing proper validation here.
    AbstractReplicationStrategy.validateReplicationStrategy(name,
                                                            AbstractReplicationStrategy.getClass(attrs.getReplicationStrategyClass()),
                                                            StorageService.instance.getTokenMetadata(),
                                                            DatabaseDescriptor.getEndpointSnitch(),
                                                            attrs.getReplicationOptions());
#endif
}

future<shared_ptr<cql_transport::event::schema_change>> create_keyspace_statement::announce_migration(query_processor& qp) const
{
    return make_ready_future<>().then([this, p = qp.proxy().shared_from_this(), &mm = qp.get_migration_manager()] {
        const auto& tm = *p->get_token_metadata_ptr();
        return mm.announce_new_keyspace(_attrs->as_ks_metadata(_name, tm));
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            using namespace cql_transport;
            return ::make_shared<event::schema_change>(
                    event::schema_change::change_type::CREATED,
                    event::schema_change::target_type::KEYSPACE,
                    this->keyspace());
        } catch (const exceptions::already_exists_exception& e) {
            if (_if_not_exists) {
                return ::shared_ptr<cql_transport::event::schema_change>();
            }
            throw e;
        }
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::create_keyspace_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_keyspace_statement>(*this));
}

future<> cql3::statements::create_keyspace_statement::grant_permissions_to_creator(const service::client_state& cs) const {
    return do_with(auth::make_data_resource(keyspace()), [&cs](const auth::resource& r) {
        return auth::grant_applicable_permissions(
                *cs.get_auth_service(),
                *cs.user(),
                r).handle_exception_type([](const auth::unsupported_authorization_operation&) {
            // Nothing.
        });
    });
}

future<::shared_ptr<messages::result_message>>
create_keyspace_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    service::storage_proxy& proxy = qp.proxy();
    return schema_altering_statement::execute(qp, state, options).then([this, p = proxy.shared_from_this()] (::shared_ptr<messages::result_message> msg) {
        bool multidc = p->get_token_metadata_ptr()->get_topology().get_datacenter_endpoints().size() > 1;
        bool simple = _attrs->get_replication_strategy_class() == "SimpleStrategy";

        if (multidc && simple) {
            static const auto warning = "Using SimpleStrategy in a multi-datacenter environment is not recommended.";

            msg->add_warning(warning);
            _logger.warn(warning);
        }

        return msg;
    });
}

}

}
