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

#pragma once

#include <regex>

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "service/migration_manager.hh"
#include "transport/event.hh"

#include "core/shared_ptr.hh"

namespace cql3 {

namespace statements {

/** A <code>CREATE KEYSPACE</code> statement parsed from a CQL query. */
class create_keyspace_statement : public schema_altering_statement {
private:
    sstring _name;
    shared_ptr<ks_prop_defs> _attrs;
    bool _if_not_exists;

public:
    /**
     * Creates a new <code>CreateKeyspaceStatement</code> instance for a given
     * keyspace name and keyword arguments.
     *
     * @param name the name of the keyspace to create
     * @param attrs map of the raw keyword arguments that followed the <code>WITH</code> keyword.
     */
    create_keyspace_statement(const sstring& name, shared_ptr<ks_prop_defs> attrs, bool if_not_exists)
        : _name{name}
        , _attrs{attrs}
        , _if_not_exists{if_not_exists}
    { }

    virtual const sstring& keyspace() const override {
        return _name;
    }

    virtual void check_access(const service::client_state& state) override {
        warn(unimplemented::cause::PERMISSIONS);
#if 0
        state.hasAllKeyspacesAccess(Permission.CREATE);
#endif
    }

    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating.
     *
     * @throws InvalidRequestException if arguments are missing or unacceptable
     */
    virtual void validate(distributed<service::storage_proxy>&, const service::client_state& state) override {
        std::string name;
        name.resize(_name.length());
        std::transform(_name.begin(), _name.end(), name.begin(), ::tolower);
        if (name == db::system_keyspace::NAME) {
            throw exceptions::invalid_request_exception("system keyspace is not user-modifiable");
        }
        // keyspace name
        std::regex name_regex("\\w+");
        if (!std::regex_match(name, name_regex)) {
            throw exceptions::invalid_request_exception(sprint("\"%s\" is not a valid keyspace name", _name.c_str()));
        }
        if (name.length() > schema::NAME_LENGTH) {
            throw exceptions::invalid_request_exception(sprint("Keyspace names shouldn't be more than %" PRId32 " characters long (got \"%s\")", schema::NAME_LENGTH, _name.c_str()));
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

    virtual future<bool> announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) override {
        return make_ready_future<>().then([this, is_local_only] {
            return service::get_local_migration_manager().announce_new_keyspace(_attrs->as_ks_metadata(_name), is_local_only);
        }).then_wrapped([this] (auto&& f) {
            try {
                f.get();
                return true;
            } catch (const exceptions::already_exists_exception& e) {
                if (_if_not_exists) {
                    return false;
                }
                throw e;
            }
        });
    }

    virtual shared_ptr<transport::event::schema_change> change_event() override {
        return make_shared<transport::event::schema_change>(transport::event::schema_change::change_type::CREATED, keyspace());
    }
};

}

}
