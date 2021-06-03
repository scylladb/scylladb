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

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "transport/event.hh"
#include "log.hh"

#include <seastar/core/shared_ptr.hh>

namespace cql3 {

class query_processor;

namespace statements {

/** A <code>CREATE KEYSPACE</code> statement parsed from a CQL query. */
class create_keyspace_statement : public schema_altering_statement {
private:
    static logging::logger _logger;

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
    create_keyspace_statement(const sstring& name, shared_ptr<ks_prop_defs> attrs, bool if_not_exists);

    virtual const sstring& keyspace() const override;

    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;

    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating.
     *
     * @throws InvalidRequestException if arguments are missing or unacceptable
     */
    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;

    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(query_processor& qp) const override;

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;

    virtual future<> grant_permissions_to_creator(const service::client_state&) const override;

    virtual future<::shared_ptr<messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options) const override;
};

}

}
