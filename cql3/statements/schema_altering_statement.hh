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
 * Copyright (C) 2014 ScyllaDB
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

#include "transport/messages_fwd.hh"
#include "transport/event.hh"

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/cql_statement.hh"

#include "core/shared_ptr.hh"

#include <experimental/optional>

namespace cql3 {

namespace statements {

namespace messages = cql_transport::messages;

/**
 * Abstract class for statements that alter the schema.
 */
class schema_altering_statement : public raw::cf_statement, public cql_statement_no_metadata {
private:
    const bool _is_column_family_level;

    future<::shared_ptr<messages::result_message>>
    execute0(service::storage_proxy& proxy, service::query_state& state, const query_options& options, bool);
protected:
    explicit schema_altering_statement(timeout_config_selector timeout_selector = &timeout_config::other_timeout);

    schema_altering_statement(::shared_ptr<cf_name> name, timeout_config_selector timeout_selector = &timeout_config::other_timeout);

    /**
     * When a new database object (keyspace, table) is created, the creator needs to be granted all applicable
     * permissions on it.
     *
     * By default, this function does nothing.
     */
    virtual future<> grant_permissions_to_creator(const service::client_state&);

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override;

    virtual bool depends_on_keyspace(const sstring& ks_name) const override;

    virtual bool depends_on_column_family(const sstring& cf_name) const override;

    virtual uint32_t get_bound_terms() override;

    virtual void prepare_keyspace(const service::client_state& state) override;

    virtual future<::shared_ptr<cql_transport::event::schema_change>> announce_migration(service::storage_proxy& proxy, bool is_local_only) = 0;

    virtual future<::shared_ptr<messages::result_message>>
    execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) override;
};

}

}
