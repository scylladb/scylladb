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
 * Copyright 2014 Cloudius Systems
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

#include "transport/messages_fwd.hh"
#include "transport/event.hh"

#include "cql3/statements/cf_statement.hh"
#include "cql3/cql_statement.hh"

#include "core/shared_ptr.hh"

#include <experimental/optional>

namespace cql3 {

namespace statements {

namespace messages = transport::messages;

/**
 * Abstract class for statements that alter the schema.
 */
class schema_altering_statement : public cf_statement, public cql_statement, public ::enable_shared_from_this<schema_altering_statement> {
private:
    const bool _is_column_family_level;

    future<::shared_ptr<messages::result_message>>
    execute0(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options, bool);
protected:
    schema_altering_statement();

    schema_altering_statement(::shared_ptr<cf_name> name);

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override;

    virtual bool depends_on_keyspace(const sstring& ks_name) const override;

    virtual bool depends_on_column_family(const sstring& cf_name) const override;

    virtual uint32_t get_bound_terms() override;

    virtual void prepare_keyspace(const service::client_state& state) override;

    virtual ::shared_ptr<prepared> prepare(database& db) override;

    virtual shared_ptr<transport::event::schema_change> change_event() = 0;

    /**
     * Announces the migration to other nodes in the cluster.
     * @return true if the execution of this statement resulted in a schema change, false otherwise (when IF NOT EXISTS
     * is used, for example)
     * @throws RequestValidationException
     */
    virtual future<bool> announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) = 0;

    virtual future<::shared_ptr<messages::result_message>>
    execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) override;

    virtual future<::shared_ptr<messages::result_message>>
    execute_internal(distributed<service::storage_proxy>&, service::query_state& state, const query_options& options) override;
};

}

}
