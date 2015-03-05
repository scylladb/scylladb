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

#ifndef CQL3_STATEMENTS_SCHEMA_ALTERING_STATEMENT_HH
#define CQL3_STATEMENTS_SCHEMA_ALTERING_STATEMENT_HH

#include "transport/messages/result_message.hh"
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
class schema_altering_statement : public cf_statement, public cql_statement {
private:
    const bool _is_column_family_level;

protected:
    schema_altering_statement()
        : cf_statement{::shared_ptr<cf_name>{}}
        , _is_column_family_level{false}
    { }

    schema_altering_statement(::shared_ptr<cf_name>&& name)
        : cf_statement{std::move(name)}
        , _is_column_family_level{true}
    { }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return cf_statement::uses_function(ks_name, function_name);
    }

    virtual uint32_t get_bound_terms() override {
        return 0;
    }

    virtual void prepare_keyspace(service::client_state& state) override {
        if (_is_column_family_level) {
            cf_statement::prepare_keyspace(state);
        }
    }

    virtual shared_ptr<transport::event::schema_change> change_event() = 0;

    /**
     * Announces the migration to other nodes in the cluster.
     * @return true if the execution of this statement resulted in a schema change, false otherwise (when IF NOT EXISTS
     * is used, for example)
     * @throws RequestValidationException
     */
    virtual bool announce_migration(bool is_local_only) = 0;

    virtual future<::shared_ptr<messages::result_message>>
    execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) override {
        // If an IF [NOT] EXISTS clause was used, this may not result in an actual schema change.  To avoid doing
        // extra work in the drivers to handle schema changes, we return an empty message in this case. (CASSANDRA-7600)
        bool did_change_schema = announce_migration(false);
        if (!did_change_schema) {
            auto result = ::make_shared<messages::result_message::void_message>();
            return make_ready_future<::shared_ptr<messages::result_message>>(result);
        }
        auto ce = change_event();
        ::shared_ptr<messages::result_message> result;
        if (!ce) {
            result = ::make_shared<messages::result_message::void_message>();
        } else {
            result = ::make_shared<messages::result_message::schema_change>(ce);
        }
        return make_ready_future<::shared_ptr<messages::result_message>>(result);
    }

    virtual future<::shared_ptr<messages::result_message>>
    execute_internal(database& db, service::query_state& state, const query_options& options) override {
        throw std::runtime_error("unsupported operation");
#if 0
        try
        {
            boolean didChangeSchema = announceMigration(true);
            if (!didChangeSchema)
                return new ResultMessage.Void();

            Event.SchemaChange ce = changeEvent();
            return ce == null ? new ResultMessage.Void() : new ResultMessage.SchemaChange(ce);
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
#endif
    }
};

}

}

#endif
