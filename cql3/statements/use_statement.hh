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

#ifndef CQL3_STATEMENTS_USE_STATEMENT_HH
#define CQL3_STATEMENTS_USE_STATEMENT_HH

#include "cql3/statements/parsed_statement.hh"
#include "cql3/cql_statement.hh"

namespace cql3 {

namespace statements {

class use_statement : public parsed_statement, public cql_statement, public ::enable_shared_from_this<use_statement> {
private:
    const sstring _keyspace;

public:
    use_statement(sstring keyspace)
        : _keyspace(keyspace)
    { }

    virtual uint32_t get_bound_terms() override {
        return 0;
    }

    virtual std::unique_ptr<prepared> prepare(database& db) override {
        return std::make_unique<parsed_statement::prepared>(this->shared_from_this());
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return parsed_statement::uses_function(ks_name, function_name);
    }

    virtual void check_access(const service::client_state& state) override {
        state.validate_login();
    }

    virtual void validate(const service::client_state& state) override {
    }

    virtual future<::shared_ptr<transport::messages::result_message>>
    execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) override {
        state.get_client_state().set_keyspace(_keyspace);
        auto result =::make_shared<transport::messages::result_message::set_keyspace>(_keyspace);
        return make_ready_future<::shared_ptr<transport::messages::result_message>>(result);
    }

    virtual future<::shared_ptr<transport::messages::result_message>>
    execute_internal(database& db, service::query_state& state, const query_options& options) override {
        // Internal queries are exclusively on the system keyspace and 'use' is thus useless
        throw std::runtime_error("unsupported operation");
    }
};

}

}

#endif
