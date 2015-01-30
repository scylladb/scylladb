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

#ifndef CQL3_CQL_STATEMENT_HH
#define CQL3_CQL_STATEMENT_HH

#include "transport/messages/result_message.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "cql3/query_options.hh"
#include "database.hh"

namespace cql3 {

class cql_statement {
public:
    virtual ~cql_statement()
    { }

    virtual int get_bound_terms() = 0;

    /**
     * Perform any access verification necessary for the statement.
     *
     * @param state the current client state
     */
    virtual void check_access(const service::client_state& state) = 0;

    /**
     * Perform additional validation required by the statment.
     * To be overriden by subclasses if needed.
     *
     * @param state the current client state
     */
    virtual void validate(const service::client_state& state) = 0;

    /**
     * Execute the statement and return the resulting result or null if there is no result.
     *
     * @param state the current query state
     * @param options options for this query (consistency, variables, pageSize, ...)
     */
    virtual future<std::experimental::optional<transport::messages::result_message>>
        execute(service::query_state& state, const query_options& options) = 0;

    /**
     * Variante of execute used for internal query against the system tables, and thus only query the local node = 0.
     *
     * @param state the current query state
     */
    virtual future<std::experimental::optional<transport::messages::result_message>>
        execute_internal(service::query_state& state, const query_options& options) = 0;

    virtual bool uses_function(sstring ks_name, sstring function_name) const = 0;
};

}

#endif
