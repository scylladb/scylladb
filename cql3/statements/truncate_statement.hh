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

#ifndef CQL3_STATEMENTS_TRUNCATE_STATEMENT_HH
#define CQL3_STATEMENTS_TRUNCATE_STATEMENT_HH

#include "cql3/statements/cf_statement.hh"
#include "cql3/cql_statement.hh"

#include <experimental/optional>

namespace cql3 {

namespace statements {

class truncate_statement : public cf_statement, public virtual cql_statement {
public:
    truncate_statement(std::experimental::optional<cf_name>&& name)
        : cf_statement{std::move(name)}
    { }

    virtual int get_bound_terms() override {
        return 0;
    }

    virtual std::unique_ptr<prepared> prepare(std::unique_ptr<cql_statement>&& stmt) override {
        return std::make_unique<parsed_statement::prepared>(std::move(stmt));
    }

    virtual void check_access(const service::client_state& state) override {
        throw std::runtime_error("not implemented");
#if 0
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);
#endif
    }

    virtual void validate(const service::client_state& state) override {
        throw std::runtime_error("not implemented");
#if 0
        ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
#endif
    }

    virtual transport::messages::result_message execute(service::query_state& state, const query_options& options) override {
        throw std::runtime_error("not implemented");
#if 0
        try
        {
            StorageProxy.truncateBlocking(keyspace(), columnFamily());
        }
        catch (UnavailableException e)
        {
            throw new TruncateException(e);
        }
        catch (TimeoutException e)
        {
            throw new TruncateException(e);
        }
        catch (IOException e)
        {
            throw new TruncateException(e);
        }
        return null;
#endif
    }

    virtual transport::messages::result_message execute_internal(service::query_state& state, const query_options& options) override {
        throw std::runtime_error("unsupported operation");
    }
};

}

}

#endif
