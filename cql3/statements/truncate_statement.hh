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

#pragma once

#include "cql3/statements/cf_statement.hh"
#include "cql3/cql_statement.hh"

#include <experimental/optional>

namespace cql3 {

namespace statements {

class truncate_statement : public cf_statement, public cql_statement, public ::enable_shared_from_this<truncate_statement> {
public:
    truncate_statement(::shared_ptr<cf_name> name)
        : cf_statement{std::move(name)}
    { }

    virtual uint32_t get_bound_terms() override {
        return 0;
    }

    virtual ::shared_ptr<prepared> prepare(database& db) override {
        return ::make_shared<parsed_statement::prepared>(this->shared_from_this());
    }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return parsed_statement::uses_function(ks_name, function_name);
    }

    virtual void check_access(const service::client_state& state) override {
        throw std::runtime_error("not implemented");
#if 0
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);
#endif
    }

    virtual void validate(distributed<service::storage_proxy>&, const service::client_state& state) override {
        throw std::runtime_error("not implemented");
#if 0
        ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
#endif
    }

    virtual future<::shared_ptr<transport::messages::result_message>>
    execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) override {
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

    virtual future<::shared_ptr<transport::messages::result_message>>
    execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) override {
        throw std::runtime_error("unsupported operation");
    }
};

}

}
