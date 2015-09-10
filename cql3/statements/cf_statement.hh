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

#include "cql3/statements/parsed_statement.hh"
#include "cql3/cf_name.hh"

#include <experimental/optional>

namespace cql3 {

namespace statements {

/**
 * Abstract class for statements that apply on a given column family.
 */
class cf_statement : public parsed_statement {
protected:
    ::shared_ptr<cf_name> _cf_name;

    cf_statement(::shared_ptr<cf_name> cf_name)
        : _cf_name(std::move(cf_name))
    { }

public:
    virtual void prepare_keyspace(const service::client_state& state) {
        if (!_cf_name->has_keyspace()) {
            // XXX: We explicitely only want to call state.getKeyspace() in this case, as we don't want to throw
            // if not logged in any keyspace but a keyspace is explicitely set on the statement. So don't move
            // the call outside the 'if' or replace the method by 'prepareKeyspace(state.getKeyspace())'
            _cf_name->set_keyspace(state.get_keyspace(), true);
        }
    }

    // Only for internal calls, use the version with ClientState for user queries
    virtual void prepare_keyspace(sstring keyspace) {
        if (!_cf_name->has_keyspace()) {
            _cf_name->set_keyspace(keyspace, true);
        }
    }

    virtual const sstring& keyspace() const {
        assert(_cf_name->has_keyspace()); // "The statement hasn't be prepared correctly";
        return _cf_name->get_keyspace();
    }

    virtual const sstring& column_family() const {
        return _cf_name->get_column_family();
    }
};

}

}
