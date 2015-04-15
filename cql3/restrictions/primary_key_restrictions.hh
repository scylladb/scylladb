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

#pragma once

#include <vector>

#include "cql3/query_options.hh"
#include "cql3/statements/bound.hh"
#include "cql3/restrictions/restrictions.hh"
#include "cql3/restrictions/restriction.hh"
#include "cql3/restrictions/abstract_restriction.hh"
#include "types.hh"
#include "query-request.hh"
#include "core/shared_ptr.hh"

namespace cql3 {
namespace restrictions {

/**
 * A set of restrictions on a primary key part (partition key or clustering key).
 *
 * XXX: In Origin this was also inheriting from "restriction", but I untangled it because it was complicating things.
 *
 * What was in AbstractPrimaryKeyRestrictions was moved here (In pre 1.8 Java interfaces could not have default
 * implementations of methods).
 */
template<typename ValueType>
class primary_key_restrictions : public restrictions {
public:
    virtual void merge_with(::shared_ptr<restriction> restriction) = 0;

    virtual std::vector<ValueType> values(const query_options& options) = 0;

    virtual std::vector<query::range<ValueType>> bounds(const query_options& options) = 0;

    virtual bool is_inclusive(statements::bound b) { return true; }

    virtual bool is_on_token() { return false; }

    virtual bool is_multi_column() { return false; }

    virtual bool is_slice() { return false; }

    virtual bool is_contains() { return false; }

    virtual bool is_IN() { return false; }
};

}
}
