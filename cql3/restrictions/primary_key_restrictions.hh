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
 * Copyright (C) 2015 ScyllaDB
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
 * What was in AbstractPrimaryKeyRestrictions was moved here (In pre 1.8 Java interfaces could not have default
 * implementations of methods).
 */

template<typename ValueType>
struct range_type_for;

template<>
struct range_type_for<partition_key> : public std::remove_reference<dht::partition_range> {};
template<>
struct range_type_for<clustering_key_prefix> : public std::remove_reference<query::clustering_range> {};

template<typename ValueType>
class primary_key_restrictions: public abstract_restriction,
        public restrictions,
        public enable_shared_from_this<primary_key_restrictions<ValueType>> {
public:
    typedef typename range_type_for<ValueType>::type bounds_range_type;

    virtual ::shared_ptr<primary_key_restrictions<ValueType>> merge_to(schema_ptr, ::shared_ptr<restriction> restriction) {
        merge_with(restriction);
        return this->shared_from_this();
    }

    virtual std::vector<ValueType> values_as_keys(const query_options& options) const = 0;
    virtual std::vector<bounds_range_type> bounds_ranges(const query_options& options) const = 0;

    using restrictions::uses_function;
    using restrictions::has_supporting_index;

    bool empty() const override {
        return get_column_defs().empty();
    }
    uint32_t size() const override {
        return uint32_t(get_column_defs().size());
    }
};

}
}
