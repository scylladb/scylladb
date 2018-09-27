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
#include "cql3/assignment_testable.hh"
#include "types.hh"
#include "schema.hh"

namespace cql3 {

namespace selection {

class result_set_builder;

/**
 * A <code>selector</code> is used to convert the data returned by the storage engine into the data requested by the
 * user. They correspond to the &lt;selector&gt; elements from the select clause.
 * <p>Since the introduction of aggregation, <code>selector</code>s cannot be called anymore by multiple threads
 * as they have an internal state.</p>
 */
class selector : public assignment_testable {
public:
    class factory;

    virtual ~selector() {}

    /**
     * Add the current value from the specified <code>result_set_builder</code>.
     *
     * @param protocol_version protocol version used for serialization
     * @param rs the <code>result_set_builder</code>
     * @throws InvalidRequestException if a problem occurs while add the input value
     */
    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) = 0;

    /**
     * Returns the selector output.
     *
     * @param protocol_version protocol version used for serialization
     * @return the selector output
     * @throws InvalidRequestException if a problem occurs while computing the output value
     */
    virtual bytes_opt get_output(cql_serialization_format sf) = 0;

    /**
     * Returns the <code>selector</code> output type.
     *
     * @return the <code>selector</code> output type.
     */
    virtual data_type get_type() = 0;

    /**
     * Checks if this <code>selector</code> is creating aggregates.
     *
     * @return <code>true</code> if this <code>selector</code> is creating aggregates <code>false</code>
     * otherwise.
     */
    virtual bool is_aggregate() {
        return false;
    }

    /**
     * Reset the internal state of this <code>selector</code>.
     */
    virtual void reset() = 0;

    virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
        auto t1 = receiver->type->underlying_type();
        auto t2 = get_type()->underlying_type();
        if (t1 == t2) {
            return assignment_testable::test_result::EXACT_MATCH;
        } else if (t1->is_value_compatible_with(*t2)) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        } else {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
    }
};

/**
 * A factory for <code>selector</code> instances.
 */
class selector::factory {
public:
    virtual ~factory() {}

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) {
        return false;
    }

    /**
     * Returns the column specification corresponding to the output value of the selector instances created by
     * this factory.
     *
     * @param schema the column family schema
     * @return a column specification
     */
    ::shared_ptr<column_specification> get_column_specification(schema_ptr schema);

    /**
     * Creates a new <code>selector</code> instance.
     *
     * @return a new <code>selector</code> instance
     */
    virtual ::shared_ptr<selector> new_instance() = 0;

    /**
     * Checks if this factory creates selectors instances that creates aggregates.
     *
     * @return <code>true</code> if this factory creates selectors instances that creates aggregates,
     * <code>false</code> otherwise
     */
    virtual bool is_aggregate_selector_factory() {
        return false;
    }

    /**
     * Checks if this factory creates <code>writetime</code> selectors instances.
     *
     * @return <code>true</code> if this factory creates <code>writetime</code> selectors instances,
     * <code>false</code> otherwise
     */
    virtual bool is_write_time_selector_factory() {
        return false;
    }

    /**
     * Checks if this factory creates <code>TTL</code> selectors instances.
     *
     * @return <code>true</code> if this factory creates <code>TTL</code> selectors instances,
     * <code>false</code> otherwise
     */
    virtual bool is_ttl_selector_factory() {
        return false;
    }

    /**
     * Returns the name of the column corresponding to the output value of the selector instances created by
     * this factory.
     *
     * @return a column name
     */
    virtual sstring column_name() = 0;

    /**
     * Returns the type of the values returned by the selector instances created by this factory.
     *
     * @return the selector output type
     */
    virtual data_type get_return_type() = 0;
};

}

}
