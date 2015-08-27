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
 * Modified by Cloudius Systems
 *
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "column_specification.hh"
#include "term.hh"
#include "column_identifier.hh"
#include "constants.hh"
#include "to_string.hh"

namespace cql3 {

/**
 * Static helper methods and classes for user types.
 */
class user_types {
    user_types() = delete;
public:
    static shared_ptr<column_specification> field_spec_of(shared_ptr<column_specification> column, size_t field);

    class literal : public term::raw {
    public:
        using elements_map_type = std::unordered_map<column_identifier, shared_ptr<term::raw>>;
        elements_map_type _entries;

        literal(elements_map_type entries);
        virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override;
    private:
        void validate_assignable_to(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver);
    public:
        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override;
        virtual sstring assignment_testable_source_context() const override;
        virtual sstring to_string() const override;
    };

    // Same purpose than Lists.DelayedValue, except we do handle bind marker in that case
    class delayed_value : public non_terminal {
        user_type _type;
        std::vector<shared_ptr<term>> _values;
    public:
        delayed_value(user_type type, std::vector<shared_ptr<term>> values);
        virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override;
        virtual bool contains_bind_marker() const override;
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names);
    private:
        std::vector<bytes_opt> bind_internal(const query_options& options);
    public:
        virtual shared_ptr<terminal> bind(const query_options& options) override;
        virtual bytes_view_opt bind_and_get(const query_options& options) override;
    };
};

}
