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
 * Copyright (C) 2015-present ScyllaDB
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

#include "cql3/abstract_marker.hh"
#include "maps.hh"
#include "column_specification.hh"
#include "column_identifier.hh"
#include "to_string.hh"
#include <unordered_set>

namespace cql3 {

/**
 * Static helper methods and classes for sets.
 */
class sets {
    sets() = delete;
public:
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification& column);

    class literal : public term::raw {
        std::vector<shared_ptr<term::raw>> _elements;
    public:
        explicit literal(std::vector<shared_ptr<term::raw>> elements)
                : _elements(std::move(elements)) {
        }
        virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const override;
        void validate_assignable_to(database& db, const sstring& keyspace, const column_specification& receiver) const;
        assignment_testable::test_result
        test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const;
        virtual sstring to_string() const override;
    };

    class value : public terminal, collection_terminal {
    public:
        std::set<managed_bytes, serialized_compare> _elements;
    public:
        value(std::set<managed_bytes, serialized_compare> elements)
                : _elements(std::move(elements)) {
        }
        static value from_serialized(const raw_value_view& v, const set_type_impl& type, cql_serialization_format sf);
        virtual cql3::raw_value get(const query_options& options) override;
        virtual managed_bytes get_with_protocol_version(cql_serialization_format sf) override;
        bool equals(const set_type_impl& st, const value& v);
        virtual sstring to_string() const override;
    };

    // See Lists.DelayedValue
    class delayed_value : public non_terminal {
        serialized_compare _comparator;
        std::vector<shared_ptr<term>> _elements;
    public:
        delayed_value(serialized_compare comparator, std::vector<shared_ptr<term>> elements)
            : _comparator(std::move(comparator)), _elements(std::move(elements)) {
        }
        virtual bool contains_bind_marker() const override;
        virtual void collect_marker_specification(variable_specifications& bound_names) const override;
        virtual shared_ptr<terminal> bind(const query_options& options);
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver);
        virtual ::shared_ptr<terminal> bind(const query_options& options) override;
    };

    class setter : public operation {
    public:
        setter(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, ::shared_ptr<terminal> value);
    };

    class adder : public operation {
    public:
        adder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void do_add(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params,
                shared_ptr<term> value, const column_definition& column);
    };

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    class discarder : public operation {
    public:
        discarder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class element_discarder : public operation {
    public:
        element_discarder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) { }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
