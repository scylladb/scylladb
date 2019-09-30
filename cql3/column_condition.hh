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

#include "cql3/term.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/operator.hh"

namespace cql3 {

/**
 * A CQL3 condition on the value of a column or collection element.  For example, "UPDATE .. IF a = 0".
 */
class column_condition final {
public:
    const column_definition& column;
private:
    // For collection, when testing the equality of a specific element, nullptr otherwise.
    ::shared_ptr<term> _collection_element;
    ::shared_ptr<term> _value;
    std::vector<::shared_ptr<term>> _in_values;
    const operator_type& _op;
public:
    column_condition(const column_definition& column, ::shared_ptr<term> collection_element,
        ::shared_ptr<term> value, std::vector<::shared_ptr<term>> in_values, const operator_type& op)
            : column(column)
            , _collection_element(std::move(collection_element))
            , _value(std::move(value))
            , _in_values(std::move(in_values))
            , _op(op)
    {
        if (op != operator_type::IN) {
            assert(_in_values.empty());
        }
    }

    static ::shared_ptr<column_condition> condition(const column_definition& def, ::shared_ptr<term> value, const operator_type& op) {
        return ::make_shared<column_condition>(def, ::shared_ptr<term>{}, std::move(value), std::vector<::shared_ptr<term>>{}, op);
    }

    static ::shared_ptr<column_condition> condition(const column_definition& def, ::shared_ptr<term> collection_element,
            ::shared_ptr<term> value, const operator_type& op) {
        return ::make_shared<column_condition>(def, std::move(collection_element), std::move(value),
            std::vector<::shared_ptr<term>>{}, op);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, std::vector<::shared_ptr<term>> in_values) {
        return ::make_shared<column_condition>(def, ::shared_ptr<term>{}, ::shared_ptr<term>{},
            std::move(in_values), operator_type::IN);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, ::shared_ptr<term> collection_element,
            std::vector<::shared_ptr<term>> in_values) {
        return ::make_shared<column_condition>(def, std::move(collection_element), ::shared_ptr<term>{},
            std::move(in_values), operator_type::IN);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, ::shared_ptr<term> in_marker) {
        return ::make_shared<column_condition>(def, ::shared_ptr<term>{}, std::move(in_marker),
            std::vector<::shared_ptr<term>>{}, operator_type::IN);
    }

    static ::shared_ptr<column_condition> in_condition(const column_definition& def, ::shared_ptr<term> collection_element,
        ::shared_ptr<term> in_marker) {
        return ::make_shared<column_condition>(def, std::move(collection_element), std::move(in_marker),
            std::vector<::shared_ptr<term>>{}, operator_type::IN);
    }

    bool uses_function(const sstring& ks_name, const sstring& function_name);
public:
    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    void collect_marker_specificaton(::shared_ptr<variable_specifications> bound_names);

    class raw final {
    private:
        ::shared_ptr<term::raw> _value;
        std::vector<::shared_ptr<term::raw>> _in_values;
        ::shared_ptr<abstract_marker::in_raw> _in_marker;

        // Can be nullptr, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        ::shared_ptr<term::raw> _collection_element;
        const operator_type& _op;
    public:
        raw(::shared_ptr<term::raw> value,
            std::vector<::shared_ptr<term::raw>> in_values,
            ::shared_ptr<abstract_marker::in_raw> in_marker,
            ::shared_ptr<term::raw> collection_element,
            const operator_type& op)
                : _value(std::move(value))
                , _in_values(std::move(in_values))
                , _in_marker(std::move(in_marker))
                , _collection_element(std::move(collection_element))
                , _op(op)
        { }

        /** A condition on a column. For example: "IF col = 'foo'" */
        static ::shared_ptr<raw> simple_condition(::shared_ptr<term::raw> value, const operator_type& op) {
            return ::make_shared<raw>(std::move(value), std::vector<::shared_ptr<term::raw>>{},
                ::shared_ptr<abstract_marker::in_raw>{}, ::shared_ptr<term::raw>{}, op);
        }

        /** An IN condition on a column. For example: "IF col IN ('foo', 'bar', ...)" */
        static ::shared_ptr<raw> simple_in_condition(std::vector<::shared_ptr<term::raw>> in_values) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::move(in_values),
                ::shared_ptr<abstract_marker::in_raw>{}, ::shared_ptr<term::raw>{}, operator_type::IN);
        }

        /** An IN condition on a column with a single marker. For example: "IF col IN ?" */
        static ::shared_ptr<raw> simple_in_condition(::shared_ptr<abstract_marker::in_raw> in_marker) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::vector<::shared_ptr<term::raw>>{},
                std::move(in_marker), ::shared_ptr<term::raw>{}, operator_type::IN);
        }

        /** A condition on a collection element. For example: "IF col['key'] = 'foo'" */
        static ::shared_ptr<raw> collection_condition(::shared_ptr<term::raw> value, ::shared_ptr<term::raw> collection_element,
                const operator_type& op) {
            return ::make_shared<raw>(std::move(value), std::vector<::shared_ptr<term::raw>>{}, ::shared_ptr<abstract_marker::in_raw>{}, std::move(collection_element), op);
        }

        /** An IN condition on a collection element. For example: "IF col['key'] IN ('foo', 'bar', ...)" */
        static ::shared_ptr<raw> collection_in_condition(::shared_ptr<term::raw> collection_element,
                std::vector<::shared_ptr<term::raw>> in_values) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::move(in_values), ::shared_ptr<abstract_marker::in_raw>{},
                std::move(collection_element), operator_type::IN);
        }

        /** An IN condition on a collection element with a single marker. For example: "IF col['key'] IN ?" */
        static ::shared_ptr<raw> collection_in_condition(::shared_ptr<term::raw> collection_element,
                ::shared_ptr<abstract_marker::in_raw> in_marker) {
            return ::make_shared<raw>(::shared_ptr<term::raw>{}, std::vector<::shared_ptr<term::raw>>{}, std::move(in_marker),
                std::move(collection_element), operator_type::IN);
        }

        ::shared_ptr<column_condition> prepare(database& db, const sstring& keyspace, const column_definition& receiver);
    };
};

}
