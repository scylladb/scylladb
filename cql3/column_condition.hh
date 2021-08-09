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

#include "cql3/term.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/expr/expression.hh"
#include "utils/like_matcher.hh"

namespace cql3 {

/**
 * A CQL3 condition on the value of a column or collection element.  For example, "UPDATE .. IF a = 0".
 */
class column_condition final {
public:
    // If _collection_element is not zero, this defines the receiver cell, not the entire receiver
    // column.
    // E.g. if column type is list<string> and expression is "a = ['test']", then the type of the
    // column definition below is list<string>. If expression is "a[0] = 'test'", then the column
    // object stands for the string cell. See column_condition::raw::prepare() for details.
    const column_definition& column;
private:
    // For collection, when testing the equality of a specific element, nullptr otherwise.
    ::shared_ptr<term> _collection_element;
    // A literal value for comparison predicates or a multi item terminal for "a IN ?"
    ::shared_ptr<term> _value;
    // List of terminals for "a IN (value, value, ...)"
    std::vector<::shared_ptr<term>> _in_values;
    const std::unique_ptr<like_matcher> _matcher;
    expr::oper_t _op;
public:
    column_condition(const column_definition& column, ::shared_ptr<term> collection_element,
        ::shared_ptr<term> value, std::vector<::shared_ptr<term>> in_values,
        std::unique_ptr<like_matcher> matcher, expr::oper_t op)
            : column(column)
            , _collection_element(std::move(collection_element))
            , _value(std::move(value))
            , _in_values(std::move(in_values))
            , _matcher(std::move(matcher))
            , _op(op)
    {
        if (op != expr::oper_t::IN) {
            assert(_in_values.empty());
        }
    }
    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    void collect_marker_specificaton(prepare_context& ctx) const;

    // Retrieve parameter marker values, if any, find the appropriate collection
    // element if the cell is a collection and an element access is used in the expression,
    // and evaluate the condition.
    bool applies_to(const data_value* cell_value, const query_options& options) const;

    /**
     * Helper constructor wrapper for
     * "IF col['key'] = 'foo'"
     * "IF col = 'foo'"
     * "IF col LIKE <pattern>"
     */
    static lw_shared_ptr<column_condition> condition(const column_definition& def, ::shared_ptr<term> collection_element,
            ::shared_ptr<term> value, std::unique_ptr<like_matcher> matcher, expr::oper_t op) {
        return make_lw_shared<column_condition>(def, std::move(collection_element), std::move(value),
            std::vector<::shared_ptr<term>>{}, std::move(matcher), op);
    }

    // Helper constructor wrapper for  "IF col IN ... and IF col['key'] IN ... */
    static lw_shared_ptr<column_condition> in_condition(const column_definition& def, ::shared_ptr<term> collection_element,
            ::shared_ptr<term> in_marker, std::vector<::shared_ptr<term>> in_values) {
        return make_lw_shared<column_condition>(def, std::move(collection_element), std::move(in_marker),
            std::move(in_values), nullptr, expr::oper_t::IN);
    }

    class raw final {
    private:
        std::optional<cql3::expr::expression> _value;
        std::vector<cql3::expr::expression> _in_values;
        std::optional<cql3::expr::expression> _in_marker;

        // Can be nullopt, used with the syntax "IF m[e] = ..." (in which case it's 'e')
        std::optional<cql3::expr::expression> _collection_element;
        expr::oper_t _op;
    public:
        raw(std::optional<cql3::expr::expression> value,
            std::vector<cql3::expr::expression> in_values,
            std::optional<cql3::expr::expression> in_marker,
            std::optional<cql3::expr::expression> collection_element,
            expr::oper_t op)
                : _value(std::move(value))
                , _in_values(std::move(in_values))
                , _in_marker(std::move(in_marker))
                , _collection_element(std::move(collection_element))
                , _op(op)
        { }

        /**
         * A condition on a column or collection element.
         * For example:
         * "IF col['key'] = 'foo'"
         * "IF col = 'foo'"
         * "IF col LIKE 'foo%'"
         */
        static lw_shared_ptr<raw> simple_condition(cql3::expr::expression value, std::optional<cql3::expr::expression> collection_element,
                expr::oper_t op) {
            return make_lw_shared<raw>(std::move(value), std::vector<cql3::expr::expression>{},
                    std::nullopt, std::move(collection_element), op);
        }

        /**
         * An IN condition on a column or a collection element. IN may contain a list of values or a single marker.
         * For example:
         * "IF col IN ('foo', 'bar', ...)"
         * "IF col IN ?"
         * "IF col['key'] IN * ('foo', 'bar', ...)"
         * "IF col['key'] IN ?"
         */
        static lw_shared_ptr<raw> in_condition(std::optional<cql3::expr::expression> collection_element,
                std::optional<cql3::expr::expression> in_marker, std::vector<cql3::expr::expression> in_values) {
            return make_lw_shared<raw>(std::nullopt, std::move(in_values), std::move(in_marker),
                    std::move(collection_element), expr::oper_t::IN);
        }

        lw_shared_ptr<column_condition> prepare(database& db, const sstring& keyspace, const column_definition& receiver) const;
    };
};

} // end of namespace cql3
