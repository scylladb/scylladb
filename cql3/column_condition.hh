/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

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
    const column_definition& _column;
private:
    // For collection, when testing the equality of a specific element, nullopt otherwise.
    std::optional<expr::expression> _collection_element;
    // A literal value for comparison predicates or a multi item terminal for "a IN ?"
    std::optional<expr::expression> _value;
    // List of terminals for "a IN (value, value, ...)"
    std::vector<expr::expression> _in_values;
    std::unique_ptr<like_matcher> _matcher;
    expr::oper_t _op;
public:
    column_condition(const column_definition& column, std::optional<expr::expression> collection_element,
        std::optional<expr::expression> value, std::vector<expr::expression> in_values,
        expr::oper_t op);

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    void collect_marker_specificaton(prepare_context& ctx);

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
    static lw_shared_ptr<column_condition> condition(const column_definition& def, std::optional<expr::expression> collection_element,
            expr::expression value, expr::oper_t op) {
        return make_lw_shared<column_condition>(def, std::move(collection_element), std::move(value),
            std::vector<expr::expression>{}, op);
    }

    // Helper constructor wrapper for  "IF col IN ... and IF col['key'] IN ... */
    static lw_shared_ptr<column_condition> in_condition(const column_definition& def, std::optional<expr::expression> collection_element,
            std::optional<expr::expression> in_marker, std::vector<expr::expression> in_values) {
        return make_lw_shared<column_condition>(def, std::move(collection_element), std::move(in_marker),
            std::move(in_values), expr::oper_t::IN);
    }

    const std::optional<expr::expression>& get_value() const {
        return _value;
    }

    expr::oper_t get_operation() const {
        return _op;
    }

    class raw final {
    private:
        shared_ptr<column_identifier::raw> _lhs;
        std::optional<cql3::expr::expression> _value;
        std::vector<cql3::expr::expression> _in_values;
        std::optional<cql3::expr::expression> _in_marker;

        // Can be nullopt, used with the syntax "IF m[e] = ..." (in which case it's 'e')
        std::optional<cql3::expr::expression> _collection_element;
        expr::oper_t _op;
    public:
        raw(shared_ptr<column_identifier::raw> lhs,
            std::optional<cql3::expr::expression> value,
            std::vector<cql3::expr::expression> in_values,
            std::optional<cql3::expr::expression> in_marker,
            std::optional<cql3::expr::expression> collection_element,
            expr::oper_t op)
                : _lhs(std::move(lhs))
                , _value(std::move(value))
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
        static lw_shared_ptr<raw> simple_condition(shared_ptr<column_identifier::raw> lhs, cql3::expr::expression value, std::optional<cql3::expr::expression> collection_element,
                expr::oper_t op) {
            return make_lw_shared<raw>(std::move(lhs), std::move(value), std::vector<cql3::expr::expression>{},
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
        static lw_shared_ptr<raw> in_condition(shared_ptr<column_identifier::raw> lhs, std::optional<cql3::expr::expression> collection_element,
                std::optional<cql3::expr::expression> in_marker, std::vector<cql3::expr::expression> in_values) {
            return make_lw_shared<raw>(std::move(lhs), std::nullopt, std::move(in_values), std::move(in_marker),
                    std::move(collection_element), expr::oper_t::IN);
        }

        lw_shared_ptr<column_condition> prepare(data_dictionary::database db, const sstring& keyspace, const schema& schema) const;
    };
};

} // end of namespace cql3
