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
    expr::expression _expr;
public:
    explicit column_condition(expr::expression expr);

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
    bool applies_to(const expr::evaluation_inputs& inputs) const;

    class raw final {
    private:
        expr::expression _expr;
    public:
        explicit raw(expr::expression expr)
                : _expr(std::move(expr))
        { }

        static lw_shared_ptr<raw> make(expr::expression expr) {
            return make_lw_shared<raw>(std::move(expr));
        }

        lw_shared_ptr<column_condition> prepare(data_dictionary::database db, const sstring& keyspace, const schema& schema) const;
    };
};

} // end of namespace cql3
