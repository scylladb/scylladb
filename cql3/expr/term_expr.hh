/*
 * Copyright (C) 2020-present ScyllaDB
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
#include "expression.hh"

// A term::raw that is implemented using an expression
namespace cql3::expr {

class term_raw_expr final : public term::raw {
    expression _expr;
public:
    explicit term_raw_expr(expression expr) : _expr(std::move(expr)) {}

    virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver) const override;

    virtual sstring to_string() const override;

    virtual test_result test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const override;

    virtual sstring assignment_testable_source_context() const override;

    const expression& as_expression() const {
        return _expr;
    }
};


}