/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "term_expr.hh"

// A term::raw that is implemented using an expression
namespace cql3::expr {

extern logging::logger expr_logger;

::shared_ptr<term>
term_raw_expr::prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver) const {
    return std::visit(overloaded_functor{
        [&] (bool bool_constant) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "bool constants are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const binary_operator&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "binary_operators are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const conjunction&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "conjunctions are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const column_value&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "column_values are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const column_value_tuple&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "column_value_tuples are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const token&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "tokens are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const unresolved_identifier&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "unresolved_identifiers are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const column_mutation_attribute&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "column_mutation_attributes are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const function_call&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "function_calls are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const cast&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "casts are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const field_selection&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "field_selections are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const term_raw_ptr& raw) -> ::shared_ptr<term> {
            return raw->prepare(db, keyspace, receiver);
        },
    }, _expr);
}

assignment_testable::test_result
term_raw_expr::test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const {
    return std::visit(overloaded_functor{
        [&] (bool bool_constant) -> test_result {
            on_internal_error(expr_logger, "bool constants are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const binary_operator&) -> test_result {
            on_internal_error(expr_logger, "binary_operators are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const conjunction&) -> test_result {
            on_internal_error(expr_logger, "conjunctions are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const column_value&) -> test_result {
            on_internal_error(expr_logger, "column_values are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const column_value_tuple&) -> test_result {
            on_internal_error(expr_logger, "column_value_tuples are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const token&) -> test_result {
            on_internal_error(expr_logger, "tokens are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const unresolved_identifier&) -> test_result {
            on_internal_error(expr_logger, "unresolved_identifiers are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const column_mutation_attribute&) -> test_result {
            on_internal_error(expr_logger, "column_mutation_attributes are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const function_call&) -> test_result {
            on_internal_error(expr_logger, "function_calls are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const cast&) -> test_result {
            on_internal_error(expr_logger, "casts are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const field_selection&) -> test_result {
            on_internal_error(expr_logger, "field_selections are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const term_raw_ptr& raw) -> test_result {
            return raw->test_assignment(db, keyspace, receiver);
        },
    }, _expr);
}

sstring
term_raw_expr::to_string() const {
    return std::visit(overloaded_functor{
        [&] (const term_raw_ptr& raw) {
            return raw->to_string();
        },
        [&] (auto& default_case) -> sstring { return fmt::format("{}", _expr); },
    }, _expr);
}

sstring
term_raw_expr::assignment_testable_source_context() const {
    return std::visit(overloaded_functor{
        [&] (const term_raw_ptr& raw) {
            return raw->assignment_testable_source_context();
        },
        [&] (auto& default_case) -> sstring { return fmt::format("{}", _expr); },
    }, _expr);
}


}