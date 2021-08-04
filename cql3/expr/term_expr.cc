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
#include "cql3/functions/function_call.hh"
#include "cql3/type_cast.hh"

namespace cql3 {

sstring
type_cast::to_string() const {
    return format("({}){}", _type, _term);
}

lw_shared_ptr<column_specification>
type_cast::casted_spec_of(database& db, const sstring& keyspace, const column_specification& receiver) const {
    return make_lw_shared<column_specification>(receiver.ks_name, receiver.cf_name,
            ::make_shared<column_identifier>(to_string(), true), _type->prepare(db, keyspace).get_type());
}

assignment_testable::test_result
type_cast::test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const {
    try {
        auto&& casted_type = _type->prepare(db, keyspace).get_type();
        if (receiver.type == casted_type) {
            return assignment_testable::test_result::EXACT_MATCH;
        } else if (receiver.type->is_value_compatible_with(*casted_type)) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        } else {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
    } catch (exceptions::invalid_request_exception& e) {
        abort();
    }
}

shared_ptr<term>
type_cast::prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) const {
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    if (!is_assignable(_term->test_assignment(db, keyspace, *casted_spec_of(db, keyspace, *receiver)))) {
        throw exceptions::invalid_request_exception(format("Cannot cast value {} to type {}", _term, _type));
    }
    if (!is_assignable(test_assignment(db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception(format("Cannot assign value {} to {} of type {}", *this, receiver->name, receiver->type->as_cql3_type()));
    }
    return _term->prepare(db, keyspace, receiver);
}

}

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
        [&] (const function_call& fc) -> ::shared_ptr<term> {
            return functions::prepare_function_call(fc, db, keyspace, receiver);
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
        [&] (const function_call& fc) -> test_result {
            return functions::test_assignment_function_call(fc, db, keyspace, receiver);
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