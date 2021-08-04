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
#include "cql3/column_identifier.hh"
#include "cql3/constants.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/lists.hh"
#include "cql3/sets.hh"
#include "cql3/user_types.hh"
#include "types/list.hh"

namespace cql3 {

abstract_marker::raw::raw(int32_t bind_index)
    : _bind_index{bind_index}
{ }

sstring
abstract_marker::raw::to_string() const {
    return "?";
}

assignment_testable::test_result
abstract_marker::raw::test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const {
    return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
}

::shared_ptr<term>
abstract_marker::raw::prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) const
{
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    if (receiver->type->is_collection()) {
        if (receiver->type->without_reversed().is_list()) {
            return ::make_shared<lists::marker>(_bind_index, receiver);
        } else if (receiver->type->without_reversed().is_set()) {
            return ::make_shared<sets::marker>(_bind_index, receiver);
        } else if (receiver->type->without_reversed().is_map()) {
            return ::make_shared<maps::marker>(_bind_index, receiver);
        }
        assert(0);
    }

    if (receiver->type->is_user_type()) {
        return ::make_shared<user_types::marker>(_bind_index, receiver);
    }

    return ::make_shared<constants::marker>(_bind_index, receiver);
}

abstract_marker::in_raw::in_raw(int32_t bind_index)
    : raw{bind_index}
{ }

lw_shared_ptr<column_specification>
abstract_marker::in_raw::make_in_receiver(const column_specification& receiver) {
    auto in_name = ::make_shared<column_identifier>(sstring("in(") + receiver.name->to_string() + sstring(")"), true);
    return make_lw_shared<column_specification>(receiver.ks_name, receiver.cf_name, in_name, list_type_impl::get_instance(receiver.type, false));
}

::shared_ptr<term>
abstract_marker::in_raw::prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) const {
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    return ::make_shared<lists::marker>(_bind_index, make_in_receiver(*receiver));
}

}

namespace cql3::expr {

static
assignment_testable::test_result
null_test_assignment(database& db,
        const sstring& keyspace,
        const column_specification& receiver) {
    return receiver.type->is_counter()
        ? assignment_testable::test_result::NOT_ASSIGNABLE
        : assignment_testable::test_result::WEAKLY_ASSIGNABLE;
}

static
::shared_ptr<term>
null_prepare_term(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver) {
    if (!is_assignable(null_test_assignment(db, keyspace, *std::get<lw_shared_ptr<column_specification>>(receiver)))) {
        throw exceptions::invalid_request_exception("Invalid null value for counter increment/decrement");
    }
    return constants::NULL_VALUE;
}

static
sstring
cast_display_name(const cast& c) {
    return format("({}){}", std::get<shared_ptr<cql3_type::raw>>(c.type), *c.arg);
}

static
lw_shared_ptr<column_specification>
casted_spec_of(const cast& c, database& db, const sstring& keyspace, const column_specification& receiver) {
    auto& type = std::get<shared_ptr<cql3_type::raw>>(c.type);
    return make_lw_shared<column_specification>(receiver.ks_name, receiver.cf_name,
            ::make_shared<column_identifier>(cast_display_name(c), true), type->prepare(db, keyspace).get_type());
}

static
assignment_testable::test_result
cast_test_assignment(const cast& c, database& db, const sstring& keyspace, const column_specification& receiver) {
    auto type = std::get<shared_ptr<cql3_type::raw>>(c.type);
    auto term = as_term_raw(*c.arg);
    try {
        auto&& casted_type = type->prepare(db, keyspace).get_type();
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

static
shared_ptr<term>
cast_prepare_term(const cast& c, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) {
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    auto type = std::get<shared_ptr<cql3_type::raw>>(c.type);
    auto term = as_term_raw(*c.arg);
    if (!is_assignable(term->test_assignment(db, keyspace, *casted_spec_of(c, db, keyspace, *receiver)))) {
        throw exceptions::invalid_request_exception(format("Cannot cast value {} to type {}", term, type));
    }
    if (!is_assignable(cast_test_assignment(c, db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception(format("Cannot assign value {} to {} of type {}", c, receiver->name, receiver->type->as_cql3_type()));
    }
    return term->prepare(db, keyspace, receiver);
}

// A term::raw that is implemented using an expression

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
        [&] (const cast& c) -> ::shared_ptr<term> {
            return cast_prepare_term(c, db, keyspace, receiver);
        },
        [&] (const field_selection&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "field_selections are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const term_raw_ptr& raw) -> ::shared_ptr<term> {
            return raw->prepare(db, keyspace, receiver);
        },
        [&] (const null&) -> ::shared_ptr<term> {
            return null_prepare_term(db, keyspace, receiver);
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
        [&] (const cast& c) -> test_result {
            return cast_test_assignment(c, db, keyspace, receiver);
        },
        [&] (const field_selection&) -> test_result {
            on_internal_error(expr_logger, "field_selections are not yet reachable via term_raw_expr::test_assignment()");
        },
        [&] (const term_raw_ptr& raw) -> test_result {
            return raw->test_assignment(db, keyspace, receiver);
        },
        [&] (const null&) -> test_result {
            return null_test_assignment(db, keyspace, receiver);
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