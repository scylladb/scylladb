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
#include "cql3/tuples.hh"
#include "types/list.hh"

namespace cql3 {

lw_shared_ptr<column_specification>
tuples::component_spec_of(const column_specification& column, size_t component) {
    return make_lw_shared<column_specification>(
            column.ks_name,
            column.cf_name,
            ::make_shared<column_identifier>(format("{}[{:d}]", column.name, component), true),
            static_pointer_cast<const tuple_type_impl>(column.type->underlying_type())->type(component));
}

void
tuples::literal::validate_assignable_to(database& db, const sstring& keyspace, const column_specification& receiver) const {
    auto tt = dynamic_pointer_cast<const tuple_type_impl>(receiver.type->underlying_type());
    if (!tt) {
        throw exceptions::invalid_request_exception(format("Invalid tuple type literal for {} of type {}", receiver.name, receiver.type->as_cql3_type()));
    }
    for (size_t i = 0; i < _elements.size(); ++i) {
        if (i >= tt->size()) {
            throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: too many elements. Type {} expects {:d} but got {:d}",
                                                            receiver.name, tt->as_cql3_type(), tt->size(), _elements.size()));
        }

        auto&& value = _elements[i];
        auto&& spec = component_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(value->test_assignment(db, keyspace, *spec))) {
            throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: component {:d} is not of type {}", receiver.name, i, spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
tuples::literal::test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const {
    try {
        validate_assignable_to(db, keyspace, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

shared_ptr<term>
tuples::literal::prepare_nontuple(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const {
    validate_assignable_to(db, keyspace, *receiver);
    std::vector<shared_ptr<term>> values;
    bool all_terminal = true;
    for (size_t i = 0; i < _elements.size(); ++i) {
        auto&& value = _elements[i]->prepare(db, keyspace, component_spec_of(*receiver, i));
        if (dynamic_pointer_cast<non_terminal>(value)) {
            all_terminal = false;
        }
        values.push_back(std::move(value));
    }
    delayed_value value(static_pointer_cast<const tuple_type_impl>(receiver->type), values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<delayed_value>(std::move(value));
    }
}

shared_ptr<term>
tuples::literal::prepare_tuple(database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) const {
    if (_elements.size() != receivers.size()) {
        throw exceptions::invalid_request_exception(format("Expected {:d} elements in value tuple, but got {:d}: {}", receivers.size(), _elements.size(), *this));
    }

    std::vector<shared_ptr<term>> values;
    std::vector<data_type> types;
    bool all_terminal = true;
    for (size_t i = 0; i < _elements.size(); ++i) {
        auto&& t = _elements[i]->prepare(db, keyspace, receivers[i]);
        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }
        values.push_back(t);
        types.push_back(receivers[i]->type);
    }
    delayed_value value(tuple_type_impl::get_instance(std::move(types)), std::move(values));
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<delayed_value>(std::move(value));
    }
}

shared_ptr<term>
tuples::literal::prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver) const {
    return std::visit(overloaded_functor{
        [&] (const lw_shared_ptr<column_specification>& nontuple) {
            return prepare_nontuple(db, keyspace, nontuple);
        },
        [&] (const std::vector<lw_shared_ptr<column_specification>>& tuple) {
            return prepare_tuple(db, keyspace, tuple);
        },
    }, receiver);
}

sstring
tuples::literal::to_string() const {
    return tuple_to_string(_elements);
}

}

namespace cql3::expr {

static
std::ostream&
operator<<(std::ostream&out, untyped_constant::type_class t)
{
    switch (t) {
        case untyped_constant::type_class::string:   return out << "STRING";
        case untyped_constant::type_class::integer:  return out << "INTEGER";
        case untyped_constant::type_class::uuid:     return out << "UUID";
        case untyped_constant::type_class::floating_point:    return out << "FLOAT";
        case untyped_constant::type_class::boolean:  return out << "BOOLEAN";
        case untyped_constant::type_class::hex:      return out << "HEX";
        case untyped_constant::type_class::duration: return out << "DURATION";
    }
    abort();
}

static
bytes
untyped_constant_parsed_value(const untyped_constant uc, data_type validator)
{
    try {
        if (uc.partial_type == untyped_constant::type_class::hex && validator == bytes_type) {
            auto v = static_cast<sstring_view>(uc.raw_text);
            v.remove_prefix(2);
            return validator->from_string(v);
        }
        if (validator->is_counter()) {
            return long_type->from_string(uc.raw_text);
        }
        return validator->from_string(uc.raw_text);
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

static
assignment_testable::test_result
untyped_constant_test_assignment(const untyped_constant& uc, database& db, const sstring& keyspace, const column_specification& receiver)
{
    auto receiver_type = receiver.type->as_cql3_type();
    if (receiver_type.is_collection() || receiver_type.is_user_type()) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
    if (!receiver_type.is_native()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    auto kind = receiver_type.get_kind();
    switch (uc.partial_type) {
        case untyped_constant::type_class::string:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::ASCII,
                    cql3_type::kind::TEXT,
                    cql3_type::kind::INET,
                    cql3_type::kind::TIMESTAMP,
                    cql3_type::kind::DATE,
                    cql3_type::kind::TIME>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::integer:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::BIGINT,
                    cql3_type::kind::COUNTER,
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT,
                    cql3_type::kind::INT,
                    cql3_type::kind::SMALLINT,
                    cql3_type::kind::TIMESTAMP,
                    cql3_type::kind::DATE,
                    cql3_type::kind::TINYINT,
                    cql3_type::kind::VARINT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::uuid:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::UUID,
                    cql3_type::kind::TIMEUUID>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::floating_point:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::boolean:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BOOLEAN>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::hex:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BLOB>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case untyped_constant::type_class::duration:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::DURATION>()) {
                return assignment_testable::test_result::EXACT_MATCH;
            }
            break;
    }
    return assignment_testable::test_result::NOT_ASSIGNABLE;
}

static
::shared_ptr<term>
untyped_constant_prepare_term(const untyped_constant& uc, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_)
{
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    if (!is_assignable(untyped_constant_test_assignment(uc, db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception(format("Invalid {} constant ({}) for \"{}\" of type {}",
            uc.partial_type, uc.raw_text, *receiver->name, receiver->type->as_cql3_type().to_string()));
    }
    return ::make_shared<constants::value>(cql3::raw_value::make_value(untyped_constant_parsed_value(uc, receiver->type)));
}

static
assignment_testable::test_result
bind_variable_test_assignment(const bind_variable& bv, database& db, const sstring& keyspace, const column_specification& receiver) {
    return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
}

static
::shared_ptr<term>
bind_variable_scalar_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_)
{
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    if (receiver->type->is_collection()) {
        if (receiver->type->without_reversed().is_list()) {
            return ::make_shared<lists::marker>(bv.bind_index, receiver);
        } else if (receiver->type->without_reversed().is_set()) {
            return ::make_shared<sets::marker>(bv.bind_index, receiver);
        } else if (receiver->type->without_reversed().is_map()) {
            return ::make_shared<maps::marker>(bv.bind_index, receiver);
        }
        assert(0);
    }

    if (receiver->type->is_user_type()) {
        return ::make_shared<user_types::marker>(bv.bind_index, receiver);
    }

    return ::make_shared<constants::marker>(bv.bind_index, receiver);
}

static
lw_shared_ptr<column_specification>
bind_variable_scalar_in_make_receiver(const column_specification& receiver) {
    auto in_name = ::make_shared<column_identifier>(sstring("in(") + receiver.name->to_string() + sstring(")"), true);
    return make_lw_shared<column_specification>(receiver.ks_name, receiver.cf_name, in_name, list_type_impl::get_instance(receiver.type, false));
}

static
::shared_ptr<term>
bind_variable_scalar_in_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) {
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    return ::make_shared<lists::marker>(bv.bind_index, bind_variable_scalar_in_make_receiver(*receiver));
}

static
lw_shared_ptr<column_specification>
bind_variable_tuple_make_receiver(const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    std::vector<data_type> types;
    types.reserve(receivers.size());
    sstring in_name = "(";
    for (auto&& receiver : receivers) {
        in_name += receiver->name->text();
        if (receiver != receivers.back()) {
            in_name += ",";
        }
        types.push_back(receiver->type);
    }
    in_name += ")";

    auto identifier = ::make_shared<column_identifier>(in_name, true);
    auto type = tuple_type_impl::get_instance(types);
    return make_lw_shared<column_specification>(receivers.front()->ks_name, receivers.front()->cf_name, identifier, type);
}

static
::shared_ptr<term>
bind_variable_tuple_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver) {
    auto& receivers = std::get<std::vector<lw_shared_ptr<column_specification>>>(receiver);
    return make_shared<tuples::marker>(bv.bind_index, bind_variable_tuple_make_receiver(receivers));
}

static
lw_shared_ptr<column_specification>
bind_variable_tuple_in_make_receiver(const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    std::vector<data_type> types;
    types.reserve(receivers.size());
    sstring in_name = "in(";
    for (auto&& receiver : receivers) {
        in_name += receiver->name->text();
        if (receiver != receivers.back()) {
            in_name += ",";
        }

        if (receiver->type->is_collection() && receiver->type->is_multi_cell()) {
            throw exceptions::invalid_request_exception("Non-frozen collection columns do not support IN relations");
        }

        types.emplace_back(receiver->type);
    }
    in_name += ")";

    auto identifier = ::make_shared<column_identifier>(in_name, true);
    auto type = tuple_type_impl::get_instance(types);
    return make_lw_shared<column_specification>(receivers.front()->ks_name, receivers.front()->cf_name, identifier, list_type_impl::get_instance(type, false));
}

static
::shared_ptr<term>
bind_variable_tuple_in_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver) {
    auto& receivers = std::get<std::vector<lw_shared_ptr<column_specification>>>(receiver);
    return make_shared<tuples::in_marker>(bv.bind_index, bind_variable_tuple_in_make_receiver(receivers));
}

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
        [&] (const bind_variable& bv) -> ::shared_ptr<term> {
            switch (bv.shape) {
            case expr::bind_variable::shape_type::scalar:  return bind_variable_scalar_prepare_term(bv, db, keyspace, receiver);
            case expr::bind_variable::shape_type::scalar_in: return bind_variable_scalar_in_prepare_term(bv, db, keyspace, receiver);
            case expr::bind_variable::shape_type::tuple: return bind_variable_tuple_prepare_term(bv, db, keyspace, receiver);
            case expr::bind_variable::shape_type::tuple_in: return bind_variable_tuple_in_prepare_term(bv, db, keyspace, receiver);
            }
            on_internal_error(expr_logger, "unexpected shape in bind_variable");
        },
        [&] (const untyped_constant& uc) -> ::shared_ptr<term> {
            return untyped_constant_prepare_term(uc, db, keyspace, receiver);
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
        [&] (const bind_variable& bv) -> test_result {
            // Same for all bind_variable::shape:s
            return bind_variable_test_assignment(bv, db, keyspace, receiver);
        },
        [&] (const untyped_constant& uc) -> test_result {
            return untyped_constant_test_assignment(uc, db, keyspace, receiver);
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