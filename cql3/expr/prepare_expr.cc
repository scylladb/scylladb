/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "expression.hh"
#include "cql3/functions/functions.hh"
#include "cql3/column_identifier.hh"
#include "cql3/constants.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/lists.hh"
#include "cql3/sets.hh"
#include "cql3/user_types.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/map.hh"
#include "types/user.hh"

#include <boost/range/algorithm/count.hpp>

namespace cql3::expr {

static
lw_shared_ptr<column_specification>
usertype_field_spec_of(const column_specification& column, size_t field) {
    auto&& ut = static_pointer_cast<const user_type_impl>(column.type);
    auto&& name = ut->field_name(field);
    auto&& sname = sstring(reinterpret_cast<const char*>(name.data()), name.size());
    return make_lw_shared<column_specification>(
                                   column.ks_name,
                                   column.cf_name,
                                   ::make_shared<column_identifier>(column.name->to_string() + "." + sname, true),
                                   ut->field_type(field));
}

static
void
usertype_constructor_validate_assignable_to(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    if (!receiver.type->is_user_type()) {
        throw exceptions::invalid_request_exception(format("Invalid user type literal for {} of type {}", receiver.name, receiver.type->as_cql3_type()));
    }

    auto ut = static_pointer_cast<const user_type_impl>(receiver.type);
    for (size_t i = 0; i < ut->size(); i++) {
        column_identifier field(to_bytes(ut->field_name(i)), utf8_type);
        if (!u.elements.contains(field)) {
            continue;
        }
        const expression& value = u.elements.at(field);
        auto&& field_spec = usertype_field_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(test_assignment(value, db, keyspace, *field_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid user type literal for {}: field {} is not of type {}", receiver.name, field, field_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
usertype_constructor_test_assignment(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    try {
        usertype_constructor_validate_assignable_to(u, db, keyspace, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
expression
usertype_constructor_prepare_expression(const usertype_constructor& u, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    usertype_constructor_validate_assignable_to(u, db, keyspace, *receiver);
    auto&& ut = static_pointer_cast<const user_type_impl>(receiver->type);
    bool all_terminal = true;

    usertype_constructor::elements_map_type prepared_elements;
    size_t found_values = 0;
    for (size_t i = 0; i < ut->size(); ++i) {
        auto&& field = column_identifier(to_bytes(ut->field_name(i)), utf8_type);
        auto iraw = u.elements.find(field);
        expression raw;
        if (iraw == u.elements.end()) {
            raw = expr::null();
        } else {
            raw = iraw->second;
            ++found_values;
        }
        expression value = prepare_expression(raw, db, keyspace, usertype_field_spec_of(*receiver, i));

        if (!is<constant>(value)) {
            all_terminal = false;
        }

        prepared_elements.emplace(std::move(field), std::move(value));
    }
    if (found_values != u.elements.size()) {
        // We had some field that are not part of the type
        for (auto&& id_val : u.elements) {
            auto&& id = id_val.first;
            if (!boost::range::count(ut->field_names(), id.bytes_)) {
                throw exceptions::invalid_request_exception(format("Unknown field '{}' in value of user defined type {}", id, ut->get_name_as_string()));
            }
        }
    }

    usertype_constructor value {
        .elements = std::move(prepared_elements),
        .type = ut
    };

    if (all_terminal) {
        return evaluate(value, query_options::DEFAULT);
    } else {
        return value;
    }
}

extern logging::logger expr_logger;

static
lw_shared_ptr<column_specification>
map_key_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(format("key({})", *column.name), true),
                dynamic_cast<const map_type_impl&>(column.type->without_reversed()).get_keys_type());
}

static
lw_shared_ptr<column_specification>
map_value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(format("value({})", *column.name), true),
                 dynamic_cast<const map_type_impl&>(column.type->without_reversed()).get_values_type());
}

static
void
map_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_map()) {
        throw exceptions::invalid_request_exception(format("Invalid map literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }
    auto&& key_spec = map_key_spec_of(receiver);
    auto&& value_spec = map_value_spec_of(receiver);
    for (auto&& entry : c.elements) {
        auto& entry_tuple = expr::as<tuple_constructor>(entry);
        if (entry_tuple.elements.size() != 2) {
            on_internal_error(expr_logger, "map element is not a tuple of arity 2");
        }
        if (!is_assignable(test_assignment(entry_tuple.elements[0], db, keyspace, *key_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: key {} is not of type {}", *receiver.name, entry_tuple.elements[0], key_spec->type->as_cql3_type()));
        }
        if (!is_assignable(test_assignment(entry_tuple.elements[1], db, keyspace, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: value {} is not of type {}", *receiver.name, entry_tuple.elements[1], value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
map_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    if (!dynamic_pointer_cast<const map_type_impl>(receiver.type)) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
    // If there is no elements, we can't say it's an exact match (an empty map if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    auto key_spec = maps::key_spec_of(receiver);
    auto value_spec = maps::value_spec_of(receiver);
    // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
    auto res = assignment_testable::test_result::EXACT_MATCH;
    for (auto entry : c.elements) {
        auto& entry_tuple = expr::as<tuple_constructor>(entry);
        if (entry_tuple.elements.size() != 2) {
            on_internal_error(expr_logger, "map element is not a tuple of arity 2");
        }
        auto t1 = test_assignment(entry_tuple.elements[0], db, keyspace, *key_spec);
        auto t2 = test_assignment(entry_tuple.elements[1], db, keyspace, *value_spec);
        if (t1 == assignment_testable::test_result::NOT_ASSIGNABLE || t2 == assignment_testable::test_result::NOT_ASSIGNABLE)
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        if (t1 != assignment_testable::test_result::EXACT_MATCH || t2 != assignment_testable::test_result::EXACT_MATCH)
            res = assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    return res;
}

static
expression
map_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    map_validate_assignable_to(c, db, keyspace, *receiver);

    auto key_spec = maps::key_spec_of(*receiver);
    auto value_spec = maps::value_spec_of(*receiver);
    std::vector<expression> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto&& entry : c.elements) {
        auto& entry_tuple = expr::as<tuple_constructor>(entry);
        if (entry_tuple.elements.size() != 2) {
            on_internal_error(expr_logger, "map element is not a tuple of arity 2");
        }
        expression k = prepare_expression(entry_tuple.elements[0], db, keyspace, key_spec);
        expression v = prepare_expression(entry_tuple.elements[1], db, keyspace, value_spec);

        // Check if one of values contains a nonpure function
        if (!is<constant>(k) || !is<constant>(v)) {
            all_terminal = false;
        }

        values.emplace_back(tuple_constructor {
            .elements = {std::move(k), std::move(v)},
            .type = entry_tuple.type
        });
    }

    collection_constructor map_value {
        .style = collection_constructor::style_type::map,
        .elements = std::move(values),
        .type = receiver->type
    };
    if (all_terminal) {
        return evaluate(map_value, query_options::DEFAULT);
    } else {
        return map_value;
    }
}

static
lw_shared_ptr<column_specification>
set_value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("value({})", *column.name), true),
            dynamic_cast<const set_type_impl&>(column.type->without_reversed()).get_elements_type());
}

static
void
set_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_set()) {
        // We've parsed empty maps as a set literal to break the ambiguity so
        // handle that case now
        if (dynamic_pointer_cast<const map_type_impl>(receiver.type) && c.elements.empty()) {
            return;
        }

        throw exceptions::invalid_request_exception(format("Invalid set literal for {} of type {}", receiver.name, receiver.type->as_cql3_type()));
    }

    auto&& value_spec = set_value_spec_of(receiver);
    for (auto& e: c.elements) {
        if (!is_assignable(test_assignment(e, db, keyspace, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid set literal for {}: value {} is not of type {}", *receiver.name, e, value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
set_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_set()) {
        // We've parsed empty maps as a set literal to break the ambiguity so handle that case now
        if (dynamic_pointer_cast<const map_type_impl>(receiver.type) && c.elements.empty()) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }

        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty set if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = set_value_spec_of(receiver);
    return test_assignment_all(c.elements, db, keyspace, *value_spec);
}

static
expression
set_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    set_validate_assignable_to(c, db, keyspace, *receiver);

    if (c.elements.empty()) {

        // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
        // other words a non-frozen collection only exists if it has elements.  Return nullptr right
        // away to simplify predicate evaluation.  See also
        // https://issues.apache.org/jira/browse/CASSANDRA-5141
        if (receiver->type->is_multi_cell()) {
            return constant::make_null(receiver->type);
        }
        // We've parsed empty maps as a set literal to break the ambiguity so
        // handle that case now. This branch works for frozen sets/maps only.
        const map_type_impl* maybe_map_type = dynamic_cast<const map_type_impl*>(receiver->type.get());
        if (maybe_map_type != nullptr) {
            collection_constructor map_value {
                .style = collection_constructor::style_type::map,
                .elements = {},
                .type = receiver->type
            };
            return expr::evaluate(map_value, query_options::DEFAULT);
        }
    }

    auto value_spec = set_value_spec_of(*receiver);
    std::vector<expression> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto& e : c.elements)
    {
        expression elem = prepare_expression(e, db, keyspace, value_spec);

        if (!is<constant>(elem)) {
            all_terminal = false;
        }

        values.push_back(std::move(elem));
    }

    collection_constructor value {
        .style = collection_constructor::style_type::set,
        .elements = std::move(values),
        .type = receiver->type
    };
    
    if (all_terminal) {
        return evaluate(value, query_options::DEFAULT);
    } else {
        return value;
    }
}

static
lw_shared_ptr<column_specification>
list_value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("value({})", *column.name), true),
                dynamic_cast<const list_type_impl&>(column.type->without_reversed()).get_elements_type());
}

static
void
list_validate_assignable_to(const collection_constructor& c, data_dictionary::database db, const sstring keyspace, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_list()) {
        throw exceptions::invalid_request_exception(format("Invalid list literal for {} of type {}",
                *receiver.name, receiver.type->as_cql3_type()));
    }
    auto&& value_spec = list_value_spec_of(receiver);
    for (auto& e : c.elements) {
        if (!is_assignable(test_assignment(e, db, keyspace, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid list literal for {}: value {} is not of type {}",
                    *receiver.name, e, value_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
list_test_assignment(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    if (!dynamic_pointer_cast<const list_type_impl>(receiver.type)) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty list if fundamentally polymorphic).
    if (c.elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = list_value_spec_of(receiver);
    return test_assignment_all(c.elements, db, keyspace, *value_spec);
}


static
expression
list_prepare_expression(const collection_constructor& c, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    list_validate_assignable_to(c, db, keyspace, *receiver);

    // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
    // other words a non-frozen collection only exists if it has elements. Return nullptr right
    // away to simplify predicate evaluation. See also
    // https://issues.apache.org/jira/browse/CASSANDRA-5141
    if (receiver->type->is_multi_cell() &&  c.elements.empty()) {
        return constant::make_null(receiver->type);
    }

    auto&& value_spec = list_value_spec_of(*receiver);
    std::vector<expression> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto& e : c.elements) {
        expression elem = prepare_expression(e, db, keyspace, value_spec);

        if (!is<constant>(elem)) {
            all_terminal = false;
        }
        values.push_back(std::move(elem));
    }
    collection_constructor value {
        .style = collection_constructor::style_type::list,
        .elements = std::move(values),
        .type = receiver->type
    };
    if (all_terminal) {
        return evaluate(value, query_options::DEFAULT);
    } else {
        return value;
    }
}

static
lw_shared_ptr<column_specification>
component_spec_of(const column_specification& column, size_t component) {
    return make_lw_shared<column_specification>(
            column.ks_name,
            column.cf_name,
            ::make_shared<column_identifier>(format("{}[{:d}]", column.name, component), true),
            static_pointer_cast<const tuple_type_impl>(column.type->underlying_type())->type(component));
}

static
void
tuple_constructor_validate_assignable_to(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    auto tt = dynamic_pointer_cast<const tuple_type_impl>(receiver.type->underlying_type());
    if (!tt) {
        throw exceptions::invalid_request_exception(format("Invalid tuple type literal for {} of type {}", receiver.name, receiver.type->as_cql3_type()));
    }
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        if (i >= tt->size()) {
            throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: too many elements. Type {} expects {:d} but got {:d}",
                                                            receiver.name, tt->as_cql3_type(), tt->size(), tc.elements.size()));
        }

        auto&& value = tc.elements[i];
        auto&& spec = component_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(test_assignment(value, db, keyspace, *spec))) {
            throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: component {:d} is not of type {}", receiver.name, i, spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
tuple_constructor_test_assignment(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    try {
        tuple_constructor_validate_assignable_to(tc, db, keyspace, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
expression
tuple_constructor_prepare_nontuple(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    tuple_constructor_validate_assignable_to(tc, db, keyspace, *receiver);
    std::vector<expression> values;
    bool all_terminal = true;
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        expression value = prepare_expression(tc.elements[i], db, keyspace, component_spec_of(*receiver, i));
        if (!is<constant>(value)) {
            all_terminal = false;
        }
        values.push_back(std::move(value));
    }
    tuple_constructor value {
        .elements  = std::move(values),
        .type = static_pointer_cast<const tuple_type_impl>(receiver->type)
    };
    if (all_terminal) {
        return evaluate(value, query_options::DEFAULT);
    } else {
        return value;
    }
}

static
expression
tuple_constructor_prepare_tuple(const tuple_constructor& tc, data_dictionary::database db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    if (tc.elements.size() != receivers.size()) {
        throw exceptions::invalid_request_exception(format("Expected {:d} elements in value tuple, but got {:d}: {}", receivers.size(), tc.elements.size(), tc));
    }

    std::vector<expression> values;
    std::vector<data_type> types;
    bool all_terminal = true;
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        expression elem = prepare_expression(tc.elements[i], db, keyspace, receivers[i]);
        if (!is<constant>(elem)) {
            all_terminal = false;
        }
        values.push_back(std::move(elem));
        types.push_back(receivers[i]->type);
    }
    tuple_constructor value {
        .elements = std::move(values),
        .type = tuple_type_impl::get_instance(std::move(types))
    };
    if (all_terminal) {
        return evaluate(value, query_options::DEFAULT);
    } else {
        return value;
    }
}

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
untyped_constant_test_assignment(const untyped_constant& uc, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver)
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
constant
untyped_constant_prepare_expression(const untyped_constant& uc, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver)
{
    if (!is_assignable(untyped_constant_test_assignment(uc, db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception(format("Invalid {} constant ({}) for \"{}\" of type {}",
            uc.partial_type, uc.raw_text, *receiver->name, receiver->type->as_cql3_type().to_string()));
    }
    raw_value raw_val = cql3::raw_value::make_value(untyped_constant_parsed_value(uc, receiver->type));
    return constant(std::move(raw_val), receiver->type);
}

static
assignment_testable::test_result
bind_variable_test_assignment(const bind_variable& bv, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
}

static
bind_variable
bind_variable_scalar_prepare_expression(const bind_variable& bv, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver)
{   
    return bind_variable {
        .shape = bind_variable::shape_type::scalar,
        .bind_index = bv.bind_index,
        .receiver = receiver
    };
}

static
lw_shared_ptr<column_specification>
bind_variable_scalar_in_make_receiver(const column_specification& receiver) {
    auto in_name = ::make_shared<column_identifier>(sstring("in(") + receiver.name->to_string() + sstring(")"), true);
    return make_lw_shared<column_specification>(receiver.ks_name, receiver.cf_name, in_name, list_type_impl::get_instance(receiver.type, false));
}

static
bind_variable
bind_variable_scalar_in_prepare_expression(const bind_variable& bv, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    return bind_variable {
        .shape = bind_variable::shape_type::scalar,
        .bind_index = bv.bind_index,
        .receiver = bind_variable_scalar_in_make_receiver(*receiver)
    };
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
bind_variable
bind_variable_tuple_prepare_expression(const bind_variable& bv, data_dictionary::database db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    return bind_variable {
        .shape = bind_variable::shape_type::tuple,
        .bind_index = bv.bind_index,
        .receiver = bind_variable_tuple_make_receiver(receivers)
    };
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
bind_variable
bind_variable_tuple_in_prepare_expression(const bind_variable& bv, data_dictionary::database db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    return bind_variable {
        .shape = bind_variable::shape_type::tuple_in,
        .bind_index = bv.bind_index,
        .receiver = bind_variable_tuple_in_make_receiver(receivers)
    };
}

static
assignment_testable::test_result
null_test_assignment(data_dictionary::database db,
        const sstring& keyspace,
        const column_specification& receiver) {
    return receiver.type->is_counter()
        ? assignment_testable::test_result::NOT_ASSIGNABLE
        : assignment_testable::test_result::WEAKLY_ASSIGNABLE;
}

static
constant
null_prepare_expression(data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    if (!is_assignable(null_test_assignment(db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception("Invalid null value for counter increment/decrement");
    }
    return constant::make_null(receiver->type);
}

static
sstring
cast_display_name(const cast& c) {
    return format("({}){}", std::get<shared_ptr<cql3_type::raw>>(c.type), c.arg);
}

static
lw_shared_ptr<column_specification>
casted_spec_of(const cast& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    auto& type = std::get<shared_ptr<cql3_type::raw>>(c.type);
    return make_lw_shared<column_specification>(receiver.ks_name, receiver.cf_name,
            ::make_shared<column_identifier>(cast_display_name(c), true), type->prepare(db, keyspace).get_type());
}

static
assignment_testable::test_result
cast_test_assignment(const cast& c, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    auto type = std::get<shared_ptr<cql3_type::raw>>(c.type);
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
expression
cast_prepare_expression(const cast& c, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    auto type = std::get<shared_ptr<cql3_type::raw>>(c.type);
    if (!is_assignable(test_assignment(c.arg, db, keyspace, *casted_spec_of(c, db, keyspace, *receiver)))) {
        throw exceptions::invalid_request_exception(format("Cannot cast value {} to type {}", c.arg, type));
    }
    if (!is_assignable(cast_test_assignment(c, db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception(format("Cannot assign value {} to {} of type {}", c, receiver->name, receiver->type->as_cql3_type()));
    }
    return prepare_expression(c.arg, db, keyspace, receiver);
}

expr::expression
prepare_function_call(const expr::function_call& fc, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    auto&& fun = std::visit(overloaded_functor{
        [] (const shared_ptr<functions::function>& func) {
            return func;
        },
        [&] (const functions::function_name& name) {
            auto args = boost::copy_range<std::vector<::shared_ptr<assignment_testable>>>(fc.args | boost::adaptors::transformed(expr::as_assignment_testable));
            auto fun = functions::functions::get(db, keyspace, name, args, receiver->ks_name, receiver->cf_name, receiver.get());
            if (!fun) {
                throw exceptions::invalid_request_exception(format("Unknown function {} called", name));
            }
            return fun;
        },
    }, fc.func);
    if (fun->is_aggregate()) {
        throw exceptions::invalid_request_exception("Aggregation function are not supported in the where clause");
    }

    // Can't use static_pointer_cast<> because function is a virtual base class of scalar_function
    auto&& scalar_fun = dynamic_pointer_cast<functions::scalar_function>(fun);

    // Functions.get() will complain if no function "name" type check with the provided arguments.
    // We still have to validate that the return type matches however
    if (!receiver->type->is_value_compatible_with(*scalar_fun->return_type())) {
        throw exceptions::invalid_request_exception(format("Type error: cannot assign result of function {} (type {}) to {} (type {})",
                                                    fun->name(), fun->return_type()->as_cql3_type(),
                                                    receiver->name, receiver->type->as_cql3_type()));
    }

    if (scalar_fun->arg_types().size() != fc.args.size()) {
        throw exceptions::invalid_request_exception(format("Incorrect number of arguments specified for function {} (expected {:d}, found {:d})",
                                                    fun->name(), fun->arg_types().size(), fc.args.size()));
    }

    std::vector<expr::expression> parameters;
    parameters.reserve(fc.args.size());
    bool all_terminal = true;
    for (size_t i = 0; i < fc.args.size(); ++i) {
        expr::expression e = prepare_expression(fc.args[i], db, keyspace,
                                                functions::functions::make_arg_spec(receiver->ks_name, receiver->cf_name, *scalar_fun, i));
        if (!expr::is<expr::constant>(e)) {
            all_terminal = false;
        }
        parameters.push_back(std::move(e));
    }

    // If all parameters are terminal and the function is pure, we can
    // evaluate it now, otherwise we'd have to wait execution time
    expr::function_call fun_call {
        .func = fun,
        .args = std::move(parameters),
        .lwt_cache_id = fc.lwt_cache_id
    };
    if (all_terminal && scalar_fun->is_pure()) {
        return expr::evaluate(fun_call, query_options::DEFAULT);
    } else {
        return fun_call;
    }
}

assignment_testable::test_result
test_assignment_function_call(const cql3::expr::function_call& fc, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
    // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
    // of another, existing, function. In that case, we return true here because we'll throw a proper exception
    // later with a more helpful error message that if we were to return false here.
    try {
        auto&& fun = std::visit(overloaded_functor{
            [&] (const functions::function_name& name) {
                auto args = boost::copy_range<std::vector<::shared_ptr<assignment_testable>>>(fc.args | boost::adaptors::transformed(expr::as_assignment_testable));
                return functions::functions::get(db, keyspace, name, args, receiver.ks_name, receiver.cf_name, &receiver);
            },
            [] (const shared_ptr<functions::function>& func) {
                return func;
            },
        }, fc.func);
        if (fun && receiver.type == fun->return_type()) {
            return assignment_testable::test_result::EXACT_MATCH;
        } else if (!fun || receiver.type->is_value_compatible_with(*fun->return_type())) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        } else {
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        }
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
}

expression
prepare_expression(const expression& expr, data_dictionary::database db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    return expr::visit(overloaded_functor{
        [] (const constant&) -> expression {
            on_internal_error(expr_logger, "Can't prepare constant_value, it should not appear in parser output");
        },
        [&] (const binary_operator&) -> expression {
            on_internal_error(expr_logger, "binary_operators are not yet reachable via prepare_expression()");
        },
        [&] (const conjunction&) -> expression {
            on_internal_error(expr_logger, "conjunctions are not yet reachable via prepare_expression()");
        },
        [&] (const column_value&) -> expression {
            on_internal_error(expr_logger, "column_values are not yet reachable via prepare_expression()");
        },
        [&] (const subscript&) -> expression {
            on_internal_error(expr_logger, "subscripts are not yet reachable via prepare_expression()");
        },
        [&] (const token&) -> expression {
            on_internal_error(expr_logger, "tokens are not yet reachable via prepare_expression()");
        },
        [&] (const unresolved_identifier&) -> expression {
            on_internal_error(expr_logger, "unresolved_identifiers are not yet reachable via prepare_expression()");
        },
        [&] (const column_mutation_attribute&) -> expression {
            on_internal_error(expr_logger, "column_mutation_attributes are not yet reachable via prepare_expression()");
        },
        [&] (const function_call& fc) -> expression {
            return prepare_function_call(fc, db, keyspace, std::move(receiver));
        },
        [&] (const cast& c) -> expression {
            return cast_prepare_expression(c, db, keyspace, receiver);
        },
        [&] (const field_selection&) -> expression {
            on_internal_error(expr_logger, "field_selections are not yet reachable via prepare_expression()");
        },
        [&] (const null&) -> expression {
            return null_prepare_expression(db, keyspace, receiver);
        },
        [&] (const bind_variable& bv) -> expression {
            switch (bv.shape) {
            case expr::bind_variable::shape_type::scalar:  return bind_variable_scalar_prepare_expression(bv, db, keyspace, receiver);
            case expr::bind_variable::shape_type::scalar_in: return bind_variable_scalar_in_prepare_expression(bv, db, keyspace, receiver);
            case expr::bind_variable::shape_type::tuple: on_internal_error(expr_logger, "prepare_expression(bind_variable(tuple))");
            case expr::bind_variable::shape_type::tuple_in: on_internal_error(expr_logger, "prepare_expression(bind_variable(tuple_in))");
            }
            on_internal_error(expr_logger, "unexpected shape in bind_variable");
        },
        [&] (const untyped_constant& uc) -> expression {
            return untyped_constant_prepare_expression(uc, db, keyspace, receiver);
        },
        [&] (const tuple_constructor& tc) -> expression {
            return tuple_constructor_prepare_nontuple(tc, db, keyspace, receiver);
        },
        [&] (const collection_constructor& c) -> expression {
            switch (c.style) {
            case collection_constructor::style_type::list: return list_prepare_expression(c, db, keyspace, receiver);
            case collection_constructor::style_type::set: return set_prepare_expression(c, db, keyspace, receiver);
            case collection_constructor::style_type::map: return map_prepare_expression(c, db, keyspace, receiver);
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> expression {
            return usertype_constructor_prepare_expression(uc, db, keyspace, receiver);
        },
    }, expr);
}

expression
prepare_expression_multi_column(const expression& expr, data_dictionary::database db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    return expr::visit(overloaded_functor{
        [&] (const bind_variable& bv) -> expression {
            switch (bv.shape) {
            case expr::bind_variable::shape_type::scalar: on_internal_error(expr_logger, "prepare_expression_multi_column(bind_variable(scalar))");
            case expr::bind_variable::shape_type::scalar_in: on_internal_error(expr_logger, "prepare_expression_multi_column(bind_variable(scalar_in))");
            case expr::bind_variable::shape_type::tuple: return bind_variable_tuple_prepare_expression(bv, db, keyspace, receivers);
            case expr::bind_variable::shape_type::tuple_in: return bind_variable_tuple_in_prepare_expression(bv, db, keyspace, receivers);
            }
            on_internal_error(expr_logger, "unexpected shape in bind_variable");
        },
        [&] (const tuple_constructor& tc) -> expression {
            return tuple_constructor_prepare_tuple(tc, db, keyspace, receivers);
        },
        [] (const auto& default_case) -> expression {
            on_internal_error(expr_logger, fmt::format("prepare_expression_multi_column({})", typeid(default_case).name()));
        },
    }, expr);
}

assignment_testable::test_result
test_assignment(const expression& expr, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    using test_result = assignment_testable::test_result;
    return expr::visit(overloaded_functor{
        [&] (const constant&) -> test_result {
            // constants shouldn't appear in parser output, only untyped_constants
            on_internal_error(expr_logger, "constants are not yet reachable via test_assignment()");
        },
        [&] (const binary_operator&) -> test_result {
            on_internal_error(expr_logger, "binary_operators are not yet reachable via test_assignment()");
        },
        [&] (const conjunction&) -> test_result {
            on_internal_error(expr_logger, "conjunctions are not yet reachable via test_assignment()");
        },
        [&] (const column_value&) -> test_result {
            on_internal_error(expr_logger, "column_values are not yet reachable via test_assignment()");
        },
        [&] (const subscript&) -> test_result {
            on_internal_error(expr_logger, "subscripts are not yet reachable via test_assignment()");
        },
        [&] (const token&) -> test_result {
            on_internal_error(expr_logger, "tokens are not yet reachable via test_assignment()");
        },
        [&] (const unresolved_identifier&) -> test_result {
            on_internal_error(expr_logger, "unresolved_identifiers are not yet reachable via test_assignment()");
        },
        [&] (const column_mutation_attribute&) -> test_result {
            on_internal_error(expr_logger, "column_mutation_attributes are not yet reachable via test_assignment()");
        },
        [&] (const function_call& fc) -> test_result {
            return test_assignment_function_call(fc, db, keyspace, receiver);
        },
        [&] (const cast& c) -> test_result {
            return cast_test_assignment(c, db, keyspace, receiver);
        },
        [&] (const field_selection&) -> test_result {
            on_internal_error(expr_logger, "field_selections are not yet reachable via test_assignment()");
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
        [&] (const tuple_constructor& tc) -> test_result {
            return tuple_constructor_test_assignment(tc, db, keyspace, receiver);
        },
        [&] (const collection_constructor& c) -> test_result {
            switch (c.style) {
            case collection_constructor::style_type::list: return list_test_assignment(c, db, keyspace, receiver);
            case collection_constructor::style_type::set: return set_test_assignment(c, db, keyspace, receiver);
            case collection_constructor::style_type::map: return map_test_assignment(c, db, keyspace, receiver);
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> test_result {
            return usertype_constructor_test_assignment(uc, db, keyspace, receiver);
        },
    }, expr);
}

assignment_testable::test_result
test_assignment_all(const std::vector<expression>& to_test, data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) {
    using test_result = assignment_testable::test_result;
    test_result res = test_result::EXACT_MATCH;
    for (auto&& e : to_test) {
        test_result t = test_assignment(e, db, keyspace, receiver);
        if (t == test_result::NOT_ASSIGNABLE) {
            return test_result::NOT_ASSIGNABLE;
        }
        if (t == test_result::WEAKLY_ASSIGNABLE) {
            res = test_result::WEAKLY_ASSIGNABLE;
        }
    }
    return res;
}

class assignment_testable_expression : public assignment_testable {
    expression _e;
public:
    explicit assignment_testable_expression(expression e) : _e(std::move(e)) {}
    virtual test_result test_assignment(data_dictionary::database db, const sstring& keyspace, const column_specification& receiver) const override {
        return expr::test_assignment(_e, db, keyspace, receiver);
    }
    virtual sstring assignment_testable_source_context() const override {
        return fmt::format("{}", _e);
    }

};

::shared_ptr<assignment_testable> as_assignment_testable(expression e) {
    return ::make_shared<assignment_testable_expression>(std::move(e));
}

}

namespace cql3 {

lw_shared_ptr<column_specification>
lists::value_spec_of(const column_specification& column) {
    return cql3::expr::list_value_spec_of(column);
}

lw_shared_ptr<column_specification>
maps::key_spec_of(const column_specification& column) {
    return cql3::expr::map_key_spec_of(column);
}

lw_shared_ptr<column_specification>
maps::value_spec_of(const column_specification& column) {
    return cql3::expr::map_value_spec_of(column);
}

lw_shared_ptr<column_specification>
sets::value_spec_of(const column_specification& column) {
    return cql3::expr::set_value_spec_of(column);
}

lw_shared_ptr<column_specification>
user_types::field_spec_of(const column_specification& column, size_t field) {
    return cql3::expr::usertype_field_spec_of(column, field);
}

}
