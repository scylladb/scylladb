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

#include "expression.hh"
#include "cql3/functions/function_call.hh"
#include "cql3/column_identifier.hh"
#include "cql3/constants.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/lists.hh"
#include "cql3/sets.hh"
#include "cql3/user_types.hh"
#include "cql3/tuples.hh"
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
usertype_constructor_validate_assignable_to(const usertype_constructor& u, database& db, const sstring& keyspace, const column_specification& receiver) {
    if (!receiver.type->is_user_type()) {
        throw exceptions::invalid_request_exception(format("Invalid user type literal for {} of type {}", receiver.name, receiver.type->as_cql3_type()));
    }

    auto ut = static_pointer_cast<const user_type_impl>(receiver.type);
    for (size_t i = 0; i < ut->size(); i++) {
        column_identifier field(to_bytes(ut->field_name(i)), utf8_type);
        if (!u.elements.contains(field)) {
            continue;
        }
        const expression& value = *u.elements.at(field);
        auto&& field_spec = usertype_field_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(test_assignment(value, db, keyspace, *field_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid user type literal for {}: field {} is not of type {}", receiver.name, field, field_spec->type->as_cql3_type()));
        }
    }
}

static
assignment_testable::test_result
usertype_constructor_test_assignment(const usertype_constructor& u, database& db, const sstring& keyspace, const column_specification& receiver) {
    try {
        usertype_constructor_validate_assignable_to(u, db, keyspace, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
shared_ptr<term>
usertype_constructor_prepare_term(const usertype_constructor& u, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    usertype_constructor_validate_assignable_to(u, db, keyspace, *receiver);
    auto&& ut = static_pointer_cast<const user_type_impl>(receiver->type);
    bool all_terminal = true;
    std::vector<shared_ptr<term>> values;
    values.reserve(u.elements.size());
    size_t found_values = 0;
    for (size_t i = 0; i < ut->size(); ++i) {
        auto&& field = column_identifier(to_bytes(ut->field_name(i)), utf8_type);
        auto iraw = u.elements.find(field);
        expression raw;
        if (iraw == u.elements.end()) {
            raw = expr::null();
        } else {
            raw = *iraw->second;
            ++found_values;
        }
        auto&& value = prepare_term(raw, db, keyspace, usertype_field_spec_of(*receiver, i));

        if (dynamic_cast<non_terminal*>(value.get())) {
            all_terminal = false;
        }

        values.push_back(std::move(value));
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

    user_types::delayed_value value(ut, values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<user_types::delayed_value>(std::move(value));
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
map_validate_assignable_to(const collection_constructor& c, database& db, const sstring& keyspace, const column_specification& receiver) {
    if (!receiver.type->without_reversed().is_map()) {
        throw exceptions::invalid_request_exception(format("Invalid map literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }
    auto&& key_spec = map_key_spec_of(receiver);
    auto&& value_spec = map_value_spec_of(receiver);
    for (auto&& entry : c.elements) {
        auto& entry_tuple = std::get<tuple_constructor>(entry);
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
map_test_assignment(const collection_constructor& c, database& db, const sstring& keyspace, const column_specification& receiver) {
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
        auto& entry_tuple = std::get<tuple_constructor>(entry);
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
::shared_ptr<term>
map_prepare_term(const collection_constructor& c, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    map_validate_assignable_to(c, db, keyspace, *receiver);

    auto key_spec = maps::key_spec_of(*receiver);
    auto value_spec = maps::value_spec_of(*receiver);
    std::unordered_map<shared_ptr<term>, shared_ptr<term>> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto&& entry : c.elements) {
        auto& entry_tuple = std::get<tuple_constructor>(entry);
        if (entry_tuple.elements.size() != 2) {
            on_internal_error(expr_logger, "map element is not a tuple of arity 2");
        }
        auto k = prepare_term(entry_tuple.elements[0], db, keyspace, key_spec);
        auto v = prepare_term(entry_tuple.elements[1], db, keyspace, value_spec);

        if (k->contains_bind_marker() || v->contains_bind_marker()) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: bind variables are not supported inside collection literals", *receiver->name));
        }

        if (dynamic_pointer_cast<non_terminal>(k) || dynamic_pointer_cast<non_terminal>(v)) {
            all_terminal = false;
        }

        values.emplace(k, v);
    }
    maps::delayed_value value(
            dynamic_cast<const map_type_impl&>(receiver->type->without_reversed()).get_keys_type()->as_less_comparator(),
            values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<maps::delayed_value>(std::move(value));
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
set_validate_assignable_to(const collection_constructor& c, database& db, const sstring& keyspace, const column_specification& receiver) {
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
set_test_assignment(const collection_constructor& c, database& db, const sstring& keyspace, const column_specification& receiver) {
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
shared_ptr<term>
set_prepare_term(const collection_constructor& c, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    set_validate_assignable_to(c, db, keyspace, *receiver);

    if (c.elements.empty()) {

        // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
        // other words a non-frozen collection only exists if it has elements.  Return nullptr right
        // away to simplify predicate evaluation.  See also
        // https://issues.apache.org/jira/browse/CASSANDRA-5141
        if (receiver->type->is_multi_cell()) {
            return cql3::constants::NULL_VALUE;
        }
        // We've parsed empty maps as a set literal to break the ambiguity so
        // handle that case now. This branch works for frozen sets/maps only.
        if (dynamic_pointer_cast<const map_type_impl>(receiver->type)) {
            // use empty_type for comparator, set is empty anyway.
            std::map<managed_bytes, managed_bytes, serialized_compare> m(empty_type->as_less_comparator());
            return ::make_shared<maps::value>(std::move(m));
        }
    }

    auto value_spec = set_value_spec_of(*receiver);
    std::vector<shared_ptr<term>> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto& e : c.elements)
    {
        auto t = prepare_term(e, db, keyspace, value_spec);

        if (t->contains_bind_marker()) {
            throw exceptions::invalid_request_exception(format("Invalid set literal for {}: bind variables are not supported inside collection literals", *receiver->name));
        }

        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }

        values.push_back(std::move(t));
    }
    auto compare = dynamic_cast<const set_type_impl&>(receiver->type->without_reversed())
            .get_elements_type()->as_less_comparator();

    auto value = ::make_shared<sets::delayed_value>(compare, std::move(values));
    if (all_terminal) {
        return value->bind(query_options::DEFAULT);
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
list_validate_assignable_to(const collection_constructor& c, database& db, const sstring keyspace, const column_specification& receiver) {
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
list_test_assignment(const collection_constructor& c, database& db, const sstring& keyspace, const column_specification& receiver) {
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
shared_ptr<term>
list_prepare_term(const collection_constructor& c, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    list_validate_assignable_to(c, db, keyspace, *receiver);

    // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
    // other words a non-frozen collection only exists if it has elements. Return nullptr right
    // away to simplify predicate evaluation. See also
    // https://issues.apache.org/jira/browse/CASSANDRA-5141
    if (receiver->type->is_multi_cell() &&  c.elements.empty()) {
        return cql3::constants::NULL_VALUE;
    }

    auto&& value_spec = list_value_spec_of(*receiver);
    std::vector<shared_ptr<term>> values;
    values.reserve(c.elements.size());
    bool all_terminal = true;
    for (auto& e : c.elements) {
        auto&& t = prepare_term(e, db, keyspace, value_spec);

        if (t->contains_bind_marker()) {
            throw exceptions::invalid_request_exception(format("Invalid list literal for {}: bind variables are not supported inside collection literals", *receiver->name));
        }
        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }
        values.push_back(std::move(t));
    }
    lists::delayed_value value(values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<lists::delayed_value>(std::move(value));
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
tuple_constructor_validate_assignable_to(const tuple_constructor& tc, database& db, const sstring& keyspace, const column_specification& receiver) {
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
tuple_constructor_test_assignment(const tuple_constructor& tc, database& db, const sstring& keyspace, const column_specification& receiver) {
    try {
        tuple_constructor_validate_assignable_to(tc, db, keyspace, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

static
shared_ptr<term>
tuple_constructor_prepare_nontuple(const tuple_constructor& tc, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    tuple_constructor_validate_assignable_to(tc, db, keyspace, *receiver);
    std::vector<shared_ptr<term>> values;
    bool all_terminal = true;
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        auto&& value = prepare_term(tc.elements[i], db, keyspace, component_spec_of(*receiver, i));
        if (dynamic_pointer_cast<non_terminal>(value)) {
            all_terminal = false;
        }
        values.push_back(std::move(value));
    }
    tuples::delayed_value value(static_pointer_cast<const tuple_type_impl>(receiver->type), values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<tuples::delayed_value>(std::move(value));
    }
}

static
shared_ptr<term>
tuple_constructor_prepare_tuple(const tuple_constructor& tc, database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    if (tc.elements.size() != receivers.size()) {
        throw exceptions::invalid_request_exception(format("Expected {:d} elements in value tuple, but got {:d}: {}", receivers.size(), tc.elements.size(), tc));
    }

    std::vector<shared_ptr<term>> values;
    std::vector<data_type> types;
    bool all_terminal = true;
    for (size_t i = 0; i < tc.elements.size(); ++i) {
        auto&& t = prepare_term(tc.elements[i], db, keyspace, receivers[i]);
        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }
        values.push_back(t);
        types.push_back(receivers[i]->type);
    }
    tuples::delayed_value value(tuple_type_impl::get_instance(std::move(types)), std::move(values));
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<tuples::delayed_value>(std::move(value));
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
untyped_constant_prepare_term(const untyped_constant& uc, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver)
{
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
bind_variable_scalar_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver)
{
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
bind_variable_scalar_in_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
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
bind_variable_tuple_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
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
bind_variable_tuple_in_prepare_term(const bind_variable& bv, database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
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
null_prepare_term(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    if (!is_assignable(null_test_assignment(db, keyspace, *receiver))) {
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
cast_prepare_term(const cast& c, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    auto type = std::get<shared_ptr<cql3_type::raw>>(c.type);
    if (!is_assignable(test_assignment(*c.arg, db, keyspace, *casted_spec_of(c, db, keyspace, *receiver)))) {
        throw exceptions::invalid_request_exception(format("Cannot cast value {} to type {}", *c.arg, type));
    }
    if (!is_assignable(cast_test_assignment(c, db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception(format("Cannot assign value {} to {} of type {}", c, receiver->name, receiver->type->as_cql3_type()));
    }
    return prepare_term(*c.arg, db, keyspace, receiver);
}

::shared_ptr<term>
prepare_term(const expression& expr, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) {
    return std::visit(overloaded_functor{
        [] (const constant&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "Can't prepare constant_value, it should not appear in parser output");
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
            return functions::prepare_function_call(fc, db, keyspace, std::move(receiver));
        },
        [&] (const cast& c) -> ::shared_ptr<term> {
            return cast_prepare_term(c, db, keyspace, receiver);
        },
        [&] (const field_selection&) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, "field_selections are not yet reachable via term_raw_expr::prepare()");
        },
        [&] (const null&) -> ::shared_ptr<term> {
            return null_prepare_term(db, keyspace, receiver);
        },
        [&] (const bind_variable& bv) -> ::shared_ptr<term> {
            switch (bv.shape) {
            case expr::bind_variable::shape_type::scalar:  return bind_variable_scalar_prepare_term(bv, db, keyspace, receiver);
            case expr::bind_variable::shape_type::scalar_in: return bind_variable_scalar_in_prepare_term(bv, db, keyspace, receiver);
            case expr::bind_variable::shape_type::tuple: on_internal_error(expr_logger, "prepare_term(bind_variable(tuple))");
            case expr::bind_variable::shape_type::tuple_in: on_internal_error(expr_logger, "prepare_term(bind_variable(tuple_in))");
            }
            on_internal_error(expr_logger, "unexpected shape in bind_variable");
        },
        [&] (const untyped_constant& uc) -> ::shared_ptr<term> {
            return untyped_constant_prepare_term(uc, db, keyspace, receiver);
        },
        [&] (const tuple_constructor& tc) -> ::shared_ptr<term> {
            return tuple_constructor_prepare_nontuple(tc, db, keyspace, receiver);
        },
        [&] (const collection_constructor& c) -> ::shared_ptr<term> {
            switch (c.style) {
            case collection_constructor::style_type::list: return list_prepare_term(c, db, keyspace, receiver);
            case collection_constructor::style_type::set: return set_prepare_term(c, db, keyspace, receiver);
            case collection_constructor::style_type::map: return map_prepare_term(c, db, keyspace, receiver);
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
        },
        [&] (const usertype_constructor& uc) -> ::shared_ptr<term> {
            return usertype_constructor_prepare_term(uc, db, keyspace, receiver);
        },
    }, expr);
}

::shared_ptr<term>
prepare_term_multi_column(const expression& expr, database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) {
    return std::visit(overloaded_functor{
        [&] (const bind_variable& bv) -> ::shared_ptr<term> {
            switch (bv.shape) {
            case expr::bind_variable::shape_type::scalar: on_internal_error(expr_logger, "prepare_term_multi_column(bind_variable(scalar))");
            case expr::bind_variable::shape_type::scalar_in: on_internal_error(expr_logger, "prepare_term_multi_column(bind_variable(scalar_in))");
            case expr::bind_variable::shape_type::tuple: return bind_variable_tuple_prepare_term(bv, db, keyspace, receivers);
            case expr::bind_variable::shape_type::tuple_in: return bind_variable_tuple_in_prepare_term(bv, db, keyspace, receivers);
            }
            on_internal_error(expr_logger, "unexpected shape in bind_variable");
        },
        [&] (const tuple_constructor& tc) -> ::shared_ptr<term> {
            return tuple_constructor_prepare_tuple(tc, db, keyspace, receivers);
        },
        [] (const auto& default_case) -> ::shared_ptr<term> {
            on_internal_error(expr_logger, fmt::format("prepare_term_multi_column({})", typeid(default_case).name()));
        },
    }, expr);
}

assignment_testable::test_result
test_assignment(const expression& expr, database& db, const sstring& keyspace, const column_specification& receiver) {
    using test_result = assignment_testable::test_result;
    return std::visit(overloaded_functor{
        [&] (const constant&) -> test_result {
            // constants shouldn't appear in parser output, only untyped_constants
            on_internal_error(expr_logger, "constants are not yet reachable via term_raw_expr::test_assignment()");
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
test_assignment_all(const std::vector<expression>& to_test, database& db, const sstring& keyspace, const column_specification& receiver) {
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
    virtual test_result test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const override {
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
