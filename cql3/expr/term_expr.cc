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
#include "types/set.hh"
#include "types/map.hh"

namespace cql3 {

lw_shared_ptr<column_specification>
maps::key_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(format("key({})", *column.name), true),
                dynamic_cast<const map_type_impl&>(column.type->without_reversed()).get_keys_type());
}

lw_shared_ptr<column_specification>
maps::value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(format("value({})", *column.name), true),
                 dynamic_cast<const map_type_impl&>(column.type->without_reversed()).get_values_type());
}

sstring
maps::literal::to_string() const {
    sstring result = "{";
    for (size_t i = 0; i < entries.size(); i++) {
        if (i > 0) {
            result += ", ";
        }
        result += entries[i].first->to_string();
        result += ":";
        result += entries[i].second->to_string();
    }
    result += "}";
    return result;
}

void
maps::literal::validate_assignable_to(database& db, const sstring& keyspace, const column_specification& receiver) const {
    if (!receiver.type->without_reversed().is_map()) {
        throw exceptions::invalid_request_exception(format("Invalid map literal for {} of type {}", *receiver.name, receiver.type->as_cql3_type()));
    }
    auto&& key_spec = maps::key_spec_of(receiver);
    auto&& value_spec = maps::value_spec_of(receiver);
    for (auto&& entry : entries) {
        if (!is_assignable(entry.first->test_assignment(db, keyspace, *key_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: key {} is not of type {}", *receiver.name, *entry.first, key_spec->type->as_cql3_type()));
        }
        if (!is_assignable(entry.second->test_assignment(db, keyspace, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: value {} is not of type {}", *receiver.name, *entry.second, value_spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
maps::literal::test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const {
    if (!dynamic_pointer_cast<const map_type_impl>(receiver.type)) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
    // If there is no elements, we can't say it's an exact match (an empty map if fundamentally polymorphic).
    if (entries.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    auto key_spec = maps::key_spec_of(receiver);
    auto value_spec = maps::value_spec_of(receiver);
    // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
    auto res = assignment_testable::test_result::EXACT_MATCH;
    for (auto entry : entries) {
        auto t1 = entry.first->test_assignment(db, keyspace, *key_spec);
        auto t2 = entry.second->test_assignment(db, keyspace, *value_spec);
        if (t1 == assignment_testable::test_result::NOT_ASSIGNABLE || t2 == assignment_testable::test_result::NOT_ASSIGNABLE)
            return assignment_testable::test_result::NOT_ASSIGNABLE;
        if (t1 != assignment_testable::test_result::EXACT_MATCH || t2 != assignment_testable::test_result::EXACT_MATCH)
            res = assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }
    return res;
}

::shared_ptr<term>
maps::literal::prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) const {
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    validate_assignable_to(db, keyspace, *receiver);

    auto key_spec = maps::key_spec_of(*receiver);
    auto value_spec = maps::value_spec_of(*receiver);
    std::unordered_map<shared_ptr<term>, shared_ptr<term>> values;
    values.reserve(entries.size());
    bool all_terminal = true;
    for (auto&& entry : entries) {
        auto k = entry.first->prepare(db, keyspace, key_spec);
        auto v = entry.second->prepare(db, keyspace, value_spec);

        if (k->contains_bind_marker() || v->contains_bind_marker()) {
            throw exceptions::invalid_request_exception(format("Invalid map literal for {}: bind variables are not supported inside collection literals", *receiver->name));
        }

        if (dynamic_pointer_cast<non_terminal>(k) || dynamic_pointer_cast<non_terminal>(v)) {
            all_terminal = false;
        }

        values.emplace(k, v);
    }
    delayed_value value(
            dynamic_cast<const map_type_impl&>(receiver->type->without_reversed()).get_keys_type()->as_less_comparator(),
            values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared<delayed_value>(std::move(value));
    }
}

lw_shared_ptr<column_specification>
sets::value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("value({})", *column.name), true),
            dynamic_cast<const set_type_impl&>(column.type->without_reversed()).get_elements_type());
}

sstring
sets::literal::to_string() const {
    return "{" + join(", ", _elements) + "}";
}

void
sets::literal::validate_assignable_to(database& db, const sstring& keyspace, const column_specification& receiver) const {
    if (!receiver.type->without_reversed().is_set()) {
        // We've parsed empty maps as a set literal to break the ambiguity so
        // handle that case now
        if (dynamic_pointer_cast<const map_type_impl>(receiver.type) && _elements.empty()) {
            return;
        }

        throw exceptions::invalid_request_exception(format("Invalid set literal for {} of type {}", receiver.name, receiver.type->as_cql3_type()));
    }

    auto&& value_spec = value_spec_of(receiver);
    for (shared_ptr<term::raw> rt : _elements) {
        if (!is_assignable(rt->test_assignment(db, keyspace, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid set literal for {}: value {} is not of type {}", *receiver.name, *rt, value_spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
sets::literal::test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const {
    if (!receiver.type->without_reversed().is_set()) {
        // We've parsed empty maps as a set literal to break the ambiguity so handle that case now
        if (dynamic_pointer_cast<const map_type_impl>(receiver.type) && _elements.empty()) {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }

        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty set if fundamentally polymorphic).
    if (_elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = value_spec_of(receiver);
    // FIXME: make assignment_testable::test_all() accept ranges
    std::vector<shared_ptr<assignment_testable>> to_test(_elements.begin(), _elements.end());
    return assignment_testable::test_all(db, keyspace, *value_spec, to_test);
}

shared_ptr<term>
sets::literal::prepare(database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) const {
    auto& receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
    validate_assignable_to(db, keyspace, *receiver);

    if (_elements.empty()) {

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

    auto value_spec = value_spec_of(*receiver);
    std::vector<shared_ptr<term>> values;
    values.reserve(_elements.size());
    bool all_terminal = true;
    for (shared_ptr<term::raw> rt : _elements)
    {
        auto t = rt->prepare(db, keyspace, value_spec);

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

    auto value = ::make_shared<delayed_value>(compare, std::move(values));
    if (all_terminal) {
        return value->bind(query_options::DEFAULT);
    } else {
        return value;
    }
}

}

namespace cql3::expr {

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
    for (auto& rt : c.elements) {
        if (!is_assignable(as_term_raw(rt)->test_assignment(db, keyspace, *value_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid list literal for {}: value {} is not of type {}",
                    *receiver.name, rt, value_spec->type->as_cql3_type()));
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
    std::vector<shared_ptr<assignment_testable>> to_test;
    to_test.reserve(c.elements.size());
    boost::copy(c.elements | boost::adaptors::transformed(as_term_raw), std::back_inserter(to_test));
    return assignment_testable::test_all(db, keyspace, *value_spec, to_test);
}


static
shared_ptr<term>
list_prepare_term(const collection_constructor& c, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver_) {
    auto receiver = std::get<lw_shared_ptr<column_specification>>(receiver_);
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
    for (auto& rt : c.elements) {
        auto&& t = as_term_raw(rt)->prepare(db, keyspace, value_spec);

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

        auto&& value = as_term_raw(tc.elements[i]);
        auto&& spec = component_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(value->test_assignment(db, keyspace, *spec))) {
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
        auto&& value = as_term_raw(tc.elements[i])->prepare(db, keyspace, component_spec_of(*receiver, i));
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
        auto&& t = as_term_raw(tc.elements[i])->prepare(db, keyspace, receivers[i]);
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
shared_ptr<term>
tuple_constructor_prepare_term(const tuple_constructor& tc, database& db, const sstring& keyspace, const column_specification_or_tuple& receiver) {
    return std::visit(overloaded_functor{
        [&] (const lw_shared_ptr<column_specification>& nontuple) {
            return tuple_constructor_prepare_nontuple(tc, db, keyspace, nontuple);
        },
        [&] (const std::vector<lw_shared_ptr<column_specification>>& tuple) {
            return tuple_constructor_prepare_tuple(tc, db, keyspace, tuple);
        },
    }, receiver);
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
        [&] (const tuple_constructor& tc) -> ::shared_ptr<term> {
            return tuple_constructor_prepare_term(tc, db, keyspace, receiver);
        },
        [&] (const collection_constructor& c) -> ::shared_ptr<term> {
            switch (c.style) {
            case collection_constructor::style_type::list: return list_prepare_term(c, db, keyspace, receiver);
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
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
        [&] (const tuple_constructor& tc) -> test_result {
            return tuple_constructor_test_assignment(tc, db, keyspace, receiver);
        },
        [&] (const collection_constructor& c) -> test_result {
            switch (c.style) {
            case collection_constructor::style_type::list: return list_test_assignment(c, db, keyspace, receiver);
            }
            on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(c.style)));
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

namespace cql3 {

lw_shared_ptr<column_specification>
lists::value_spec_of(const column_specification& column) {
    return cql3::expr::list_value_spec_of(column);
}

}
