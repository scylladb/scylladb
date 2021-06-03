/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "sets.hh"
#include "constants.hh"
#include "cql3_type.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace cql3 {

lw_shared_ptr<column_specification>
sets::value_spec_of(const column_specification& column) {
    return make_lw_shared<column_specification>(column.ks_name, column.cf_name,
            ::make_shared<column_identifier>(format("value({})", *column.name), true),
            dynamic_cast<const set_type_impl&>(column.type->without_reversed()).get_elements_type());
}

shared_ptr<term>
sets::literal::prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const {
    validate_assignable_to(db, keyspace, *receiver);

    if (_elements.empty()) {

        // In Cassandra, an empty (unfrozen) map/set/list is equivalent to the column being null. In
        // other words a non-frozen collection only exists if it has elements.  Return nullptr right
        // away to simplify predicate evaluation.  See also
        // https://issues.apache.org/jira/browse/CASSANDRA-5141
        if (receiver->type->is_multi_cell()) {
            return cql3::constants::null_literal::NULL_VALUE;
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

sstring
sets::literal::to_string() const {
    return "{" + join(", ", _elements) + "}";
}

sets::value
sets::value::from_serialized(const raw_value_view& val, const set_type_impl& type, cql_serialization_format sf) {
    try {
        std::set<managed_bytes, serialized_compare> elements(type.get_elements_type()->as_less_comparator());
        if (sf.collection_format_unchanged()) {
            utils::chunked_vector<managed_bytes> tmp = val.with_value([sf] (const FragmentedView auto& v) {
                return partially_deserialize_listlike(v, sf);
            });
            for (auto&& element : tmp) {
                elements.insert(std::move(element));
            }
        } else [[unlikely]] {
            auto s = val.deserialize<set_type_impl::native_type>(type, sf);
            for (auto&& element : s) {
                elements.insert(elements.end(), managed_bytes(type.get_elements_type()->decompose(element)));
            }
        }
        return value(std::move(elements));
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

cql3::raw_value
sets::value::get(const query_options& options) {
    return cql3::raw_value::make_value(get_with_protocol_version(options.get_cql_serialization_format()));
}

managed_bytes
sets::value::get_with_protocol_version(cql_serialization_format sf) {
    return collection_type_impl::pack_fragmented(_elements.begin(), _elements.end(),
            _elements.size(), sf);
}

bool
sets::value::equals(const set_type_impl& st, const value& v) {
    if (_elements.size() != v._elements.size()) {
        return false;
    }
    auto&& elements_type = st.get_elements_type();
    return std::equal(_elements.begin(), _elements.end(),
            v._elements.begin(),
            [elements_type] (managed_bytes_view v1, managed_bytes_view v2) {
                return elements_type->equal(v1, v2);
            });
}

sstring
sets::value::to_string() const {
    sstring result = "{";
    bool first = true;
    for (auto&& e : _elements) {
        if (!first) {
            result += ", ";
        }
        first = true;
        result += to_hex(e);
    }
    result += "}";
    return result;
}

bool
sets::delayed_value::contains_bind_marker() const {
    // False since we don't support them in collection
    return false;
}

void
sets::delayed_value::collect_marker_specification(variable_specifications& bound_names) const {
}

shared_ptr<terminal>
sets::delayed_value::bind(const query_options& options) {
    std::set<managed_bytes, serialized_compare> buffers(_comparator);
    for (auto&& t : _elements) {
        auto b = t->bind_and_get(options);

        if (b.is_null()) {
            throw exceptions::invalid_request_exception("null is not supported inside collections");
        }
        if (b.is_unset_value()) {
            return constants::UNSET_VALUE;
        }
        // We don't support value > 64K because the serialization format encode the length as an unsigned short.
        if (b.size_bytes() > std::numeric_limits<uint16_t>::max()) {
            throw exceptions::invalid_request_exception(format("Set value is too long. Set values are limited to {:d} bytes but {:d} bytes value provided",
                    std::numeric_limits<uint16_t>::max(),
                    b.size_bytes()));
        }
        buffers.insert(buffers.end(), *to_managed_bytes_opt(b));
    }
    return ::make_shared<value>(std::move(buffers));
}


sets::marker::marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
    : abstract_marker{bind_index, std::move(receiver)} {
    if (!_receiver->type->without_reversed().is_set()) {
        throw std::runtime_error(format("Receiver {} for set marker has wrong type: {}",
                                        _receiver->cf_name, _receiver->type->name()));
    }
}

::shared_ptr<terminal>
sets::marker::bind(const query_options& options) {
    const auto& value = options.get_value_at(_bind_index);
    if (value.is_null()) {
        return nullptr;
    } else if (value.is_unset_value()) {
        return constants::UNSET_VALUE;
    } else {
        auto& type = dynamic_cast<const set_type_impl&>(_receiver->type->without_reversed());
        try {
            value.validate(type, options.get_cql_serialization_format());
        } catch (marshal_exception& e) {
            throw exceptions::invalid_request_exception(
                    format("Exception while binding column {:s}: {:s}", _receiver->name->to_cql_string(), e.what()));
        }
        return make_shared<cql3::sets::value>(value::from_serialized(value, type, options.get_cql_serialization_format()));
    }
}

void
sets::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    auto value = _t->bind(params._options);
    execute(m, row_key, params, column, std::move(value));
}

void
sets::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, ::shared_ptr<terminal> value) {
    if (value == constants::UNSET_VALUE) {
        return;
    }
    if (column.type->is_multi_cell()) {
        // Delete all cells first, then add new ones
        collection_mutation_description mut;
        mut.tomb = params.make_tombstone_just_before();
        m.set_cell(row_key, column, mut.serialize(*column.type));
    }
    adder::do_add(m, row_key, params, value, column);
}

void
sets::adder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    const auto& value = _t->bind(params._options);
    if (value == constants::UNSET_VALUE) {
        return;
    }
    assert(column.type->is_multi_cell()); // "Attempted to add items to a frozen set";
    do_add(m, row_key, params, value, column);
}

void
sets::adder::do_add(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params,
        shared_ptr<term> value, const column_definition& column) {
    auto set_value = dynamic_pointer_cast<sets::value>(std::move(value));
    auto& set_type = dynamic_cast<const set_type_impl&>(column.type->without_reversed());
    if (column.type->is_multi_cell()) {
        if (!set_value || set_value->_elements.empty()) {
            return;
        }

        // FIXME: collection_mutation_view_description? not compatible with params.make_cell().
        collection_mutation_description mut;

        for (auto&& e : set_value->_elements) {
            mut.cells.emplace_back(to_bytes(e), params.make_cell(*set_type.value_comparator(), bytes_view(), atomic_cell::collection_member::yes));
        }

        m.set_cell(row_key, column, mut.serialize(set_type));
    } else if (set_value != nullptr) {
        // for frozen sets, we're overwriting the whole cell
        auto v = set_value->get_with_protocol_version(cql_serialization_format::internal());
        m.set_cell(row_key, column, params.make_cell(*column.type, raw_value_view::make_value(v)));
    } else {
        m.set_cell(row_key, column, params.make_dead_cell());
    }
}

void
sets::discarder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to remove items from a frozen set";

    auto&& value = _t->bind(params._options);
    if (!value || value == constants::UNSET_VALUE) {
        return;
    }

    collection_mutation_description mut;
    auto svalue = dynamic_pointer_cast<sets::value>(value);
    assert(svalue);
    mut.cells.reserve(svalue->_elements.size());
    for (auto&& e : svalue->_elements) {
        mut.cells.push_back({to_bytes(e), params.make_dead_cell()});
    }
    m.set_cell(row_key, column, mut.serialize(*column.type));
}

void sets::element_discarder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params)
{
    assert(column.type->is_multi_cell() && "Attempted to remove items from a frozen set");
    auto elt = _t->bind(params._options);
    if (!elt) {
        throw exceptions::invalid_request_exception("Invalid null set element");
    }
    collection_mutation_description mut;
    mut.cells.emplace_back(elt->get(params._options).to_bytes(), params.make_dead_cell());
    m.set_cell(row_key, column, mut.serialize(*column.type));
}

}
