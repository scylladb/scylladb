/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "lists.hh"
#include "update_parameters.hh"
#include "column_identifier.hh"
#include "cql3_type.hh"
#include "constants.hh"
#include <boost/iterator/transform_iterator.hpp>
#include <boost/range/adaptor/reversed.hpp>

namespace cql3 {

shared_ptr<column_specification>
lists::index_spec_of(shared_ptr<column_specification> column) {
    return make_shared<column_specification>(column->ks_name, column->cf_name,
            ::make_shared<column_identifier>(sprint("idx(%s)", *column->name), true), int32_type);
}

shared_ptr<column_specification>
lists::value_spec_of(shared_ptr<column_specification> column) {
    return make_shared<column_specification>(column->ks_name, column->cf_name,
            ::make_shared<column_identifier>(sprint("value(%s)", *column->name), true),
                dynamic_pointer_cast<const list_type_impl>(column->type)->get_elements_type());
}

shared_ptr<term>
lists::literal::prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    validate_assignable_to(db, keyspace, receiver);

    auto&& value_spec = value_spec_of(receiver);
    std::vector<shared_ptr<term>> values;
    values.reserve(_elements.size());
    bool all_terminal = true;
    for (auto rt : _elements) {
        auto&& t = rt->prepare(db, keyspace, value_spec);

        if (t->contains_bind_marker()) {
            throw exceptions::invalid_request_exception(sprint("Invalid list literal for %s: bind variables are not supported inside collection literals", *receiver->name));
        }
        if (dynamic_pointer_cast<non_terminal>(t)) {
            all_terminal = false;
        }
        values.push_back(std::move(t));
    }
    delayed_value value(values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared(std::move(value));
    }
}

void
lists::literal::validate_assignable_to(database& db, const sstring keyspace, shared_ptr<column_specification> receiver) {
    if (!dynamic_pointer_cast<const list_type_impl>(receiver->type)) {
        throw exceptions::invalid_request_exception(sprint("Invalid list literal for %s of type %s",
                *receiver->name, *receiver->type->as_cql3_type()));
    }
    auto&& value_spec = value_spec_of(receiver);
    for (auto rt : _elements) {
        if (!is_assignable(rt->test_assignment(db, keyspace, value_spec))) {
            throw exceptions::invalid_request_exception(sprint("Invalid list literal for %s: value %s is not of type %s",
                    *receiver->name, *rt, *value_spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result
lists::literal::test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    if (!dynamic_pointer_cast<const list_type_impl>(receiver->type)) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }

    // If there is no elements, we can't say it's an exact match (an empty list if fundamentally polymorphic).
    if (_elements.empty()) {
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    }

    auto&& value_spec = value_spec_of(receiver);
    std::vector<shared_ptr<assignment_testable>> to_test;
    to_test.reserve(_elements.size());
    std::copy(_elements.begin(), _elements.end(), std::back_inserter(to_test));
    return assignment_testable::test_all(db, keyspace, value_spec, to_test);
}

sstring
lists::literal::to_string() const {
    return ::to_string(_elements);
}

lists::value
lists::value::from_serialized(bytes_view v, list_type type, serialization_format sf) {
    try {
        // Collections have this small hack that validate cannot be called on a serialized object,
        // but compose does the validation (so we're fine).
        // FIXME: deserializeForNativeProtocol()?!
        auto l = value_cast<list_type_impl::native_type>(type->deserialize(v, sf));
        std::vector<bytes_opt> elements;
        elements.reserve(l.size());
        for (auto&& element : l) {
            // elements can be null in lists that represent a set of IN values
            elements.push_back(element.is_null() ? bytes_opt() : bytes_opt(type->get_elements_type()->decompose(element)));
        }
        return value(std::move(elements));
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

bytes_opt
lists::value::get(const query_options& options) {
    return get_with_protocol_version(options.get_serialization_format());
}

bytes
lists::value::get_with_protocol_version(serialization_format sf) {
    // Can't use boost::indirect_iterator, because optional is not an iterator
    auto deref = [] (bytes_opt& x) { return *x; };
    return collection_type_impl::pack(
            boost::make_transform_iterator(_elements.begin(), deref),
            boost::make_transform_iterator( _elements.end(), deref),
            _elements.size(), sf);
}

bool
lists::value::equals(shared_ptr<list_type_impl> lt, const value& v) {
    if (_elements.size() != v._elements.size()) {
        return false;
    }
    return std::equal(_elements.begin(), _elements.end(),
            v._elements.begin(),
            [t = lt->get_elements_type()] (const bytes_opt& e1, const bytes_opt& e2) { return t->equal(*e1, *e2); });
}

std::vector<bytes_opt>
lists::value::get_elements() {
    return _elements;
}

sstring
lists::value::to_string() const {
    std::ostringstream os;
    os << "[";
    bool is_first = true;
    for (auto&& e : _elements) {
        if (!is_first) {
            os << ", ";
        }
        is_first = false;
        os << to_hex(e);
    }
    os << "]";
    return os.str();
}

bool
lists::delayed_value::contains_bind_marker() const {
    // False since we don't support them in collection
    return false;
}

void
lists::delayed_value::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
}

shared_ptr<terminal>
lists::delayed_value::bind(const query_options& options) {
    std::vector<bytes_opt> buffers;
    buffers.reserve(_elements.size());
    for (auto&& t : _elements) {
        auto bo = t->bind_and_get(options);

        if (!bo) {
            throw exceptions::invalid_request_exception("null is not supported inside collections");
        }

        // We don't support value > 64K because the serialization format encode the length as an unsigned short.
        if (bo->size() > std::numeric_limits<uint16_t>::max()) {
            throw exceptions::invalid_request_exception(sprint("List value is too long. List values are limited to %d bytes but %d bytes value provided",
                    std::numeric_limits<uint16_t>::max(),
                    bo->size()));
        }

        buffers.push_back(std::move(to_bytes(*bo)));
    }
    return ::make_shared<value>(buffers);
}

::shared_ptr<terminal>
lists::marker::bind(const query_options& options) {
    const auto& value = options.get_value_at(_bind_index);
    auto ltype = static_pointer_cast<const list_type_impl>(_receiver->type);
    if (!value) {
        return nullptr;
    } else {
        return make_shared(value::from_serialized(*value, std::move(ltype), options.get_serialization_format()));
    }
}

constexpr const db_clock::time_point lists::precision_time::REFERENCE_TIME;
thread_local lists::precision_time lists::precision_time::_last = {db_clock::time_point::max(), 0};

lists::precision_time
lists::precision_time::get_next(db_clock::time_point millis) {
    // FIXME: and if time goes backwards?
    assert(millis <= _last.millis);
    auto next =  millis < _last.millis
            ? precision_time{millis, 9999}
            : precision_time{millis, std::max(0, _last.nanos - 1)};
    _last = next;
    return next;
}

void
lists::setter::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    if (column.type->is_multi_cell()) {
        // delete + append
        collection_type_impl::mutation mut;
        mut.tomb = params.make_tombstone_just_before();
        auto ctype = static_pointer_cast<const list_type_impl>(column.type);
        auto col_mut = ctype->serialize_mutation_form(std::move(mut));
        m.set_cell(prefix, column, std::move(col_mut));
    }
    do_append(_t, m, prefix, column, params);
}

bool
lists::setter_by_index::requires_read() {
    return true;
}

void
lists::setter_by_index::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
    operation::collect_marker_specification(bound_names);
    _idx->collect_marker_specification(std::move(bound_names));
}

void
lists::setter_by_index::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    // we should not get here for frozen lists
    assert(column.type->is_multi_cell()); // "Attempted to set an individual element on a frozen list";

    auto row_key = clustering_key::from_clustering_prefix(*params._schema, prefix);

    auto index = _idx->bind_and_get(params._options);
    auto value = _t->bind_and_get(params._options);

    if (!index) {
        throw exceptions::invalid_request_exception("Invalid null value for list index");
    }

    auto idx = net::ntoh(int32_t(*unaligned_cast<int32_t>(index->begin())));

    auto existing_list_opt = params.get_prefetched_list(m.key(), row_key, column);
    if (!existing_list_opt) {
        throw exceptions::invalid_request_exception("Attempted to set an element on a list which is null");
    }
    collection_mutation::view existing_list_ser = *existing_list_opt;
    auto ltype = dynamic_pointer_cast<const list_type_impl>(column.type);
    collection_type_impl::mutation_view existing_list = ltype->deserialize_mutation_form(existing_list_ser);
    // we verified that index is an int32_type
    if (idx < 0 || size_t(idx) >= existing_list.cells.size()) {
        throw exceptions::invalid_request_exception(sprint("List index %d out of bound, list has size %d",
                idx, existing_list.cells.size()));
    }

    bytes_view eidx = existing_list.cells[idx].first;
    list_type_impl::mutation mut;
    mut.cells.reserve(1);
    if (!value) {
        mut.cells.emplace_back(to_bytes(eidx), params.make_dead_cell());
    } else {
        if (value->size() > std::numeric_limits<uint16_t>::max()) {
            throw exceptions::invalid_request_exception(
                    sprint("List value is too long. List values are limited to %d bytes but %d bytes value provided",
                            std::numeric_limits<uint16_t>::max(), value->size()));
        }
        mut.cells.emplace_back(to_bytes(eidx), params.make_cell(*value));
    }
    auto smut = ltype->serialize_mutation_form(mut);
    m.set_cell(prefix, column, atomic_cell_or_collection::from_collection_mutation(std::move(smut)));
}

void
lists::appender::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to append to a frozen list";
    do_append(_t, m, prefix, column, params);
}

void
lists::do_append(shared_ptr<term> t,
        mutation& m,
        const exploded_clustering_prefix& prefix,
        const column_definition& column,
        const update_parameters& params) {
    auto&& value = t->bind(params._options);
    auto&& list_value = dynamic_pointer_cast<lists::value>(value);
    auto&& ltype = dynamic_pointer_cast<const list_type_impl>(column.type);
    if (column.type->is_multi_cell()) {
        // If we append null, do nothing. Note that for Setter, we've
        // already removed the previous value so we're good here too
        if (!value) {
            return;
        }

        auto&& to_add = list_value->_elements;
        collection_type_impl::mutation appended;
        appended.cells.reserve(to_add.size());
        for (auto&& e : to_add) {
            auto uuid1 = utils::UUID_gen::get_time_UUID_bytes();
            auto uuid = bytes(reinterpret_cast<const int8_t*>(uuid1.data()), uuid1.size());
            // FIXME: can e be empty?
            appended.cells.emplace_back(std::move(uuid), params.make_cell(*e));
        }
        m.set_cell(prefix, column, ltype->serialize_mutation_form(appended));
    } else {
        // for frozen lists, we're overwriting the whole cell value
        if (!value) {
            m.set_cell(prefix, column, params.make_dead_cell());
        } else {
            auto&& to_add = list_value->_elements;
            auto deref = [] (const bytes_opt& v) { return *v; };
            auto&& newv = collection_mutation::one{list_type_impl::pack(
                    boost::make_transform_iterator(to_add.begin(), deref),
                    boost::make_transform_iterator(to_add.end(), deref),
                    to_add.size(), serialization_format::internal())};
            m.set_cell(prefix, column, atomic_cell_or_collection::from_collection_mutation(std::move(newv)));
        }
    }
}

void
lists::prepender::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to prepend to a frozen list";
    auto&& value = _t->bind(params._options);
    if (!value) {
        return;
    }

    auto&& lvalue = dynamic_pointer_cast<lists::value>(std::move(value));
    assert(lvalue);
    auto time = precision_time::REFERENCE_TIME - (db_clock::now() - precision_time::REFERENCE_TIME);

    collection_type_impl::mutation mut;
    mut.cells.reserve(lvalue->get_elements().size());
    // We reverse the order of insertion, so that the last element gets the lastest time
    // (lists are sorted by time)
    for (auto&& v : lvalue->_elements | boost::adaptors::reversed) {
        auto&& pt = precision_time::get_next(time);
        auto uuid = utils::UUID_gen::get_time_UUID_bytes(pt.millis.time_since_epoch().count(), pt.nanos);
        mut.cells.emplace_back(bytes(uuid.data(), uuid.size()), params.make_cell(*v));
    }
    // now reverse again, to get the original order back
    std::reverse(mut.cells.begin(), mut.cells.end());
    auto&& ltype = static_cast<const list_type_impl*>(column.type.get());
    m.set_cell(prefix, column, atomic_cell_or_collection::from_collection_mutation(ltype->serialize_mutation_form(std::move(mut))));
}

bool
lists::discarder::requires_read() {
    return true;
}

void
lists::discarder::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to delete from a frozen list";
    auto&& row_key = clustering_key::from_clustering_prefix(*params._schema, prefix);
    auto&& existing_list = params.get_prefetched_list(m.key(), row_key, column);
    // We want to call bind before possibly returning to reject queries where the value provided is not a list.
    auto&& value = _t->bind(params._options);

    auto&& ltype = static_pointer_cast<const list_type_impl>(column.type);

    if (!existing_list) {
        return;
    }

    auto&& elist = ltype->deserialize_mutation_form(*existing_list);

    if (elist.cells.empty()) {
        return;
    }

    if (!value) {
        return;
    }

    auto lvalue = dynamic_pointer_cast<lists::value>(value);
    assert(lvalue);

    // Note: below, we will call 'contains' on this toDiscard list for each element of existingList.
    // Meaning that if toDiscard is big, converting it to a HashSet might be more efficient. However,
    // the read-before-write this operation requires limits its usefulness on big lists, so in practice
    // toDiscard will be small and keeping a list will be more efficient.
    auto&& to_discard = lvalue->_elements;
    collection_type_impl::mutation mnew;
    for (auto&& cell : elist.cells) {
        auto have_value = [&] (bytes_view value) {
            return std::find_if(to_discard.begin(), to_discard.end(),
                                [ltype, value] (auto&& v) { return ltype->get_elements_type()->equal(*v, value); })
                                         != to_discard.end();
        };
        if (cell.second.is_live() && have_value(cell.second.value())) {
            mnew.cells.emplace_back(bytes(cell.first.begin(), cell.first.end()), params.make_dead_cell());
        }
    }
    auto mnew_ser = ltype->serialize_mutation_form(mnew);
    m.set_cell(prefix, column, atomic_cell_or_collection::from_collection_mutation(std::move(mnew_ser)));
}

bool
lists::discarder_by_index::requires_read() {
    return true;
}

void
lists::discarder_by_index::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to delete an item by index from a frozen list";
    auto&& index = _t->bind(params._options);
    if (!index) {
        throw exceptions::invalid_request_exception("Invalid null value for list index");
    }

    auto ltype = static_pointer_cast<const list_type_impl>(column.type);
    auto cvalue = dynamic_pointer_cast<constants::value>(index);
    assert(cvalue);

    auto row_key = clustering_key::from_clustering_prefix(*params._schema, prefix);
    auto&& existing_list = params.get_prefetched_list(m.key(), row_key, column);
    int32_t idx = read_simple_exactly<int32_t>(*cvalue->_bytes);
    if (!existing_list) {
        throw exceptions::invalid_request_exception("Attempted to delete an element from a list which is null");
    }
    auto&& deserialized = ltype->deserialize_mutation_form(*existing_list);
    if (idx < 0 || size_t(idx) >= deserialized.cells.size()) {
        throw exceptions::invalid_request_exception(sprint("List index %d out of bound, list has size %d", idx, deserialized.cells.size()));
    }
    collection_type_impl::mutation mut;
    mut.cells.emplace_back(to_bytes(deserialized.cells[idx].first), params.make_dead_cell());
    m.set_cell(prefix, column, ltype->serialize_mutation_form(mut));
}

}
