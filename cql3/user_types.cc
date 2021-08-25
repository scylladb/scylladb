/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified by ScyllaDB
 *
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

#include "cql3/user_types.hh"

#include "cql3/cql3_type.hh"
#include "cql3/constants.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

#include "types/user.hh"

namespace cql3 {

user_types::value::value(std::vector<managed_bytes_opt> elements, data_type my_type)
        : terminal(std::move(my_type)), _elements(std::move(elements)) {
}

user_types::value user_types::value::from_serialized(const raw_value_view& v, const user_type_impl& type) {
    return v.with_value([&] (const FragmentedView auto& val) {
        std::vector<managed_bytes_opt> elements = type.split_fragmented(val);
        if (elements.size() > type.size()) {
            throw exceptions::invalid_request_exception(
                    format("User Defined Type value contained too many fields (expected {}, got {})", type.size(), elements.size()));
        }

        return value(std::move(elements), type.shared_from_this());
    });
}

cql3::raw_value user_types::value::get(const query_options&) {
    return cql3::raw_value::make_value(tuple_type_impl::build_value_fragmented(_elements));
}

sstring user_types::value::to_string() const {
    return format("({})", join(", ", _elements));
}

user_types::delayed_value::delayed_value(user_type type, std::vector<shared_ptr<term>> values)
        : _type(std::move(type)), _values(std::move(values)) {
}
bool user_types::delayed_value::contains_bind_marker() const {
    return boost::algorithm::any_of(_values, std::mem_fn(&term::contains_bind_marker));
}

void user_types::delayed_value::fill_prepare_context(prepare_context& ctx) const {
    for (auto&& v : _values) {
        v->fill_prepare_context(ctx);
    }
}

std::vector<managed_bytes_opt> user_types::delayed_value::bind_internal(const query_options& options) {
    auto sf = options.get_cql_serialization_format();

    // user_types::literal::prepare makes sure that every field gets a corresponding value.
    // For missing fields the values become nullopts.
    assert(_type->size() == _values.size());

    std::vector<managed_bytes_opt> buffers;
    for (size_t i = 0; i < _type->size(); ++i) {
        const auto& value = expr::evaluate_to_raw_view(_values[i], options);
        if (!_type->is_multi_cell() && value.is_unset_value()) {
            throw exceptions::invalid_request_exception(format("Invalid unset value for field '{}' of user defined type {}",
                        _type->field_name_as_string(i), _type->get_name_as_string()));
        }

        buffers.push_back(to_managed_bytes_opt(value));

        // Inside UDT values, we must force the serialization of collections to v3 whatever protocol
        // version is in use since we're going to store directly that serialized value.
        if (!sf.collection_format_unchanged() && _type->field_type(i)->is_collection() && buffers.back()) {
            auto&& ctype = static_pointer_cast<const collection_type_impl>(_type->field_type(i));
            buffers.back() = ctype->reserialize(sf, cql_serialization_format::latest(), managed_bytes_view(*buffers.back()));
        }
    }
    return buffers;
}

shared_ptr<terminal> user_types::delayed_value::bind(const query_options& options) {
    return ::make_shared<user_types::value>(bind_internal(options), _type);
}

shared_ptr<terminal> user_types::marker::bind(const query_options& options) {
    auto value = options.get_value_at(_bind_index);
    if (value.is_null()) {
        return nullptr;
    }
    if (value.is_unset_value()) {
        return constants::UNSET_VALUE;
    }
    return make_shared<user_types::value>(value::from_serialized(value, static_cast<const user_type_impl&>(*_receiver->type)));
}

void user_types::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    const expr::constant value = expr::evaluate(_t, params._options);
    execute(m, row_key, params, column, value);
}

void user_types::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const expr::constant& ut_value) {
    if (ut_value.is_unset_value()) {
        return;
    }

    auto& type = static_cast<const user_type_impl&>(*column.type);
    if (type.is_multi_cell()) {
        // Non-frozen user defined type.

        collection_mutation_description mut;

        // Setting a non-frozen (multi-cell) UDT means overwriting all cells.
        // We start by deleting all existing cells.

        // TODO? (kbraun): is this the desired behaviour?
        // This is how C* does it, but consider the following scenario:
        // create type ut (a int, b int);
        // create table cf (a int primary key, b ut);
        // insert into cf json '{"a": 1, "b":{"a":1, "b":2}}';
        // insert into cf json '{"a": 1, "b":{"a":1}}' default unset;
        // currently this would set b.b to null. However, by specifying 'default unset',
        // perhaps the user intended to leave b.a unchanged.
        mut.tomb = params.make_tombstone_just_before();

        if (!ut_value.is_null()) {
            const auto& elems = expr::get_user_type_elements(ut_value);
            // There might be fewer elements given than fields in the type
            // (e.g. when the user uses a short tuple literal), but never more.
            assert(elems.size() <= type.size());

            for (size_t i = 0; i < elems.size(); ++i) {
                if (!elems[i]) {
                    // This field was not specified or was specified as null.
                    continue;
                }

                mut.cells.emplace_back(serialize_field_index(i),
                        params.make_cell(*type.type(i), *elems[i], atomic_cell::collection_member::yes));
            }
        }

        m.set_cell(row_key, column, mut.serialize(type));
    } else {
        if (!ut_value.is_null()) {
            m.set_cell(row_key, column, params.make_cell(type, ut_value.value.to_view()));
        } else {
            m.set_cell(row_key, column, params.make_dead_cell());
        }
    }
}

void user_types::setter_by_field::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_user_type() && column.type->is_multi_cell());

    auto value = expr::evaluate_to_raw_view(_t, params._options);
    if (value.is_unset_value()) {
        return;
    }

    auto& type = static_cast<const user_type_impl&>(*column.type);

    collection_mutation_description mut;
    mut.cells.emplace_back(serialize_field_index(_field_idx), value
                ? params.make_cell(*type.type(_field_idx), value, atomic_cell::collection_member::yes)
                : params.make_dead_cell());

    m.set_cell(row_key, column, mut.serialize(type));
}

void user_types::deleter_by_field::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_user_type() && column.type->is_multi_cell());

    collection_mutation_description mut;
    mut.cells.emplace_back(serialize_field_index(_field_idx), params.make_dead_cell());

    m.set_cell(row_key, column, mut.serialize(*column.type));
}

}
