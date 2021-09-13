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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "maps.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/term.hh"
#include "operation.hh"
#include "update_parameters.hh"
#include "exceptions/exceptions.hh"
#include "cql3/cql3_type.hh"
#include "constants.hh"
#include "types/map.hh"

namespace cql3 {

maps::value
maps::value::from_serialized(const raw_value_view& fragmented_value, const map_type_impl& type, cql_serialization_format sf) {
    try {
        // Collections have this small hack that validate cannot be called on a serialized object,
        // but compose does the validation (so we're fine).
        // FIXME: deserialize_for_native_protocol?!
        auto m = fragmented_value.deserialize<map_type_impl::native_type>(type, sf);
        std::map<managed_bytes, managed_bytes, serialized_compare> map(type.get_keys_type()->as_less_comparator());
        if (sf.collection_format_unchanged()) {
            std::vector<std::pair<managed_bytes, managed_bytes>> tmp = fragmented_value.with_value([sf] (const FragmentedView auto& v) {
                return partially_deserialize_map(v, sf);
            });
            for (auto&& key_value : tmp) {
                map.insert(std::move(key_value));
            }
        } else [[unlikely]] {
            auto m = fragmented_value.deserialize<map_type_impl::native_type>(type, sf);
            for (auto&& e : m) {
                map.emplace(type.get_keys_type()->decompose(e.first),
                            type.get_values_type()->decompose(e.second));
            }
        }
        return maps::value(std::move(map), type.shared_from_this());
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

cql3::raw_value
maps::value::get(const query_options& options) {
    return cql3::raw_value::make_value(get_with_protocol_version(options.get_cql_serialization_format()));
}

managed_bytes
maps::value::get_with_protocol_version(cql_serialization_format sf) {
    //FIXME: share code with serialize_partially_deserialized_form
    size_t len = collection_value_len(sf) * map.size() * 2 + collection_size_len(sf);
    for (auto&& e : map) {
        len += e.first.size() + e.second.size();
    }
    managed_bytes b(managed_bytes::initialized_later(), len);
    managed_bytes_mutable_view out(b);

    write_collection_size(out, map.size(), sf);
    for (auto&& e : map) {
        write_collection_value(out, sf, e.first);
        write_collection_value(out, sf, e.second);
    }
    return b;
}

bool
maps::value::equals(const map_type_impl& mt, const value& v) {
    return std::equal(map.begin(), map.end(),
                      v.map.begin(), v.map.end(),
                      [&mt] (auto&& e1, auto&& e2) {
        return mt.get_keys_type()->compare(e1.first, e2.first) == 0
                && mt.get_values_type()->compare(e1.second, e2.second) == 0;
    });
}

sstring
maps::value::to_string() const {
    // FIXME:
    abort();
}

bool
maps::delayed_value::contains_bind_marker() const {
    // False since we don't support them in collection
    return false;
}

void
maps::delayed_value::fill_prepare_context(prepare_context& ctx) const {
}

shared_ptr<terminal>
maps::delayed_value::bind(const query_options& options) {
    const map_type_impl& my_map_type = dynamic_cast<const map_type_impl&>(_my_type->without_reversed());

    serialized_compare comparator = my_map_type.get_keys_type()->as_less_comparator();
    std::map<managed_bytes, managed_bytes, serialized_compare> buffers(std::move(comparator));
    for (auto&& entry : _elements) {
        auto&& key = entry.first;
        auto&& value = entry.second;

        // We don't support values > 64K because the serialization format encode the length as an unsigned short.
        auto key_bytes = expr::evaluate_to_raw_view(key, options);
        if (key_bytes.is_null()) {
            throw exceptions::invalid_request_exception("null is not supported inside collections");
        }
        if (key_bytes.is_unset_value()) {
            throw exceptions::invalid_request_exception("unset value is not supported inside collections");
        }
        if (key_bytes.size_bytes() > std::numeric_limits<uint16_t>::max()) {
            throw exceptions::invalid_request_exception(format("Map key is too long. Map keys are limited to {:d} bytes but {:d} bytes keys provided",
                                                   std::numeric_limits<uint16_t>::max(),
                                                   key_bytes.size_bytes()));
        }
        auto value_bytes = expr::evaluate_to_raw_view(value, options);
        if (value_bytes.is_null()) {
            throw exceptions::invalid_request_exception("null is not supported inside collections");\
        }
        if (value_bytes.is_unset_value()) {
            return constants::UNSET_VALUE;
        }
        buffers.emplace(*to_managed_bytes_opt(key_bytes), *to_managed_bytes_opt(value_bytes));
    }
    return ::make_shared<value>(std::move(buffers), _my_type);
}

::shared_ptr<terminal>
maps::marker::bind(const query_options& options) {
    auto val = options.get_value_at(_bind_index);
    if (val.is_null()) {
        return nullptr;
    }
    if (val.is_unset_value()) {
        return constants::UNSET_VALUE;
    }
    try {
        val.validate(*_receiver->type, options.get_cql_serialization_format());
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(
                format("Exception while binding column {:s}: {:s}", _receiver->name->to_cql_string(), e.what()));
    }
    return ::make_shared<maps::value>(
            maps::value::from_serialized(
                    val,
                    dynamic_cast<const map_type_impl&>(_receiver->type->without_reversed()),
                    options.get_cql_serialization_format()));
}

void
maps::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    expr::constant value = expr::evaluate(_t, params._options);
    execute(m, row_key, params, column, value);
}

void
maps::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const expr::constant& value) {
    if (value.is_unset_value()) {
        return;
    }
    if (column.type->is_multi_cell()) {
        // Delete all cells first, then put new ones
        collection_mutation_description mut;
        mut.tomb = params.make_tombstone_just_before();
        m.set_cell(row_key, column, mut.serialize(*column.type));
    }
    do_put(m, row_key, params, value, column);
}

void
maps::setter_by_key::fill_prepare_context(prepare_context& ctx) const {
    operation::fill_prepare_context(ctx);
    _k->fill_prepare_context(ctx);
}

void
maps::setter_by_key::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    using exceptions::invalid_request_exception;
    assert(column.type->is_multi_cell()); // "Attempted to set a value for a single key on a frozen map"m
    auto key = expr::evaluate_to_raw_view(_k, params._options);
    auto value = expr::evaluate_to_raw_view(_t, params._options);
    if (value.is_unset_value()) {
        return;
    }
    if (key.is_unset_value()) {
        throw invalid_request_exception("Invalid unset map key");
    }
    if (!key) {
        throw invalid_request_exception("Invalid null map key");
    }
    auto ctype = static_cast<const map_type_impl*>(column.type.get());
    auto avalue = value ? params.make_cell(*ctype->get_values_type(), value, atomic_cell::collection_member::yes) : params.make_dead_cell();
    collection_mutation_description update;
    update.cells.emplace_back(to_bytes(key), std::move(avalue));

    m.set_cell(prefix, column, update.serialize(*ctype));
}

void
maps::putter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to add items to a frozen map";
    expr::constant value = expr::evaluate(_t, params._options);
    if (!value.is_unset_value()) {
        do_put(m, prefix, params, value, column);
    }
}

void
maps::do_put(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params,
        const expr::constant& map_value, const column_definition& column) {
    if (column.type->is_multi_cell()) {
        if (map_value.is_null()) {
            return;
        }

        collection_mutation_description mut;

        auto ctype = static_cast<const map_type_impl*>(column.type.get());
        for (auto&& e : expr::get_map_elements(map_value)) {
            mut.cells.emplace_back(to_bytes(e.first), params.make_cell(*ctype->get_values_type(), raw_value_view::make_value(e.second), atomic_cell::collection_member::yes));
        }

        m.set_cell(prefix, column, mut.serialize(*ctype));
    } else {
        // for frozen maps, we're overwriting the whole cell
        if (map_value.is_null()) {
            m.set_cell(prefix, column, params.make_dead_cell());
        } else {
            m.set_cell(prefix, column, params.make_cell(*column.type, map_value.value.to_view()));
        }
    }
}

void
maps::discarder_by_key::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to delete a single key in a frozen map";
    expr::constant key = expr::evaluate(_t, params._options);
    if (key.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null map key");
    }
    if (key.is_unset_value()) {
        throw exceptions::invalid_request_exception("Invalid unset map key");
    }
    collection_mutation_description mut;
    mut.cells.emplace_back(std::move(key.value).to_bytes(), params.make_dead_cell());

    m.set_cell(prefix, column, mut.serialize(*column.type));
}

}

