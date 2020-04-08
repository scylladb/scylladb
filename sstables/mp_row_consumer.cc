/*
 * Copyright (C) 2018 ScyllaDB
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

#include "mp_row_consumer.hh"
#include "column_translation.hh"
#include "concrete_types.hh"

namespace sstables {

atomic_cell make_counter_cell(api::timestamp_type timestamp, bytes_view value) {
    static constexpr size_t shard_size = 32;

    if (value.empty()) {
        // This will never happen in a correct MC sstable but
        // we had a bug #4363 that caused empty counters
        // to be incorrectly stored inside sstables.
        counter_cell_builder ccb;
        return ccb.build(timestamp);
    }

    data_input in(value);

    auto header_size = in.read<int16_t>();
    for (auto i = 0; i < header_size; i++) {
        auto idx = in.read<int16_t>();
        if (idx >= 0) {
            throw marshal_exception("encountered a local shard in a counter cell");
        }
    }
    auto header_length = (size_t(header_size) + 1) * sizeof(int16_t);
    auto shard_count = (value.size() - header_length) / shard_size;
    if (shard_count != size_t(header_size)) {
        throw marshal_exception("encountered remote shards in a counter cell");
    }

    counter_cell_builder ccb(shard_count);
    for (auto i = 0u; i < shard_count; i++) {
        auto id_hi = in.read<int64_t>();
        auto id_lo = in.read<int64_t>();
        auto clock = in.read<int64_t>();
        auto value = in.read<int64_t>();
        ccb.add_maybe_unsorted_shard(counter_shard(counter_id(utils::UUID(id_hi, id_lo)), value, clock));
    }
    ccb.sort_and_remove_duplicates();
    return ccb.build(timestamp);
}

// See #6130.
static data_type freeze_types_in_collections(data_type t) {
    return ::visit(*t, make_visitor(
    [] (const map_type_impl& typ) -> data_type {
        return map_type_impl::get_instance(
                freeze_types_in_collections(typ.get_keys_type()->freeze()),
                freeze_types_in_collections(typ.get_values_type()->freeze()),
                typ.is_multi_cell());
    },
    [] (const set_type_impl& typ) -> data_type {
        return set_type_impl::get_instance(
                freeze_types_in_collections(typ.get_elements_type()->freeze()),
                typ.is_multi_cell());
    },
    [] (const list_type_impl& typ) -> data_type {
        return list_type_impl::get_instance(
                freeze_types_in_collections(typ.get_elements_type()->freeze()),
                typ.is_multi_cell());
    },
    [&] (const abstract_type& typ) -> data_type {
        return std::move(t);
    }
    ));
}

/* If this function returns false, the caller cannot assume that the SSTable comes from Scylla.
 * It might, if for some reason a table was created using Scylla that didn't contain any feature bit,
 * but that should never happen. */
static bool is_certainly_scylla_sstable(const sstable_enabled_features& features) {
    return features.enabled_features;
}

std::vector<column_translation::column_info> column_translation::state::build(
        const schema& s,
        const utils::chunked_vector<serialization_header::column_desc>& src,
        const sstable_enabled_features& features,
        bool is_static) {
    std::vector<column_info> cols;
    if (s.is_dense()) {
        const column_definition& col = is_static ? *s.static_begin() : *s.regular_begin();
        cols.push_back(column_info{
            &col.name(),
            col.type,
            col.id,
            col.type->value_length_if_fixed(),
            col.is_multi_cell(),
            col.is_counter(),
            false
        });
    } else {
        cols.reserve(src.size());
        for (auto&& desc : src) {
            const bytes& type_name = desc.type_name.value;
            data_type type = db::marshal::type_parser::parse(to_sstring_view(type_name));
            if (!features.is_enabled(CorrectUDTsInCollections) && is_certainly_scylla_sstable(features)) {
                // See #6130.
                type = freeze_types_in_collections(std::move(type));
            }
            const column_definition* def = s.get_column_definition(desc.name.value);
            std::optional<column_id> id;
            bool schema_mismatch = false;
            if (def) {
                id = def->id;
                schema_mismatch = def->is_multi_cell() != type->is_multi_cell() ||
                                  def->is_counter() != type->is_counter() ||
                                  !def->type->is_value_compatible_with(*type);
            }
            cols.push_back(column_info{
                &desc.name.value,
                type,
                id,
                type->value_length_if_fixed(),
                type->is_multi_cell(),
                type->is_counter(),
                schema_mismatch
            });
        }
        boost::range::stable_partition(cols, [](const column_info& column) { return !column.is_collection; });
    }
    return cols;
}

}
