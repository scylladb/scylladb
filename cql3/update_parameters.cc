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

#include "cql3/update_parameters.hh"
#include "query-result-reader.hh"
#include "types/map.hh"

namespace cql3 {

const std::vector<std::pair<data_value, data_value>> *
update_parameters::get_prefetched_list(const partition_key& pkey, const clustering_key& ckey, const column_definition& column) const {

    auto row = _prefetched.find_row(pkey, column.is_static() ? clustering_key::make_empty() : ckey);

    if (row == nullptr) {
        return nullptr;
    }

    auto j = row->cells.find(column.ordinal_id);
    if (j == row->cells.end()) {
        return nullptr;
    }
    const data_value& cell = j->second;
    // Ensured by collections_as_maps flag in read_command flags
    assert(cell.type()->is_map());
    const map_type_impl& map_type = static_cast<const map_type_impl&>(*cell.type());
    return &map_type.from_value(cell);
}

update_parameters::prefetch_data::prefetch_data(schema_ptr schema)
    : rows(key_less{*schema})
    , schema(schema)
{ }


const update_parameters::prefetch_data::row*
update_parameters::prefetch_data::find_row(const partition_key& pkey, const clustering_key& ckey) const {

    // If clustering key is empty, find the first matching row of the partition defined by the partition key.
    key_view key{pkey, ckey};
    if (ckey.is_empty()) {
        const auto it = rows.lower_bound(key);
        if (it != rows.end() && rows.key_comp().pk_cmp(it->first.first, pkey) == 0) {
            return &it->second;
        }
        return nullptr;
    }
    const auto it = rows.find(key);
    return it == rows.end() ? nullptr : &it->second;
}

// Implements ResultVisitor concept from query.hh
class prefetch_data_builder {
    update_parameters::prefetch_data& _data;
    const query::partition_slice& _ps;
    schema_ptr _schema;
    std::optional<partition_key> _pkey;
    // Number of regular rows in the current partition
    uint64_t _row_count;

    // Add partition key columns to the current full row
    void add_partition_key(update_parameters::prefetch_data::row& cells, const partition_key& key)
    {
        auto i = key.begin(*_schema);
        for (auto&& col : _schema->partition_key_columns()) {
            cells.cells.emplace(col.ordinal_id, col.type->deserialize_value(*i));
            ++i;
        }
    }

    // Add clustering key columns to the current full row
    void add_clustering_key(update_parameters::prefetch_data::row& cells, const clustering_key& key)
    {
        auto i = key.begin(*_schema);
        for (auto&& col : _schema->clustering_key_columns()) {
            if (i == key.end(*_schema)) {
                break;
            }
            cells.cells.emplace(col.ordinal_id, col.type->deserialize_value(*i));
            ++i;
        }
    }

    // Add a prefetched cell to the current full row
    void add_cell(update_parameters::prefetch_data::row& cells, const column_definition& def,
            const std::optional<query::result_bytes_view>& cell) {

        if (cell == std::nullopt) {
            return;
        }
        auto type = def.type;
        // We use collections_as_maps flag, so set/list type is map, reconstruct the
        // data type used for serialization.
        if (type->is_listlike() && type->is_multi_cell()) {
            auto ctype = static_pointer_cast<const collection_type_impl>(type);
            type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        }
        cells.cells.emplace(def.ordinal_id, type->deserialize(*cell));
    };
public:
    prefetch_data_builder(schema_ptr s, update_parameters::prefetch_data& data, const query::partition_slice& ps)
        : _data(data)
        , _ps(ps)
        , _schema(std::move(s))
        , _row_count(0)
    { }

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _pkey = key;
        _row_count = row_count;
    }

    void accept_new_partition(uint64_t row_count) {
        assert(0);
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row,
                    const query::result_row_view& row) {
        update_parameters::prefetch_data::row cells;

        add_partition_key(cells, *_pkey);
        add_clustering_key(cells, key);
        auto static_row_iterator = static_row.iterator();
        for (auto&& id : _ps.static_columns) {
            add_cell(cells, _schema->static_column_at(id), static_row_iterator.next_collection_cell());
        }
        auto row_iterator = row.iterator();
        for (auto&& id : _ps.regular_columns) {
            add_cell(cells, _schema->regular_column_at(id), row_iterator.next_collection_cell());
        }

        _data.rows.emplace(std::make_pair(*_pkey, key), std::move(cells));
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        assert(0);
    }

    void accept_partition_end(const query::result_row_view& static_row) {

        if (_row_count > 0) {
            // Static row cells have been added into every regular row.
            return;
        }
        // The partition contains only a static row. Add it.

        update_parameters::prefetch_data::row cells;
        add_partition_key(cells, *_pkey);

        auto static_row_iterator = static_row.iterator();
        for (auto&& id : _ps.static_columns) {
            add_cell(cells, _schema->static_column_at(id), static_row_iterator.next_collection_cell());
        }
        // There are no regular rows in the partition. Use empty clustering key prefix to create
        // a row with static columns.
        // _row_count > 0 check above prevents entering this branch for tables with no clustering
        // key (and thus no static row), but even if it didn't, emplace() for such a table would do
        // nothing, since an element with the same key (partition key, empty clustering key, cells)
        // has already been added to the map by accept_new_row(), called earlier on the same
        // partition key.
        _data.rows.emplace(std::make_pair(*_pkey, clustering_key_prefix::make_empty()), std::move(cells));
    }
};

update_parameters::prefetch_data update_parameters::build_prefetch_data(schema_ptr schema, const query::result& query_result,
            const query::partition_slice& slice) {

    update_parameters::prefetch_data rows(schema);
    query::result_view::consume(query_result, slice, prefetch_data_builder(schema, rows, slice));
    return rows;
}

} // end of namespace cql3
