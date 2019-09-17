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
 * Copyright (C) 2015 ScyllaDB
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

const update_parameters::prefetch_data::cell_list*
update_parameters::get_prefetched_list(
    partition_key_view pkey,
    clustering_key_view ckey,
    const column_definition& column) const
{
    if (_prefetched.rows.empty()) {
        return {};
    }

    if (column.is_static()) {
        ckey = clustering_key_view::make_empty();
    }
    auto i = _prefetched.rows.find(std::make_pair(std::move(pkey), std::move(ckey)));
    if (i == _prefetched.rows.end()) {
        return {};
    }

    auto&& row = i->second;
    auto j = row.find(column.id);
    if (j == row.end()) {
        return {};
    }
    return &j->second;
}

update_parameters::prefetch_data::prefetch_data(schema_ptr schema)
    : rows(8, key_hashing(*schema), key_equality(*schema))
    , schema(schema)
{ }

// Implements ResultVisitor concept from query.hh
class prefetch_data_builder {
    update_parameters::prefetch_data& _data;
    const query::partition_slice& _ps;
    schema_ptr _schema;
    std::optional<partition_key> _pkey;
private:
    void add_cell(update_parameters::prefetch_data::row& cells, const column_definition& def, const std::optional<query::result_bytes_view>& cell) {
        if (cell) {
            auto ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (!ctype->is_multi_cell()) {
                throw std::logic_error(format("cannot prefetch frozen collection: {}", def.name_as_text()));
            }
            auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
            update_parameters::prefetch_data::cell_list list;
            // FIXME: Iterate over a range instead of fully exploded collection
          cell->with_linearized([&] (bytes_view cell_view) {
            auto dv = map_type->deserialize(cell_view);
            for (auto&& el : value_cast<map_type_impl::native_type>(dv)) {
                list.emplace_back(update_parameters::prefetch_data::cell{el.first.serialize(), el.second.serialize()});
            }
            cells.emplace(def.id, std::move(list));
          });
        }
    };
public:
    prefetch_data_builder(schema_ptr s, update_parameters::prefetch_data& data, const query::partition_slice& ps)
        : _data(data)
        , _ps(ps)
        , _schema(std::move(s))
    { }

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        _pkey = key;
    }

    void accept_new_partition(uint32_t row_count) {
        assert(0);
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row,
                    const query::result_row_view& row) {
        update_parameters::prefetch_data::row cells;

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
        update_parameters::prefetch_data::row cells;

        auto static_row_iterator = static_row.iterator();
        for (auto&& id : _ps.static_columns) {
            add_cell(cells, _schema->static_column_at(id), static_row_iterator.next_collection_cell());
        }

        _data.rows.emplace(std::make_pair(*_pkey, clustering_key_prefix::make_empty()), std::move(cells));
    }
};

update_parameters::prefetch_data update_parameters::build_prefetch_data(schema_ptr schema, const query::result& query_result,
            const query::partition_slice& slice) {

    update_parameters::prefetch_data rows(schema);
    query::result_view::consume(query_result, slice, prefetch_data_builder(schema, rows, slice));
    return rows;
}

}
