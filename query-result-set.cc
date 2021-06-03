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

#include "query-result-set.hh"
#include "query-result-reader.hh"
#include "partition_slice_builder.hh"
#include "mutation.hh"
#include "types/map.hh"
#include "utils/exceptions.hh"
#include "mutation_query.hh"

#include <fmt/format.h>

namespace query {

class deserialization_error : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

// Result set builder is passed as a visitor to query_result::consume()
// function. You can call the build() method to obtain a result set that
// contains cells from the visited results.
class result_set_builder {
    schema_ptr _schema;
    const partition_slice& _slice;
    std::vector<result_set_row> _rows;
    std::unordered_map<sstring, non_null_data_value> _pkey_cells;
    uint64_t _row_count;
public:
    // Keep slice live as long as the builder is used.
    result_set_builder(schema_ptr schema, const partition_slice& slice);
    result_set build();
    void accept_new_partition(const partition_key& key, uint64_t row_count);
    void accept_new_partition(uint64_t row_count);
    void accept_new_row(const clustering_key& key, const result_row_view& static_row, const result_row_view& row);
    void accept_new_row(const result_row_view &static_row, const result_row_view &row);
    void accept_partition_end(const result_row_view& static_row);
private:
    std::unordered_map<sstring, non_null_data_value> deserialize(const partition_key& key);
    std::unordered_map<sstring, non_null_data_value> deserialize(const clustering_key& key);
    std::unordered_map<sstring, non_null_data_value> deserialize(const result_row_view& row, bool is_static);
};

std::ostream& operator<<(std::ostream& out, const result_set_row& row) {
    for (auto&& cell : row._cells) {
        auto&& type = static_cast<const data_value&>(cell.second).type();
        auto&& value = cell.second;
        out << cell.first << "=\"" << type->to_string(type->decompose(value)) << "\" ";
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const result_set& rs) {
    for (auto&& row : rs._rows) {
        out << row << std::endl;
    }
    return out;
}

static logging::logger query_result_log("query result log");

non_null_data_value::non_null_data_value(data_value&& v) : _v(std::move(v)) {
    if (_v.is_null()) {
        on_internal_error(query_result_log, "Trying to add a null data_value to a result_set_row");
    }
}

result_set_builder::result_set_builder(schema_ptr schema, const partition_slice& slice)
    : _schema{schema}, _slice(slice)
{ }

result_set result_set_builder::build() {
    return { _schema, std::move(_rows) };
}

void result_set_builder::accept_new_partition(const partition_key& key, uint64_t row_count)
{
    _pkey_cells = deserialize(key);
    accept_new_partition(row_count);
}

void result_set_builder::accept_new_partition(uint64_t row_count)
{
    _row_count = row_count;
}

void result_set_builder::accept_new_row(const clustering_key& key, const result_row_view& static_row, const result_row_view& row)
{
    auto ckey_cells = deserialize(key);
    auto static_cells = deserialize(static_row, true);
    auto regular_cells = deserialize(row, false);

    std::unordered_map<sstring, non_null_data_value> cells;
    cells.insert(_pkey_cells.begin(), _pkey_cells.end());
    cells.insert(ckey_cells.begin(), ckey_cells.end());
    cells.insert(static_cells.begin(), static_cells.end());
    cells.insert(regular_cells.begin(), regular_cells.end());
    _rows.emplace_back(_schema, std::move(cells));
}

void result_set_builder::accept_new_row(const query::result_row_view &static_row, const query::result_row_view &row)
{
    auto static_cells = deserialize(static_row, true);
    auto regular_cells = deserialize(row, false);

    std::unordered_map<sstring, non_null_data_value> cells;
    cells.insert(_pkey_cells.begin(), _pkey_cells.end());
    cells.insert(static_cells.begin(), static_cells.end());
    cells.insert(regular_cells.begin(), regular_cells.end());
    _rows.emplace_back(_schema, std::move(cells));
}

void result_set_builder::accept_partition_end(const result_row_view& static_row)
{
    if (_row_count == 0) {
        auto static_cells = deserialize(static_row, true);
        std::unordered_map<sstring, non_null_data_value> cells;
        cells.insert(_pkey_cells.begin(), _pkey_cells.end());
        cells.insert(static_cells.begin(), static_cells.end());
        _rows.emplace_back(_schema, std::move(cells));
    }
    _pkey_cells.clear();
}

std::unordered_map<sstring, non_null_data_value>
result_set_builder::deserialize(const partition_key& key)
{
    std::unordered_map<sstring, non_null_data_value> cells;
    auto i = key.begin(*_schema);
    for (auto&& col : _schema->partition_key_columns()) {
        cells.emplace(col.name_as_text(), col.type->deserialize_value(*i));
        ++i;
    }
    return cells;
}

std::unordered_map<sstring, non_null_data_value>
result_set_builder::deserialize(const clustering_key& key)
{
    std::unordered_map<sstring, non_null_data_value> cells;
    auto i = key.begin(*_schema);
    for (auto&& col : _schema->clustering_key_columns()) {
        if (i == key.end(*_schema)) {
            break;
        }
        cells.emplace(col.name_as_text(), col.type->deserialize_value(*i));
        ++i;
    }
    return cells;
}

std::unordered_map<sstring, non_null_data_value>
result_set_builder::deserialize(const result_row_view& row, bool is_static)
{
    std::unordered_map<sstring, non_null_data_value> cells;
    auto i = row.iterator();
    auto column_ids = is_static ? _slice.static_columns : _slice.regular_columns;
    auto columns = column_ids | boost::adaptors::transformed([this, is_static] (column_id id) -> const column_definition& {
        if (is_static) {
            return _schema->static_column_at(id);
        } else {
            return _schema->regular_column_at(id);
        }
    });
    size_t index = 0;
    for (auto &&col : columns) {
      try {
        if (col.is_atomic()) {
            auto cell = i.next_atomic_cell();
            if (cell) {
                cells.emplace(col.name_as_text(), col.type->deserialize_value(cell->value()));
            }
        } else {
            auto cell = i.next_collection_cell();
            if (cell) {
                    if (col.type->is_collection()) {
                        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
                        if (_slice.options.contains<partition_slice::option::collections_as_maps>()) {
                            ctype = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
                        }

                        cells.emplace(col.name_as_text(), ctype->deserialize_value(*cell, _slice.cql_format()));
                    } else {
                        cells.emplace(col.name_as_text(), col.type->deserialize_value(*cell));
                    }
            }
        }
        index++;
      } catch (...) {
            throw deserialization_error(fmt::format(FMT_STRING("failed on column {}.{}#{} (version: {}, id: {}, index: {}, type: {}): {}"),
                _schema->ks_name(), _schema->cf_name(), col.name_as_text(), _schema->version(), col.id, index, col.type->name(), std::current_exception()));
      }
    }
    return cells;
}

result_set
result_set::from_raw_result(schema_ptr s, const partition_slice& slice, const result& r) {
    result_set_builder builder{std::move(s), slice};
    result_view::consume(r, slice, builder);
    return builder.build();
}

result_set::result_set(const mutation& m) : result_set([&m] {
    auto slice = partition_slice_builder(*m.schema()).build();
    auto qr = query_mutation(mutation(m), slice);
    return result_set::from_raw_result(m.schema(), slice, qr);
}())
{ }

}
