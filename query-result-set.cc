/*
 * Copyright 2015 Cloudius Systems
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

namespace query {

// Result set builder is passed as a visitor to query_result::consume()
// function. You can call the build() method to obtain a result set that
// contains cells from the visited results.
class result_set_builder {
    schema_ptr _schema;
    const partition_slice& _slice;
    std::vector<result_set_row> _rows;
    std::unordered_map<sstring, data_value> _pkey_cells;
    uint32_t _row_count;
public:
    // Keep slice live as long as the builder is used.
    result_set_builder(schema_ptr schema, const partition_slice& slice);
    result_set build() const;
    void accept_new_partition(const partition_key& key, uint32_t row_count);
    void accept_new_partition(uint32_t row_count);
    void accept_new_row(const clustering_key& key, const result_row_view& static_row, const result_row_view& row);
    void accept_new_row(const result_row_view &static_row, const result_row_view &row);
    void accept_partition_end(const result_row_view& static_row);
private:
    std::unordered_map<sstring, data_value> deserialize(const partition_key& key);
    std::unordered_map<sstring, data_value> deserialize(const clustering_key& key);
    std::unordered_map<sstring, data_value> deserialize(const result_row_view& row, bool is_static);
};

std::ostream& operator<<(std::ostream& out, const result_set_row& row) {
    for (auto&& cell : row._cells) {
        auto&& type = cell.second.type();
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

result_set_builder::result_set_builder(schema_ptr schema, const partition_slice& slice)
    : _schema{schema}, _slice(slice)
{ }

result_set result_set_builder::build() const {
    return { _schema, _rows };
}

void result_set_builder::accept_new_partition(const partition_key& key, uint32_t row_count)
{
    _pkey_cells = deserialize(key);
    accept_new_partition(row_count);
}

void result_set_builder::accept_new_partition(uint32_t row_count)
{
    _row_count = row_count;
}

void result_set_builder::accept_new_row(const clustering_key& key, const result_row_view& static_row, const result_row_view& row)
{
    auto ckey_cells = deserialize(key);
    auto static_cells = deserialize(static_row, true);
    auto regular_cells = deserialize(row, false);

    std::unordered_map<sstring, data_value> cells;
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

    std::unordered_map<sstring, data_value> cells;
    cells.insert(_pkey_cells.begin(), _pkey_cells.end());
    cells.insert(static_cells.begin(), static_cells.end());
    cells.insert(regular_cells.begin(), regular_cells.end());
    _rows.emplace_back(_schema, std::move(cells));
}

void result_set_builder::accept_partition_end(const result_row_view& static_row)
{
    if (_row_count == 0) {
        auto static_cells = deserialize(static_row, true);
        std::unordered_map<sstring, data_value> cells;
        cells.insert(_pkey_cells.begin(), _pkey_cells.end());
        cells.insert(static_cells.begin(), static_cells.end());
        _rows.emplace_back(_schema, std::move(cells));
    }
    _pkey_cells.clear();
}

std::unordered_map<sstring, data_value>
result_set_builder::deserialize(const partition_key& key)
{
    std::unordered_map<sstring, data_value> cells;
    auto i = key.begin(*_schema);
    for (auto&& col : _schema->partition_key_columns()) {
        cells.emplace(col.name_as_text(), col.type->deserialize_value(*i));
        ++i;
    }
    return cells;
}

std::unordered_map<sstring, data_value>
result_set_builder::deserialize(const clustering_key& key)
{
    std::unordered_map<sstring, data_value> cells;
    auto i = key.begin(*_schema);
    for (auto&& col : _schema->clustering_key_columns()) {
        cells.emplace(col.name_as_text(), col.type->deserialize_value(*i));
        ++i;
    }
    return cells;
}

std::unordered_map<sstring, data_value>
result_set_builder::deserialize(const result_row_view& row, bool is_static)
{
    std::unordered_map<sstring, data_value> cells;
    auto i = row.iterator();
    auto column_ids = is_static ? _slice.static_columns : _slice.regular_columns;
    auto columns = column_ids | boost::adaptors::transformed([this, is_static] (column_id id) -> const column_definition& {
        if (is_static) {
            return _schema->static_column_at(id);
        } else {
            return _schema->regular_column_at(id);
        }
    });
    for (auto &&col : columns) {
        if (col.is_atomic()) {
            auto cell = i.next_atomic_cell();
            if (cell) {
                auto view = cell.value();
                cells.emplace(col.name_as_text(), col.type->deserialize_value(view.value()));
            }
        } else {
            auto cell = i.next_collection_cell();
            if (cell) {
                auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
                auto view = cell.value();
                auto normal_form = ctype->to_value(ctype->deserialize_mutation_form(view), serialization_format::internal());
                cells.emplace(col.name_as_text(), col.type->deserialize_value(normal_form));
            }
        }
    }
    return cells;
}

result_set
result_set::from_raw_result(schema_ptr s, const partition_slice& slice, const result& r) {
    auto make = [&slice, s = std::move(s)] (bytes_view v) mutable {
        result_set_builder builder{std::move(s), slice};
        result_view view(v);
        view.consume(slice, builder);
        return builder.build();
    };

    if (r.buf().is_linearized()) {
        return make(r.buf().view());
    } else {
        // FIXME: make result_view::consume() work on fragments to avoid linearization.
        bytes_ostream w(r.buf());
        return make(w.linearize());
    }
}

}
