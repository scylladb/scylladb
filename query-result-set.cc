/*
 * Copyright 2015 Cloudius Systems
 */

#include "query-result-set.hh"
#include "query-result-reader.hh"

namespace query {

// Result set builder is passed as a visitor to query_result::consume()
// function. You can call the build() method to obtain a result set that
// contains cells from the visited results.
class result_set_builder {
    schema_ptr _schema;
    std::vector<result_set_row> _rows;
    std::unordered_map<sstring, data_value> _pkey_cells;
public:
    result_set_builder(schema_ptr schema);
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
        auto&& value = cell.second.value();
        out << cell.first << "=\"" << type->decompose(value) << "\" ";
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const result_set& rs) {
    for (auto&& row : rs._rows) {
        out << row << std::endl;
    }
    return out;
}

result_set_builder::result_set_builder(schema_ptr schema)
    : _schema{schema}
{ }

result_set result_set_builder::build() const {
    return { _rows };
}

void result_set_builder::accept_new_partition(const partition_key& key, uint32_t row_count)
{
    _pkey_cells = deserialize(key);
}

void result_set_builder::accept_new_partition(uint32_t row_count)
{
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
    auto columns = is_static ? _schema->static_columns() : _schema->regular_columns();
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
                cells.emplace(col.name_as_text(), ctype->deserialize_value(view.data, serialization_format::internal()));
            }
        }
    }
    return cells;
}

result_set
result_set::from_raw_result(schema_ptr s, const partition_slice& slice, const result& r) {
    auto make = [&slice, s = std::move(s)] (bytes_view v) mutable {
        result_set_builder builder{std::move(s)};
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
