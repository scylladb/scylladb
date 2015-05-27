/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "query-result-reader.hh"

#include "core/shared_ptr.hh"

#include <experimental/optional>
#include <stdexcept>

#include <boost/any.hpp>

namespace query {

class no_such_column : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

class null_column_value : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

// Result set row is a set of cells that are associated with a row
// including regular column cells, partition keys, as well as static values.
class result_set_row {
    schema_ptr _schema;
    std::unordered_map<sstring, data_value> _cells;
public:
    result_set_row(schema_ptr schema, std::unordered_map<sstring, data_value>&& cells)
        : _schema{schema}
        , _cells{std::move(cells)}
    { }
    // Look up a deserialized row cell value by column name.
    template<typename T>
    std::experimental::optional<T>
    get(const sstring& column_name) const throw (no_such_column) {
        auto it = _cells.find(column_name);
        if (it == _cells.end()) {
            throw no_such_column(column_name);
        }
        auto&& value = it->second.value();
        if (value.empty()) {
            return std::experimental::nullopt;
        }
        return std::experimental::optional<T>{boost::any_cast<T>(value)};
    }
    template<typename T>
    T get_nonnull(const sstring& column_name) const throw (no_such_column, null_column_value) {
        auto v = get<T>(column_name);
        if (v) {
            return *v;
        }
        throw null_column_value(column_name);
    }
    friend inline bool operator==(const result_set_row& x, const result_set_row& y);
    friend inline bool operator!=(const result_set_row& x, const result_set_row& y);
};

inline bool operator==(const result_set_row& x, const result_set_row& y) {
    return x._schema == y._schema && x._cells == y._cells;
}

inline bool operator!=(const result_set_row& x, const result_set_row& y) {
    return !(x == y);
}

// Result set is an in-memory representation of query results in
// deserialized format. To obtain a result set, use the result_set_builder
// class as a visitor to query_result::consume() function.
class result_set {
    std::vector<result_set_row> _rows;
public:
    result_set(const std::vector<result_set_row>& rows)
        : _rows{std::move(rows)}
    { }
    bool empty() const {
        return _rows.empty();
    }
    const result_set_row& row(size_t idx) const throw (std::out_of_range) {
        if (idx >= _rows.size()) {
            throw std::out_of_range("no such row in result set: " + std::to_string(idx));
        }
        return _rows[idx];
    }
    friend inline bool operator==(const result_set& x, const result_set& y);
};

inline bool operator==(const result_set& x, const result_set& y) {
    return x._rows == y._rows;
}

// Result set builder is passed as a visitor to query_result::consume()
// function. You can call the build() method to obtain a result set that
// contains cells from the visited results.
class result_set_builder {
    schema_ptr _schema;
    std::vector<result_set_row> _rows;
    std::unordered_map<sstring, data_value> _pkey_cells;
public:
    result_set_builder(schema_ptr schema);
    lw_shared_ptr<result_set> build() const;
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

}
