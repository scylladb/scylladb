/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once


#include "core/shared_ptr.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "schema.hh"
#include "mutation.hh"

#include <experimental/optional>
#include <stdexcept>

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
    bool has(const sstring& column_name) const {
        return _cells.count(column_name) > 0;
    }
    // Look up a deserialized row cell value by column name; throws no_such_column on error
    const data_value&
    get_data_value(const sstring& column_name) const {
        auto it = _cells.find(column_name);
        if (it == _cells.end()) {
            throw no_such_column(column_name);
        }
        return it->second;
    }
    // Look up a deserialized row cell value by column name; throws no_such_column on error.
    template<typename T>
    std::experimental::optional<T>
    get(const sstring& column_name) const {
        auto&& value = get_data_value(column_name);
        if (value.is_null()) {
            return std::experimental::nullopt;
        }
        return std::experimental::optional<T>{value_cast<T>(value)};
    }
    // throws no_such_column or null_column_value on error
    template<typename T>
    T get_nonnull(const sstring& column_name) const {
        auto v = get<T>(column_name);
        if (v) {
            return *v;
        }
        throw null_column_value(column_name);
    }
    const std::unordered_map<sstring, data_value>& cells() const { return _cells; }
    friend inline bool operator==(const result_set_row& x, const result_set_row& y);
    friend inline bool operator!=(const result_set_row& x, const result_set_row& y);
    friend std::ostream& operator<<(std::ostream& out, const result_set_row& row);
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
    schema_ptr _schema;
    std::vector<result_set_row> _rows;
public:
    static result_set from_raw_result(schema_ptr, const partition_slice&, const result&);
    result_set(schema_ptr s, const std::vector<result_set_row>& rows)
        : _schema(std::move(s)), _rows{std::move(rows)}
    { }
    explicit result_set(const mutation&);
    bool empty() const {
        return _rows.empty();
    }
    // throws std::out_of_range on error
    const result_set_row& row(size_t idx) const {
        if (idx >= _rows.size()) {
            throw std::out_of_range("no such row in result set: " + std::to_string(idx));
        }
        return _rows[idx];
    }
    const std::vector<result_set_row>& rows() const {
        return _rows;
    }
    const schema_ptr& schema() const {
        return _schema;
    }
    friend inline bool operator==(const result_set& x, const result_set& y);
    friend std::ostream& operator<<(std::ostream& out, const result_set& rs);
};

inline bool operator==(const result_set& x, const result_set& y) {
    return x._rows == y._rows;
}

}
