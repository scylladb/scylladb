/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once


#include <seastar/core/shared_ptr.hh>
#include <fmt/ostream.h>
#include "types/types.hh"
#include "schema/schema.hh"

#include <optional>
#include <stdexcept>

class mutation;

namespace query {

class result;

class no_value : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

class non_null_data_value {
    data_value _v;

public:
    explicit non_null_data_value(data_value&& v);
    operator const data_value&() const {
        return _v;
    }
};

inline bool operator==(const non_null_data_value& x, const non_null_data_value& y) {
    return static_cast<const data_value&>(x) == static_cast<const data_value&>(y);
}

// Result set row is a set of cells that are associated with a row
// including regular column cells, partition keys, as well as static values.
class result_set_row {
    schema_ptr _schema;
    const std::unordered_map<sstring, non_null_data_value> _cells;
public:
    result_set_row(schema_ptr schema, std::unordered_map<sstring, non_null_data_value>&& cells)
        : _schema{schema}
        , _cells{std::move(cells)}
    { }
    // Look up a deserialized row cell value by column name
    const data_value*
    get_data_value(const sstring& column_name) const {
        auto it = _cells.find(column_name);
        if (it == _cells.end()) {
            return nullptr;
        }
        return &static_cast<const data_value&>(it->second);
    }
    // Look up a deserialized row cell value by column name
    template<typename T>
    std::optional<T>
    get(const sstring& column_name) const {
        if (const auto *value = get_ptr<T>(column_name)) {
            return std::optional(*value);
        }
        return std::nullopt;
    }
    template<typename T>
    const T*
    get_ptr(const sstring& column_name) const {
        const auto *value = get_data_value(column_name);
        if (value == nullptr) {
            return nullptr;
        }
        return &value_cast<T>(*value);
    }
    // throws no_value on error
    template<typename T>
    const T& get_nonnull(const sstring& column_name) const {
        auto v = get_ptr<std::remove_reference_t<T>>(column_name);
        if (v) {
            return *v;
        }
        throw no_value(column_name);
    }
    const std::unordered_map<sstring, non_null_data_value>& cells() const { return _cells; }
    friend inline bool operator==(const result_set_row& x, const result_set_row& y) = default;
    friend std::ostream& operator<<(std::ostream& out, const result_set_row& row);
};

// Result set is an in-memory representation of query results in
// deserialized format. To obtain a result set, use the result_set_builder
// class as a visitor to query_result::consume() function.
class result_set {
    schema_ptr _schema;
    std::vector<result_set_row> _rows;
public:
    static result_set from_raw_result(schema_ptr, const partition_slice&, const result&);
    result_set(schema_ptr s, std::vector<result_set_row>&& rows)
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

template <> struct fmt::formatter<query::result_set> : fmt::ostream_formatter {};
template <> struct fmt::formatter<query::result_set_row> : fmt::ostream_formatter {};
