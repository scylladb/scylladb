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

#include <map>

#include "query-result-set.hh"

//
// Contains assertions for query::result_set objects
//
// Example use:
//
//  assert_that(rs)
//     .has(a_row().with_column("column_name", "value"));
//

class row_assertion {
    std::map<bytes, data_value> _expected_values;
    bool _only_that = false;
public:
    row_assertion& with_column(bytes name, data_value value) {
        _expected_values.emplace(name, value);
        return *this;
    }
    row_assertion& and_only_that() {
        _only_that = true;
        return *this;
    }
private:
    friend class result_set_assertions;
    bool matches(const query::result_set_row& row) const;
    sstring describe(schema_ptr s) const;
};

inline
row_assertion a_row() {
    return {};
}

class result_set_assertions {
    const query::result_set& _rs;
public:
    result_set_assertions(const query::result_set& rs) : _rs(rs) { }
    const result_set_assertions& has(const row_assertion& ra) const;
    const result_set_assertions& has_only(const row_assertion& ra) const;
    const result_set_assertions& is_empty() const;
    const result_set_assertions& has_size(int row_count) const;
};

// Make rs live as long as the returned assertion object is used
inline
result_set_assertions assert_that(const query::result_set& rs) {
    return { rs };
}
