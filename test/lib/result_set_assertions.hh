/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
