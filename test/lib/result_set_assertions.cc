/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>

#include "test/lib/result_set_assertions.hh"

static inline
sstring to_sstring(const bytes& b) {
    return sstring(b.begin(), b.end());
}

bool
row_assertion::matches(const query::result_set_row& row) const {
    for (auto&& column_and_value : _expected_values) {
        auto&& name = column_and_value.first;
        auto&& value = column_and_value.second;

        // FIXME: result_set_row works on sstring column names instead of more general "bytes".
        auto ss_name = to_sstring(name);

        const data_value* val = row.get_data_value(ss_name);
        if (val == nullptr) {
            if (!value.is_null()) {
                return false;
            }
        } else {
            if (*val != value) {
                return false;
            }
        }
    }
    if (_only_that) {
        for (auto&& e : row.cells()) {
            auto name = to_bytes(e.first);
            if (!_expected_values.contains(name)) {
                return false;
            }
        }
    }
    return true;
}

sstring
row_assertion::describe(schema_ptr schema) const {
    return format("{{{}}}", fmt::join(_expected_values | boost::adaptors::transformed([&schema] (auto&& e) {
        auto&& name = e.first;
        auto&& value = e.second;
        const column_definition* def = schema->get_column_definition(name);
        if (!def) {
            BOOST_FAIL(format("Schema is missing column definition for '{}'", name));
        }
        if (value.is_null()) {
            return format("{}=null", to_sstring(name));
        } else {
            return format("{}=\"{}\"", to_sstring(name), def->type->to_string(def->type->decompose(value)));
        }
    }), ", "));
}

const result_set_assertions&
result_set_assertions::has(const row_assertion& ra) const {
    for (auto&& row : _rs.rows()) {
        if (ra.matches(row)) {
            return *this;
        }
    }
    BOOST_FAIL(format("Row {} not found in {}", ra.describe(_rs.schema()), _rs));
    return *this;
}

const result_set_assertions&
result_set_assertions::has_only(const row_assertion& ra) const {
    BOOST_REQUIRE(_rs.rows().size() == 1);
    auto& row = _rs.rows()[0];
    if (!ra.matches(row)) {
        BOOST_FAIL(format("Expected {} but got {}", ra.describe(_rs.schema()), row));
    }
    return *this;
}

const result_set_assertions&
result_set_assertions::is_empty() const {
    BOOST_REQUIRE_EQUAL(_rs.rows().size(), 0);
    return *this;
}

const result_set_assertions&
result_set_assertions::has_size(int row_count) const {
    BOOST_REQUIRE_EQUAL(_rs.rows().size(), row_count);
    return *this;
}
