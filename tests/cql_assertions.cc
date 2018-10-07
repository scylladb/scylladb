
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

#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "cql_assertions.hh"
#include "transport/messages/result_message.hh"
#include "to_string.hh"
#include "bytes.hh"

static inline void fail(sstring msg) {
    throw std::runtime_error(msg);
}

rows_assertions::rows_assertions(shared_ptr<cql_transport::messages::result_message::rows> rows)
    : _rows(rows)
{ }

rows_assertions
rows_assertions::with_size(size_t size) {
    auto rs = _rows->rs().result_set();
    auto row_count = rs.size();
    if (row_count != size) {
        fail(sprint("Expected %d row(s) but got %d", size, row_count));
    }
    return {*this};
}

rows_assertions
rows_assertions::is_empty() {
    auto rs = _rows->rs().result_set();
    auto row_count = rs.size();
    if (row_count != 0) {
        auto&& first_row = *rs.rows().begin();
        fail(sprint("Expected no rows, but got %d. First row: %s", row_count, to_string(first_row)));
    }
    return {*this};
}

rows_assertions
rows_assertions::is_not_empty() {
    auto rs = _rows->rs().result_set();
    auto row_count = rs.size();
    if (row_count == 0) {
        fail("Expected some rows, but was result was empty");
    }
    return {*this};
}

rows_assertions
rows_assertions::with_row(std::initializer_list<bytes_opt> values) {
    auto rs = _rows->rs().result_set();
    std::vector<bytes_opt> expected_row(values);
    for (auto&& row : rs.rows()) {
        if (row == expected_row) {
            return {*this};
        }
    }
    fail(sprint("Expected row not found: %s not in %s\n", to_string(expected_row), _rows));
    return {*this};
}

// Verifies that the result has the following rows and only that rows, in that order.
rows_assertions
rows_assertions::with_rows(std::initializer_list<std::initializer_list<bytes_opt>> rows) {
    auto rs = _rows->rs().result_set();
    auto actual_i = rs.rows().begin();
    auto actual_end = rs.rows().end();
    int row_nr = 0;
    for (auto&& row : rows) {
        if (actual_i == actual_end) {
            fail(sprint("Expected more rows (%d), got %d", rows.size(), rs.size()));
        }
        auto& actual = *actual_i;
        if (!std::equal(
            std::begin(row), std::end(row),
            std::begin(actual), std::end(actual))) {
            fail(sprint("row %d differs, expected %s got %s", row_nr, to_string(row), to_string(actual)));
        }
        ++actual_i;
        ++row_nr;
    }
    if (actual_i != actual_end) {
        fail(sprint("Expected less rows (%d), got %d. Next row is: %s", rows.size(), rs.size(),
                    to_string(*actual_i)));
    }
    return {*this};
}

// Verifies that the result has the following rows and only those rows.
rows_assertions
rows_assertions::with_rows_ignore_order(std::vector<std::vector<bytes_opt>> rows) {
    auto rs = _rows->rs().result_set();
    auto& actual = rs.rows();
    for (auto&& expected : rows) {
        auto found = std::find_if(std::begin(actual), std::end(actual), [&] (auto&& row) {
            return std::equal(
                    std::begin(row), std::end(row),
                    std::begin(expected), std::end(expected));
        });
        if (found == std::end(actual)) {
            fail(sprint("row %s not found in result set (%s)", to_string(expected),
               ::join(", ", actual | boost::adaptors::transformed([] (auto& r) { return to_string(r); }))));
        }
    }
    if (rs.size() != rows.size()) {
        fail(sprint("Expected more rows (%d), got %d", rs.size(), rows.size()));
    }
    return {*this};
}

result_msg_assertions::result_msg_assertions(shared_ptr<cql_transport::messages::result_message> msg)
    : _msg(msg)
{ }

rows_assertions result_msg_assertions::is_rows() {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(_msg);
    if (!rows) {
        fail("Expected rows in result set");
    }
    return rows_assertions(rows);
}

result_msg_assertions assert_that(shared_ptr<cql_transport::messages::result_message> msg) {
    return result_msg_assertions(msg);
}

rows_assertions rows_assertions::with_serialized_columns_count(size_t columns_count) {
    size_t serialized_column_count = _rows->rs().get_metadata().column_count();
    if (serialized_column_count != columns_count) {
        fail(sprint("Expected %d serialized columns(s) but got %d", columns_count, serialized_column_count));
    }
    return {*this};
}
