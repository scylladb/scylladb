
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <fmt/ranges.h>
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"
#include "transport/messages/result_message.hh"
#include "utils/assert.hh"
#include "utils/to_string.hh"
#include "bytes.hh"

static inline void fail(sstring msg) {
    throw std::runtime_error(msg);
}

rows_assertions::rows_assertions(shared_ptr<cql_transport::messages::result_message::rows> rows)
    : _rows(rows)
{ }

rows_assertions
rows_assertions::with_size(size_t size) {
    const auto& rs = _rows->rs().result_set();
    auto row_count = rs.size();
    if (row_count != size) {
        fail(format("Expected {:d} row(s) but got {:d}", size, row_count));
    }
    return {*this};
}

rows_assertions
rows_assertions::is_empty() {
    const auto& rs = _rows->rs().result_set();
    auto row_count = rs.size();
    if (row_count != 0) {
        auto&& first_row = *rs.rows().begin();
        fail(format("Expected no rows, but got {:d}. First row: {}", row_count, first_row));
    }
    return {*this};
}

rows_assertions
rows_assertions::is_not_empty() {
    const auto& rs = _rows->rs().result_set();
    auto row_count = rs.size();
    if (row_count == 0) {
        fail("Expected some rows, but was result was empty");
    }
    return {*this};
}

rows_assertions
rows_assertions::rows_assertions::is_null() {
    const auto& rs = _rows->rs().result_set();
    for (auto&& row : rs.rows()) {
        for (const managed_bytes_opt& v : row) {
            if (v) {
                fail(format("Expected null values. Found: {}\n", v));
            }
        }
    }
    return {*this};
}

rows_assertions
rows_assertions::rows_assertions::is_not_null() {
    const auto& rs = _rows->rs().result_set();
    for (auto&& row : rs.rows()) {
        for (const managed_bytes_opt& v : row) {
            if (!v) {
                fail(format("Expected non-null values. {}\n", fmt::to_string(row)));
            }
        }
    }
    return is_not_empty();
}

rows_assertions
rows_assertions::with_column_types(std::initializer_list<data_type> column_types) {
    auto meta = _rows->rs().result_set().get_metadata();
    const auto& columns = meta.get_names();
    if (column_types.size() != columns.size()) {
        fail(format("Expected {:d} columns, got {:d}", column_types.size(), meta.column_count()));
    }
    auto expected_it = column_types.begin();
    auto actual_it = columns.begin();
    for (int i = 0; i < (int)columns.size(); i++) {
        const auto& expected_type = *expected_it++;
        const auto& actual_spec = *actual_it++;
        if (expected_type != actual_spec->type) {
            fail(format("Column {:d}: expected type {}, got {}", i, expected_type->name(), actual_spec->type->name()));
        }
    }
    return {*this};
}

rows_assertions
rows_assertions::with_row(std::initializer_list<bytes_opt> values) {
    const auto& rs = _rows->rs().result_set();
    std::vector<managed_bytes_opt> expected_row = boost::copy_range<std::vector<managed_bytes_opt>>((values | boost::adaptors::transformed(to_managed_bytes_opt)));
    for (auto&& row : rs.rows()) {
        if (row == expected_row) {
            return {*this};
        }
    }
    fail(format("Expected row not found: {} not in {}\n", fmt::to_string(expected_row), _rows));
    return {*this};
}

// Verifies that the result has the following rows and only that rows, in that order.
rows_assertions
rows_assertions::with_rows(std::vector<std::vector<bytes_opt>> rows) {
    const auto& rs = _rows->rs().result_set();
    auto actual_i = rs.rows().begin();
    auto actual_end = rs.rows().end();
    int row_nr = 0;
    for (auto&& row : rows) {
        if (actual_i == actual_end) {
            fail(format("Expected more rows ({:d}), got {:d}", rows.size(), rs.size()));
        }
        auto& actual = *actual_i;
        auto expected_row = row | boost::adaptors::transformed(to_managed_bytes_opt);
        if (!std::equal(
            std::begin(expected_row), std::end(expected_row),
            std::begin(actual), std::end(actual))) {
            fail(format("row {:d} differs, expected {} got {}", row_nr, fmt::to_string(row), fmt::to_string(actual)));
        }
        ++actual_i;
        ++row_nr;
    }
    if (actual_i != actual_end) {
        fail(format("Expected less rows ({:d}), got {:d}. Next row is: {}", rows.size(), rs.size(),
                    fmt::to_string(*actual_i)));
    }
    return {*this};
}

// Verifies that the result has the following rows and only those rows.
rows_assertions
rows_assertions::with_rows_ignore_order(std::vector<std::vector<bytes_opt>> rows) {
    const auto& rs = _rows->rs().result_set();
    auto& actual = rs.rows();
    for (auto&& expected : rows) {
        auto expected_row = expected | boost::adaptors::transformed(to_managed_bytes_opt);
        auto found = std::find_if(std::begin(actual), std::end(actual), [&] (auto&& row) {
            return std::equal(
                    std::begin(row), std::end(row),
                    std::begin(expected_row), std::end(expected_row));
        });
        if (found == std::end(actual)) {
            fail(format("row {} not found in result set ({})", fmt::to_string(expected),
               fmt::join(actual | boost::adaptors::transformed([] (auto& r) { return fmt::to_string(r); }), ", ")));
        }
    }
    if (rs.size() != rows.size()) {
        fail(format("Expected different number of rows ({:d}), got {:d}", rows.size(), rs.size()));
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
        fail(format("Expected {:d} serialized columns(s) but got {:d}", columns_count, serialized_column_count));
    }
    return {*this};
}

shared_ptr<cql_transport::messages::result_message> cquery_nofail(
        cql_test_env& env, sstring_view query, std::unique_ptr<cql3::query_options>&& qo, const seastar::compat::source_location& loc) {
    try {
        if (qo) {
            return env.execute_cql(query, std::move(qo)).get();
        } else {
            return env.execute_cql(query).get();
        }
    } catch (...) {
        BOOST_FAIL(format("query '{}' failed: {}\n{}:{}: originally from here",
                          query, std::current_exception(), loc.file_name(), loc.line()));
    }
    return shared_ptr<cql_transport::messages::result_message>(nullptr);
}

void require_rows(cql_test_env& e,
                  sstring_view qstr,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const seastar::compat::source_location& loc) {
    try {
        assert_that(cquery_nofail(e, qstr, nullptr, loc)).is_rows().with_rows_ignore_order(expected);
    }
    catch (const std::exception& e) {
        BOOST_FAIL(format("query '{}' failed: {}\n{}:{}: originally from here",
                          qstr, e.what(), loc.file_name(), loc.line()));
    }
}

void eventually_require_rows(cql_test_env& e, sstring_view qstr, const std::vector<std::vector<bytes_opt>>& expected,
                             const seastar::compat::source_location& loc) {
    try {
        eventually([&] {
            assert_that(cquery_nofail(e, qstr, nullptr, loc)).is_rows().with_rows_ignore_order(expected);
        });
    } catch (const std::exception& e) {
        BOOST_FAIL(format("query '{}' failed: {}\n{}:{}: originally from here",
                          qstr, e.what(), loc.file_name(), loc.line()));
    }
}

void require_rows(cql_test_env& e,
                  cql3::prepared_cache_key_type id,
                  const std::vector<cql3::raw_value>& values,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const seastar::compat::source_location& loc) {
    try {
        assert_that(e.execute_prepared(id, values).get()).is_rows().with_rows_ignore_order(expected);
    } catch (const std::exception& e) {
        BOOST_FAIL(format("execute_prepared failed: {}\n{}:{}: originally from here",
                          e.what(), loc.file_name(), loc.line()));
    }
}

future<> require_column_has_value(cql_test_env& e, const sstring& table_name,
        std::vector<data_value> pk, std::vector<data_value> ck,
        const sstring& column_name, data_value expected) {
    auto& db = e.local_db();
    auto& cf = db.find_column_family("ks", table_name);
    auto schema = cf.schema();
    auto pkey = partition_key::from_deeply_exploded(*schema, pk);
    auto ckey = clustering_key::from_deeply_exploded(*schema, ck);
    auto exp = expected.type()->decompose(expected);
    auto dk = dht::decorate_key(*schema, pkey);
    auto shard = cf.get_effective_replication_map()->shard_for_reads(*schema, dk._token);
    return e.db().invoke_on(shard, [&e, dk = std::move(dk),
                                  ckey = std::move(ckey),
                                  column_name = std::move(column_name),
                                  exp = std::move(exp),
                                  table_name = std::move(table_name)] (replica::database& db) mutable {
      auto& cf = db.find_column_family("ks", table_name);
      auto schema = cf.schema();
      return cf.find_row(schema, make_reader_permit(e), dk, ckey).then([schema, column_name, exp] (auto row) {
        SCYLLA_ASSERT(row != nullptr);
        auto col_def = schema->get_column_definition(utf8_type->decompose(column_name));
        SCYLLA_ASSERT(col_def != nullptr);
        const atomic_cell_or_collection* cell = row->find_cell(col_def->id);
        if (!cell) {
            SCYLLA_ASSERT(((void)"column not set", 0));
        }
        bytes actual;
        if (!col_def->type->is_multi_cell()) {
            auto c = cell->as_atomic_cell(*col_def);
            SCYLLA_ASSERT(c.is_live());
            actual = c.value().linearize();
        } else {
            actual = linearized(serialize_for_cql(*col_def->type,
                    cell->as_collection_mutation()));
        }
        SCYLLA_ASSERT(col_def->type->equal(actual, exp));
      });
    });
}
