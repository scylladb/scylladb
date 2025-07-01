
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/assert.hh"
#include "utils/managed_bytes.hh"
#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message_base.hh"
#include "bytes.hh"
#include <source_location>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>

class columns_assertions {
    const cql3::metadata& _metadata;
    const std::vector<managed_bytes_opt>& _columns;

    columns_assertions& do_with_raw_column(const char* name, std::function<void(data_type, managed_bytes_view)> func);

    void fail(const sstring& msg);

public:
    columns_assertions(const cql3::metadata& metadata, const std::vector<managed_bytes_opt>& columns)
        : _metadata(metadata), _columns(columns)
    { }

    columns_assertions& with_raw_column(const char* name, std::function<bool(managed_bytes_view)> predicate);
    columns_assertions& with_raw_column(const char* name, managed_bytes_view value);

    template <typename T>
    columns_assertions& with_typed_column(const char* name, std::function<bool(const T& value)> predicate) {
        return do_with_raw_column(name, [this, name, predicate] (data_type type, managed_bytes_view value) {
            if (type != data_type_for<T>()) {
                fail(seastar::format("Column {} is not of type {}, but of type {}", name, data_type_for<T>()->name(), type->name()));
            }
            if (!predicate(value_cast<T>(type->deserialize(value)))) {
                fail(seastar::format("Column {} failed predicate check: value = {}", name, value));
            }
        });
    }

    template <typename T>
    columns_assertions& with_typed_column(const char* name, const T& value) {
        return  with_typed_column<T>(name, [this, name, &value] (const T& cell_value) {
            if (cell_value != value) {
                fail(seastar::format("Expected column {} to have value {}, but got {}", name, value, cell_value));
            }
            return true;
        });
    }
};

class rows_assertions {
    shared_ptr<cql_transport::messages::result_message::rows> _rows;
public:
    rows_assertions(shared_ptr<cql_transport::messages::result_message::rows> rows);
    rows_assertions with_size(size_t size);
    rows_assertions is_empty();
    rows_assertions is_not_empty();
    rows_assertions with_column_types(std::initializer_list<data_type> column_types);
    rows_assertions with_row(std::initializer_list<bytes_opt> values);

    // Verifies that the result has the following rows and only that rows, in that order.
    rows_assertions with_rows(std::vector<std::vector<bytes_opt>> rows);
    // Verifies that the result has the following rows and only those rows.
    rows_assertions with_rows_ignore_order(std::vector<std::vector<bytes_opt>> rows);
    rows_assertions with_serialized_columns_count(size_t columns_count);

    columns_assertions with_columns_of_row(size_t row_index);

    rows_assertions is_null();
    rows_assertions is_not_null();
};

class result_msg_assertions {
    shared_ptr<cql_transport::messages::result_message> _msg;
public:
    result_msg_assertions(shared_ptr<cql_transport::messages::result_message> msg);
    rows_assertions is_rows();
};

result_msg_assertions assert_that(shared_ptr<cql_transport::messages::result_message> msg);

template<typename T>
void assert_that_failed(future<T>& f)
{
    try {
        f.get();
        SCYLLA_ASSERT(f.failed());
    }
    catch (...) {
    }
}

template<typename T>
void assert_that_failed(future<T>&& f)
{
    try {
        f.get();
        SCYLLA_ASSERT(f.failed());
    }
    catch (...) {
    }
}

/// Invokes env.execute_cql(query), awaits its result, and returns it.  If an exception is thrown,
/// invokes BOOST_FAIL with useful diagnostics.
///
/// \note Should be called from a seastar::thread context, as it awaits the CQL result.
shared_ptr<cql_transport::messages::result_message> cquery_nofail(
        cql_test_env& env,
        std::string_view query,
        std::unique_ptr<cql3::query_options>&& qo = nullptr,
        const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Asserts that cquery_nofail(e, qstr) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  std::string_view qstr,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Like require_rows, but wraps assertions in \c eventually.
void eventually_require_rows(
        cql_test_env& e, std::string_view qstr, const std::vector<std::vector<bytes_opt>>& expected,
        const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Asserts that e.execute_prepared(id, values) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  cql3::prepared_cache_key_type id,
                  const std::vector<cql3::raw_value>& values,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Asserts that a cell at the given table.partition.row.column position contains expected data
future<> require_column_has_value(cql_test_env&, const sstring& table_name,
        std::vector<data_value> pk, std::vector<data_value> ck, const sstring& column_name, data_value expected);
