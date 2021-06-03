
/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message_base.hh"
#include "bytes.hh"
#include <experimental/source_location>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>

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

template<typename... T>
void assert_that_failed(future<T...>& f)
{
    try {
        f.get();
        assert(f.failed());
    }
    catch (...) {
    }
}

template<typename... T>
void assert_that_failed(future<T...>&& f)
{
    try {
        f.get();
        assert(f.failed());
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
        sstring_view query,
        std::unique_ptr<cql3::query_options>&& qo = nullptr,
        const std::experimental::source_location& loc = std::experimental::source_location::current());

/// Asserts that cquery_nofail(e, qstr) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  sstring_view qstr,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const std::experimental::source_location& loc = std::experimental::source_location::current());

/// Like require_rows, but wraps assertions in \c eventually.
void eventually_require_rows(
        cql_test_env& e, sstring_view qstr, const std::vector<std::vector<bytes_opt>>& expected,
        const std::experimental::source_location& loc = std::experimental::source_location::current());

/// Asserts that e.execute_prepared(id, values) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  cql3::prepared_cache_key_type id,
                  const std::vector<cql3::raw_value>& values,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const std::experimental::source_location& loc = std::experimental::source_location::current());
