/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
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


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include "utils/big_decimal.hh"
#include "exceptions/exceptions.hh"
#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "core/future-util.hh"
#include "transport/messages/result_message.hh"

#include "disk-error-handler.hh"
#include "db/config.hh"


thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

namespace {

template <typename Env>
void create_table(Env &e) {
        e.execute_cql("CREATE TABLE test (a tinyint primary key,"
                      " b smallint,"
                      " c int,"
                      " d bigint,"
                      " e float,"
                      " f double,"
                      " g_0 decimal,"
                      " g_2 decimal,"
                      " h varint, "
                      " t text)").get();

        e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g_0, g_2, h, t) VALUES (1, 1, 1, 1, 1, 1, 1, 1.00, 1, 'a')").get();
        e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g_0, g_2, h, t) VALUES (2, 2, 2, 2, 2, 2, 2, 2.00, 2, 'b')").get();
}

} /* anonymous namespace */

SEASTAR_TEST_CASE(test_aggregate_avg) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT avg(a), "
                                 "avg(b), "
                                 "avg(c), "
                                 "avg(d), "
                                 "avg(e), "
                                 "avg(f), "
                                 "avg(g_0), "
                                 "avg(g_2), "
                                 "avg(h) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(1))},
                                                          {short_type->decompose(int16_t(1))},
                                                          {int32_type->decompose(int32_t(1))},
                                                          {long_type->decompose(int64_t(1))},
                                                          {float_type->decompose((1.f+2.f)/2L)},
                                                          {double_type->decompose((1.d+2.d)/2L)},
                                                          {decimal_type->from_string("2")},
                                                          {decimal_type->from_string("1.50")},
                                                          {varint_type->from_string("1")}});
    });
}

SEASTAR_TEST_CASE(test_aggregate_sum) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT sum(a), "
                                 "sum(b), "
                                 "sum(c), "
                                 "sum(d), "
                                 "sum(e), "
                                 "sum(f), "
                                 "sum(g_0), "
                                 "sum(g_2), "
                                 "sum(h) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(3))},
                                                          {short_type->decompose(int16_t(3))},
                                                          {int32_type->decompose(int32_t(3))},
                                                          {long_type->decompose(int64_t(3))},
                                                          {float_type->decompose(3.f)},
                                                          {double_type->decompose(3.d)},
                                                          {decimal_type->from_string("3")},
                                                          {decimal_type->from_string("3.00")},
                                                          {varint_type->from_string("3")}});
    });
}

SEASTAR_TEST_CASE(test_aggregate_max) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT max(a), "
                                 "max(b), "
                                 "max(c), "
                                 "max(d), "
                                 "max(e), "
                                 "max(f), "
                                 "max(g_0), "
                                 "max(g_2), "
                                 "max(h),"
                                 "max(t) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(2))},
                                                          {short_type->decompose(int16_t(2))},
                                                          {int32_type->decompose(int32_t(2))},
                                                          {long_type->decompose(int64_t(2))},
                                                          {float_type->decompose(2.f)},
                                                          {double_type->decompose(2.d)},
                                                          {decimal_type->from_string("2")},
                                                          {decimal_type->from_string("2.00")},
                                                          {varint_type->from_string("2")},
                                                          {utf8_type->from_string("b")}});
    });
}

SEASTAR_TEST_CASE(test_aggregate_min) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT min(a), "
                                 "min(b), "
                                 "min(c), "
                                 "min(d), "
                                 "min(e), "
                                 "min(f), "
                                 "min(g_0), "
                                 "min(g_2), "
                                 "min(h),"
                                 "min(t) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(1))},
                                                          {short_type->decompose(int16_t(1))},
                                                          {int32_type->decompose(int32_t(1))},
                                                          {long_type->decompose(int64_t(1))},
                                                          {float_type->decompose(1.f)},
                                                          {double_type->decompose(1.d)},
                                                          {decimal_type->from_string("1")},
                                                          {decimal_type->from_string("1.00")},
                                                          {varint_type->from_string("1")},
                                                          {utf8_type->from_string("a")}});
    });
}

SEASTAR_TEST_CASE(test_aggregate_count) {
    return do_with_cql_env_thread([&] (auto& e) {

        e.execute_cql("CREATE TABLE test(a int primary key, b int, c int)").get();
        e.execute_cql("INSERT INTO test(a, b) VALUES (1, 1)").get();
        e.execute_cql("INSERT INTO test(a, c) VALUES (2, 2)").get();
        e.execute_cql("INSERT INTO test(a, c) VALUES (3, 3)").get();

        {
            auto msg = e.execute_cql("SELECT count(*) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(a) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(b) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(1))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(c) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(2))}});
        }
        // FIXME: Scylla doesn't support more occurences of count() function in select statement
        //        if count(*) is used, see issue #2218:
        //   SELELCT count(*), count(a) ...
        // auto msg = e.execute_cql("SELECT count(a), count(b), count(c), count(*) FROM test").get0();
        // assert_that(msg).is_rows().with_size(2).with_row({{long_type->decompose(int64_t(3))},
        //                                                  {long_type->decompose(int64_t(1))},
        //                                                  {long_type->decompose(int64_t(2))},
        //                                                  {long_type->decompose(int64_t(3))}});
    });
}
