/*
 * Copyright (C) 2017-present ScyllaDB
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
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include "transport/messages/result_message.hh"
#include "types/set.hh"

#include "db/config.hh"

namespace {

void create_table(cql_test_env& e) {
    e.execute_cql("CREATE TABLE test (a tinyint primary key,"
                  " b smallint,"
                  " c int,"
                  " d bigint,"
                  " e float,"
                  " f double,"
                  " g_0 decimal,"
                  " g_2 decimal,"
                  " h varint, "
                  " t text,"
                  " dt date,"
                  " tm timestamp,"
                  " tu timeuuid,"
                  " bl blob, "
                  " bo boolean)").get();

    e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g_0, g_2, h, t, dt, tm, tu, bl, bo) VALUES (1, 1, 1, 1, 1, 1, 1, 1.00, 1, 'a', '2017-12-02', '2017-12-02t03:00:00', b650cbe0-f914-11e7-8892-000000000004, 0x0101, false)").get();
    e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g_0, g_2, h, t, dt, tm, tu, bl, bo) VALUES (2, 2, 2, 2, 2, 2, 2, 2.00, 2, 'b', '2016-12-02', '2016-12-02t06:00:00', D2177dD0-EAa2-11de-a572-001B779C76e3, 0x01, true)").get();
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
                                                          {double_type->decompose((1.+2.)/2L)},
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
                                                          {double_type->decompose(3.)},
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
                                 "max(h), "
                                 "max(t), "
                                 "max(dt), "
                                 "max(tm), "
                                 "max(tu), "
                                 "max(bl), "
                                 "max(bo) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(2))},
                                                          {short_type->decompose(int16_t(2))},
                                                          {int32_type->decompose(int32_t(2))},
                                                          {long_type->decompose(int64_t(2))},
                                                          {float_type->decompose(2.f)},
                                                          {double_type->decompose(2.)},
                                                          {decimal_type->from_string("2")},
                                                          {decimal_type->from_string("2.00")},
                                                          {varint_type->from_string("2")},
                                                          {utf8_type->from_string("b")},
                                                          {simple_date_type->from_string("2017-12-02")},
                                                          {timestamp_type->from_string("2017-12-02t03:00:00")},
                                                          {timeuuid_type->from_string("b650cbe0-f914-11e7-8892-000000000004")},
                                                          {bytes_type->from_string("0101")},
                                                          {boolean_type->from_string("true")},
        });
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
                                 "min(h), "
                                 "min(t), "
                                 "min(dt), "
                                 "min(tm), "
                                 "min(tu), "
                                 "min(bl), "
                                 "min(bo) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(1))},
                                                          {short_type->decompose(int16_t(1))},
                                                          {int32_type->decompose(int32_t(1))},
                                                          {long_type->decompose(int64_t(1))},
                                                          {float_type->decompose(1.f)},
                                                          {double_type->decompose(1.)},
                                                          {decimal_type->from_string("1")},
                                                          {decimal_type->from_string("1.00")},
                                                          {varint_type->from_string("1")},
                                                          {utf8_type->from_string("a")},
                                                          {simple_date_type->from_string("2016-12-02")},
                                                          {timestamp_type->from_string("2016-12-02t06:00:00")},
                                                          {timeuuid_type->from_string("D2177dD0-EAa2-11de-a572-001B779C76e3")},
                                                          {bytes_type->from_string("01")},
                                                          {boolean_type->from_string("false")},
        });
    });
}

SEASTAR_TEST_CASE(test_aggregate_count) {
    return do_with_cql_env_thread([&] (auto& e) {

        e.execute_cql("CREATE TABLE test(a int primary key, b int, c int, bl blob, bo boolean)").get();
        e.execute_cql("INSERT INTO test(a, b, bl, bo) VALUES (1, 1, 0x01, false)").get();
        e.execute_cql("INSERT INTO test(a, c, bo) VALUES (2, 2, true)").get();
        e.execute_cql("INSERT INTO test(a, c, bl) VALUES (3, 3, 0x03)").get();

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
        {
            auto msg = e.execute_cql("SELECT count(bo) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(2))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(a), count(b), count(c), count(bl), count(bo), count(*) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))},
                                                              {long_type->decompose(int64_t(1))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(3))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(a), count(b), count(c), count(bo), count(*) FROM test LIMIT 1").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))},
                                                              {long_type->decompose(int64_t(1))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(3))}});
        }
    });
}

SEASTAR_TEST_CASE(test_reverse_type_aggregation) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test(p int, c timestamp, v int, primary key (p, c)) with clustering order by (c desc)").get();
        e.execute_cql("INSERT INTO test(p, c, v) VALUES (1, 1, 1)").get();
        e.execute_cql("INSERT INTO test(p, c, v) VALUES (1, 2, 1)").get();

        {
            auto tp = db_clock::from_time_t({ 0 }) + std::chrono::milliseconds(1);
            auto msg = e.execute_cql("SELECT min(c) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{timestamp_type->decompose(tp)}});
        }
        {
            auto tp = db_clock::from_time_t({ 0 }) + std::chrono::milliseconds(2);
            auto msg = e.execute_cql("SELECT max(c) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{timestamp_type->decompose(tp)}});
        }
    });
}

// Tests #6768
SEASTAR_TEST_CASE(test_minmax_on_set) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (id int PRIMARY KEY, s1 set<int>, s2 set<blob>);").get();
        e.execute_cql("INSERT INTO test (id, s1, s2) VALUES (1, {-1, 1}, {0xff, 0x01});").get();
        e.execute_cql("INSERT INTO test (id, s1, s2) VALUES (2, {-2, 2}, {0xfe, 0x02});").get();

        const auto set_type_int = set_type_impl::get_instance(int32_type, true);
        const auto set_type_blob = set_type_impl::get_instance(bytes_type, true);
        {
            const auto msg = e.execute_cql("SELECT max(s1), max(s2) FROM test;").get0();
            assert_that(msg).is_rows().with_size(1).with_row({
                    set_type_int->decompose(make_set_value(set_type_int, {-1, 1})),
                    set_type_blob->decompose(make_set_value(set_type_blob, {"\x02", "\xfe"}))
            });
        }
        {
            const auto msg = e.execute_cql("SELECT min(s1), min(s2) FROM test;").get0();
            assert_that(msg).is_rows().with_size(1).with_row({
                    set_type_int->decompose(make_set_value(set_type_int, {-2, 2})),
                    set_type_blob->decompose(make_set_value(set_type_blob, {"\x01", "\xff"}))
            });
        }
    });
}
