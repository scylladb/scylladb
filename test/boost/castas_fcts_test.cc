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
#include "test/lib/exception_utils.hh"

#include <seastar/core/future-util.hh>
#include "transport/messages/result_message.hh"

#include "db/config.hh"

namespace {

template<typename T>
struct cql_type_name {
};
template<>
struct cql_type_name<int> {
    static constexpr char value[] = "int";
};
constexpr char cql_type_name<int>::value[];
template<>
struct cql_type_name<long> {
    static constexpr char value[] = "bigint";
};
constexpr char cql_type_name<long>::value[];
template<>
struct cql_type_name<float> {
    static constexpr char value[] = "float";
};
constexpr char cql_type_name<float>::value[];
template<>
struct cql_type_name<double> {
    static constexpr char value[] = "double";
};
constexpr char cql_type_name<double>::value[];

template<typename RetType, typename Type>
auto test_explicit_type_casting_in_avg_function() {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql(format("CREATE TABLE air_quality_data (sensor_id text, time timestamp, co_ppm {}, PRIMARY KEY (sensor_id, time));", cql_type_name<Type>::value)).get();
        e.execute_cql(
            "begin unlogged batch \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:00', 17); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:01', 18); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:02', 19); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:03', 20); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:04', 30); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:05', 31); \n"
            "  INSERT INTO air_quality_data(sensor_id, time, co_ppm) VALUES ('my_home', '2016-08-30 07:01:10', 20); \n"
            "apply batch;").get();
            auto msg = e.execute_cql(format("select avg(CAST(co_ppm AS {})) from air_quality_data;", cql_type_name<RetType>::value)).get0();
            assert_that(msg).is_rows().with_size(1).with_row({{data_type_for<RetType>()->decompose( RetType(17 + 18 + 19 + 20 + 30 + 31 + 20) / RetType(7) )}});
    });
}

} /* anonymous namespace */

SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_int) {
    return test_explicit_type_casting_in_avg_function<double, int>();
}
SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_long) {
    return test_explicit_type_casting_in_avg_function<double, long>();
}

SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_float) {
    return test_explicit_type_casting_in_avg_function<float, float>();
}

SEASTAR_TEST_CASE(test_explicit_type_casting_in_avg_function_double) {
    return test_explicit_type_casting_in_avg_function<double, double>();
}

SEASTAR_TEST_CASE(test_unsupported_conversions) {
    return do_with_cql_env_thread([&] (auto& e) {
        using ire = exceptions::invalid_request_exception;
        using exception_predicate::message_contains;
        e.execute_cql("CREATE TABLE air_quality_data_text (sensor_id text, time timestamp, co_ppm text, PRIMARY KEY (sensor_id, time));").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select CAST(co_ppm AS int) from air_quality_data_text").get(), ire,
            message_contains("UTF8Type cannot be cast to org.apache.cassandra.db.marshal.Int32Type"));
        e.execute_cql("CREATE TABLE air_quality_data_ascii (sensor_id text, time timestamp, co_ppm ascii, PRIMARY KEY (sensor_id, time));").get();
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select CAST(co_ppm AS int) from air_quality_data_ascii").get(), ire,
            message_contains("AsciiType cannot be cast to org.apache.cassandra.db.marshal.Int32Type"));
    });
}

SEASTAR_TEST_CASE(test_decimal_to_bigint) {
    return do_with_cql_env_thread([&](auto& e) {
        e.execute_cql("CREATE TABLE test (key text primary key, value decimal)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k1', 9223372036854775807)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k2', 9223372036854775808)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k3', 18446744073709551615)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k4', 18446744073709551616)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k5', 18446744073709551617)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k6', 18446744073709551617.1)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k7', 18446744073709551617.9)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k8', -1)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k9', -9223372036854775808)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k10', -9223372036854775809)").get();
        auto v = e.execute_cql("SELECT key,CAST(value as bigint) from test").get0();
        assert_that(v).is_rows().with_rows_ignore_order({
            {{utf8_type->decompose("k1")}, {long_type->decompose(std::numeric_limits<int64_t>::max())}},
            {{utf8_type->decompose("k2")}, {long_type->decompose(std::numeric_limits<int64_t>::min())}},
            {{utf8_type->decompose("k3")}, {long_type->decompose(int64_t(-1))}},
            {{utf8_type->decompose("k4")}, {long_type->decompose(int64_t(0))}},
            {{utf8_type->decompose("k5")}, {long_type->decompose(int64_t(1))}},
            {{utf8_type->decompose("k6")}, {long_type->decompose(int64_t(1))}},
            {{utf8_type->decompose("k7")}, {long_type->decompose(int64_t(1))}},
            {{utf8_type->decompose("k8")}, {long_type->decompose(int64_t(-1))}},
            {{utf8_type->decompose("k9")}, {long_type->decompose(std::numeric_limits<int64_t>::min())}},
            {{utf8_type->decompose("k10")}, {long_type->decompose(std::numeric_limits<int64_t>::max())}},
        });
    });
}

SEASTAR_TEST_CASE(test_decimal_to_float) {
    return do_with_cql_env_thread([&](auto& e) {
        e.execute_cql("CREATE TABLE test (key text primary key, value decimal)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k1', 10)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k2', 1e1)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k3', 100e-1)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k4', -1e1)").get();
        auto v = e.execute_cql("SELECT key, CAST(value as float) from test").get0();
        assert_that(v).is_rows().with_rows_ignore_order({
            {{serialized("k1")}, {serialized(float(10))}},
            {{serialized("k2")}, {serialized(float(10))}},
            {{serialized("k3")}, {serialized(float(10))}},
            {{serialized("k4")}, {serialized(float(-10))}},
        });
    });
}

SEASTAR_TEST_CASE(test_varint_to_bigint) {
    return do_with_cql_env_thread([&](auto& e) {
        e.execute_cql("CREATE TABLE test (key text primary key, value varint)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k1', 9223372036854775807)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k2', 9223372036854775808)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k3', 18446744073709551615)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k4', 18446744073709551616)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k5', 18446744073709551617)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k6', -1)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k7', -9223372036854775808)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k8', -9223372036854775809)").get();
        auto v = e.execute_cql("SELECT key,CAST(value as bigint) from test").get0();
        assert_that(v).is_rows().with_rows_ignore_order({
            {{utf8_type->decompose("k1")}, {long_type->decompose(std::numeric_limits<int64_t>::max())}},
            {{utf8_type->decompose("k2")}, {long_type->decompose(std::numeric_limits<int64_t>::min())}},
            {{utf8_type->decompose("k3")}, {long_type->decompose(int64_t(-1))}},
            {{utf8_type->decompose("k4")}, {long_type->decompose(int64_t(0))}},
            {{utf8_type->decompose("k5")}, {long_type->decompose(int64_t(1))}},
            {{utf8_type->decompose("k6")}, {long_type->decompose(int64_t(-1))}},
            {{utf8_type->decompose("k7")}, {long_type->decompose(std::numeric_limits<int64_t>::min())}},
            {{utf8_type->decompose("k8")}, {long_type->decompose(std::numeric_limits<int64_t>::max())}},
        });
    });
}

SEASTAR_TEST_CASE(test_varint_to_int) {
    return do_with_cql_env_thread([&](auto& e) {
        e.execute_cql("CREATE TABLE test (key text primary key, value varint)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k1', 2147483647)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k2', 2147483648)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k3', 4294967295)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k4', 4294967296)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k5', 4294967297)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k6', -1)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k7', -2147483648)").get();
        e.execute_cql("INSERT INTO test (key, value) VALUES ('k8', -2147483649)").get();
        auto v = e.execute_cql("SELECT key,CAST(value as int) from test").get0();
        assert_that(v).is_rows().with_rows_ignore_order({
            {{utf8_type->decompose("k1")}, {int32_type->decompose(std::numeric_limits<int32_t>::max())}},
            {{utf8_type->decompose("k2")}, {int32_type->decompose(std::numeric_limits<int32_t>::min())}},
            {{utf8_type->decompose("k3")}, {int32_type->decompose(int32_t(-1))}},
            {{utf8_type->decompose("k4")}, {int32_type->decompose(int32_t(0))}},
            {{utf8_type->decompose("k5")}, {int32_type->decompose(int32_t(1))}},
            {{utf8_type->decompose("k6")}, {int32_type->decompose(int32_t(-1))}},
            {{utf8_type->decompose("k7")}, {int32_type->decompose(std::numeric_limits<int32_t>::min())}},
            {{utf8_type->decompose("k8")}, {int32_type->decompose(std::numeric_limits<int32_t>::max())}},
        });
    });
}

SEASTAR_TEST_CASE(test_numeric_casts_in_selection_clause) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (a tinyint primary key,"
                      " b smallint,"
                      " c int,"
                      " d bigint,"
                      " e float,"
                      " f double,"
                      " g decimal,"
                      " h varint,"
                      " i int)").get();

        e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g, h) VALUES (1, 2, 3, 4, 5.2, 6.3, 7.3, 8)").get();
        {
            auto msg = e.execute_cql("SELECT CAST(a AS tinyint), "
                                     "CAST(b AS tinyint), "
                                     "CAST(c AS tinyint), "
                                     "CAST(d AS tinyint), "
                                     "CAST(e AS tinyint), "
                                     "CAST(f AS tinyint), "
                                     "CAST(g AS tinyint), "
                                     "CAST(h AS tinyint), "
                                     "CAST(i AS tinyint) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(1))},
                                                              {byte_type->decompose(int8_t(2))},
                                                              {byte_type->decompose(int8_t(3))},
                                                              {byte_type->decompose(int8_t(4))},
                                                              {byte_type->decompose(int8_t(5))},
                                                              {byte_type->decompose(int8_t(6))},
                                                              {byte_type->decompose(int8_t(7))},
                                                              {byte_type->decompose(int8_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS smallint), "
                                     "CAST(b AS smallint), "
                                     "CAST(c AS smallint), "
                                     "CAST(d AS smallint), "
                                     "CAST(e AS smallint), "
                                     "CAST(f AS smallint), "
                                     "CAST(g AS smallint), "
                                     "CAST(h AS smallint), "
                                     "CAST(i AS smallint) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{short_type->decompose(int16_t(1))},
                                                              {short_type->decompose(int16_t(2))},
                                                              {short_type->decompose(int16_t(3))},
                                                              {short_type->decompose(int16_t(4))},
                                                              {short_type->decompose(int16_t(5))},
                                                              {short_type->decompose(int16_t(6))},
                                                              {short_type->decompose(int16_t(7))},
                                                              {short_type->decompose(int16_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS int), "
                                     "CAST(b AS int), "
                                     "CAST(c AS int), "
                                     "CAST(d AS int), "
                                     "CAST(e AS int), "
                                     "CAST(f AS int), "
                                     "CAST(g AS int), "
                                     "CAST(h AS int), "
                                     "CAST(i AS int) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{int32_type->decompose(int32_t(1))},
                                                              {int32_type->decompose(int32_t(2))},
                                                              {int32_type->decompose(int32_t(3))},
                                                              {int32_type->decompose(int32_t(4))},
                                                              {int32_type->decompose(int32_t(5))},
                                                              {int32_type->decompose(int32_t(6))},
                                                              {int32_type->decompose(int32_t(7))},
                                                              {int32_type->decompose(int32_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS bigint), "
                                     "CAST(b AS bigint), "
                                     "CAST(c AS bigint), "
                                     "CAST(d AS bigint), "
                                     "CAST(e AS bigint), "
                                     "CAST(f AS bigint), "
                                     "CAST(g AS bigint), "
                                     "CAST(h AS bigint), "
                                     "CAST(i AS bigint) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(1))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(3))},
                                                              {long_type->decompose(int64_t(4))},
                                                              {long_type->decompose(int64_t(5))},
                                                              {long_type->decompose(int64_t(6))},
                                                              {long_type->decompose(int64_t(7))},
                                                              {long_type->decompose(int64_t(8))},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS float), "
                                     "CAST(b AS float), "
                                     "CAST(c AS float), "
                                     "CAST(d AS float), "
                                     "CAST(e AS float), "
                                     "CAST(f AS float), "
                                     "CAST(g AS float), "
                                     "CAST(h AS float), "
                                     "CAST(i AS float) FROM test").get0();
            // Conversions that include floating point cannot be compared with assert_that(), because result
            //   of such conversions may be slightly different from theoretical values.
            auto cmp = [&](::size_t index, float req) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().result_set().rows().front();
                auto val = value_cast<float>( float_type->deserialize(row[index].value()) );
                BOOST_CHECK_CLOSE(val, req, 1e-4);
            };
            auto cmp_null = [&](::size_t index) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().result_set().rows().front();
                BOOST_CHECK(!row[index]);
            };
            cmp(0, 1.f);
            cmp(1, 2.f);
            cmp(2, 3.f);
            cmp(3, 4.f);
            cmp(4, 5.2f);
            cmp(5, 6.3f);
            cmp(6, 7.3f);
            cmp(7, 8.f);
            cmp_null(8);
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS double), "
                                     "CAST(b AS double), "
                                     "CAST(c AS double), "
                                     "CAST(d AS double), "
                                     "CAST(e AS double), "
                                     "CAST(f AS double), "
                                     "CAST(g AS double), "
                                     "CAST(h AS double), "
                                     "CAST(i AS double) FROM test").get0();
            // Conversions that include floating points cannot be compared with assert_that(), because result
            //   of such conversions may be slightly different from theoretical values.
            auto cmp = [&](::size_t index, double req) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().result_set().rows().front();
                auto val = value_cast<double>( double_type->deserialize(row[index].value()) );
                BOOST_CHECK_CLOSE(val, req, 1e-4);
            };
            auto cmp_null = [&](::size_t index) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().result_set().rows().front();
                BOOST_CHECK(!row[index]);
            };
            cmp(0, 1.);
            cmp(1, 2.);
            cmp(2, 3.);
            cmp(3, 4.);
            cmp(4, 5.2);
            cmp(5, 6.3);
            cmp(6, 7.3);
            cmp(7, 8.);
            cmp_null(8);
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS decimal), "
                                     "CAST(b AS decimal), "
                                     "CAST(c AS decimal), "
                                     "CAST(d AS decimal), "
                                     "CAST(e AS decimal), "
                                     "CAST(f AS decimal), "
                                     "CAST(g AS decimal), "
                                     "CAST(h AS decimal), "
                                     "CAST(i AS decimal) FROM test").get0();
            // Conversions that include floating points cannot be compared with assert_that(), because result
            //   of such conversions may be slightly different from theoretical values.
            auto cmp = [&](::size_t index, double req) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().result_set().rows().front();
                auto val = value_cast<big_decimal>( decimal_type->deserialize(row[index].value()) );
                BOOST_CHECK_CLOSE(boost::lexical_cast<double>(val.to_string()), req, 1e-4);
            };
            auto cmp_null = [&](::size_t index) {
                auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().result_set().rows().front();
                BOOST_CHECK(!row[index]);
            };
            cmp(0, 1.);
            cmp(1, 2.);
            cmp(2, 3.);
            cmp(3, 4.);
            cmp(4, 5.2);
            cmp(5, 6.3);
            cmp(6, 7.3);
            cmp(7, 8.);
            cmp_null(8);
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS ascii), "
                                     "CAST(b AS ascii), "
                                     "CAST(c AS ascii), "
                                     "CAST(d AS ascii), "
                                     "CAST(e AS ascii), "
                                     "CAST(f AS ascii), "
                                     "CAST(g AS ascii), "
                                     "CAST(h AS ascii), "
                                     "CAST(i AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->decompose("1")},
                                                              {ascii_type->decompose("2")},
                                                              {ascii_type->decompose("3")},
                                                              {ascii_type->decompose("4")},
                                                              {ascii_type->decompose("5.2")},
                                                              {ascii_type->decompose("6.3")},
                                                              {ascii_type->decompose("7.3")},
                                                              {ascii_type->decompose("8")},
                                                              {}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS text), "
                                     "CAST(b AS text), "
                                     "CAST(c AS text), "
                                     "CAST(d AS text), "
                                     "CAST(e AS text), "
                                     "CAST(f AS text), "
                                     "CAST(g AS text), "
                                     "CAST(h AS text), "
                                     "CAST(i AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->decompose("1")},
                                                              {utf8_type->decompose("2")},
                                                              {utf8_type->decompose("3")},
                                                              {utf8_type->decompose("4")},
                                                              {utf8_type->decompose("5.2")},
                                                              {utf8_type->decompose("6.3")},
                                                              {utf8_type->decompose("7.3")},
                                                              {utf8_type->decompose("8")},
                                                              {}});
        }
    });
}

SEASTAR_TEST_CASE(test_integers_to_decimal_casts_in_selection_clause) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (a tinyint primary key,"
                      " b smallint,"
                      " c int,"
                      " d bigint,"
                      " h varint)").get();

        e.execute_cql("INSERT INTO test (a, b, c, d, h) VALUES (1, 2, 3, 4, 8)").get();
        auto msg = e.execute_cql("SELECT CAST(a AS decimal), "
                                 "CAST(b AS decimal), "
                                 "CAST(c AS decimal), "
                                 "CAST(d AS decimal), "
                                 "CAST(h AS decimal) FROM test").get0();

        auto cmp = [&](::size_t index, auto x) {
            auto row = dynamic_cast<cql_transport::messages::result_message::rows&>(*msg).rs().result_set().rows().front();
            auto val = value_cast<big_decimal>( decimal_type->deserialize(row[index].value()) );
            BOOST_CHECK_EQUAL(val.unscaled_value(), x*10);
            BOOST_CHECK_EQUAL(val.scale(), 1);
        };
        cmp(0, 1);
        cmp(1, 2);
        cmp(2, 3);
        cmp(3, 4);
        cmp(4, 8);
    });
}

SEASTAR_TEST_CASE(test_integers_to_decimal_casts_with_avg_in_selection_clause) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (a tinyint primary key,"
                      " b smallint,"
                      " c int,"
                      " d bigint,"
                      " h varint)").get();

        e.execute_cql("INSERT INTO test (a, b, c, d, h) VALUES (1, 2, 3, 4, 8)").get();
        e.execute_cql("INSERT INTO test (a, b, c, d, h) VALUES (2, 3, 4, 5, 9)").get();
        auto msg = e.execute_cql("SELECT CAST(avg(CAST(a AS decimal)) AS text), "
                                 "CAST(avg(CAST(b AS decimal)) AS text), "
                                 "CAST(avg(CAST(c AS decimal)) AS text), "
                                 "CAST(avg(CAST(d AS decimal)) AS text), "
                                 "CAST(avg(CAST(h AS decimal)) AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->decompose("1.5")},
                                                              {utf8_type->decompose("2.5")},
                                                              {utf8_type->decompose("3.5")},
                                                              {utf8_type->decompose("4.5")},
                                                              {utf8_type->decompose("8.5")}});
    });
}

SEASTAR_TEST_CASE(test_time_casts_in_selection_clause) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (a timeuuid primary key,"
                      "b timestamp,"
                      "c date,"
                      "d time)").get();

        e.execute_cql("INSERT INTO test (a, b, c, d) VALUES (d2177dd0-eaa2-11de-a572-001b779c76e3, '2015-05-21 11:03:02+00', '2015-05-21', '11:03:02')").get();
        {
            auto msg = e.execute_cql("SELECT CAST(a AS timestamp), CAST(a AS date), CAST(b as date), CAST(c AS timestamp) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{timestamp_type->from_string("2009-12-17t00:26:29.805+00")},
                                                              {simple_date_type->from_string("2009-12-17")},
                                                              {simple_date_type->from_string("2015-05-21")},
                                                              {timestamp_type->from_string("2015-05-21t00:00:00+00")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(CAST(a AS timestamp) AS text), CAST(CAST(a AS date) AS text), CAST(CAST(b as date) AS text), CAST(CAST(c AS timestamp) AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->from_string("2009-12-17T00:26:29.805000")},
                                                              {utf8_type->from_string("2009-12-17")},
                                                              {utf8_type->from_string("2015-05-21")},
                                                              {utf8_type->from_string("2015-05-21T00:00:00")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS text), CAST(b as text), CAST(c AS text), CAST(d AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->from_string("d2177dd0-eaa2-11de-a572-001b779c76e3")},
                                                              {utf8_type->from_string("2015-05-21T11:03:02")},
                                                              {utf8_type->from_string("2015-05-21")},
                                                              {utf8_type->from_string("11:03:02.000000000")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(CAST(a AS timestamp) AS ascii), CAST(CAST(a AS date) AS ascii), CAST(CAST(b as date) AS ascii), CAST(CAST(c AS timestamp) AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->from_string("2009-12-17T00:26:29.805000")},
                                                              {ascii_type->from_string("2009-12-17")},
                                                              {ascii_type->from_string("2015-05-21")},
                                                              {ascii_type->from_string("2015-05-21T00:00:00")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS ascii), CAST(b as ascii), CAST(c AS ascii), CAST(d AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->from_string("d2177dd0-eaa2-11de-a572-001b779c76e3")},
                                                              {ascii_type->from_string("2015-05-21T11:03:02")},
                                                              {ascii_type->from_string("2015-05-21")},
                                                              {ascii_type->from_string("11:03:02.000000000")}});
        }
    });
}

SEASTAR_TEST_CASE(test_other_type_casts_in_selection_clause) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (a ascii primary key,"
                      "b inet,"
                      "c boolean)").get();
        e.execute_cql("INSERT INTO test (a, b, c) VALUES ('test', '127.0.0.1', true)").get();
        {
            auto msg = e.execute_cql("SELECT CAST(a AS text), CAST(b as text), CAST(c AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->from_string("test")},
                                                              {utf8_type->from_string("127.0.0.1")},
                                                              {utf8_type->from_string("true")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS ascii), CAST(b as ascii), CAST(c AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->from_string("test")},
                                                              {ascii_type->from_string("127.0.0.1")},
                                                              {ascii_type->from_string("true")}});
        }
    });
}

SEASTAR_TEST_CASE(test_casts_with_revrsed_order_in_selection_clause) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test (a int,"
                      "b smallint,"
                      "c double,"
                      "primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)").get();
        e.execute_cql("INSERT INTO test (a, b, c) VALUES (1, 2, 6.3)").get();
        {
            auto msg = e.execute_cql("SELECT CAST(a AS tinyint), CAST(b as tinyint), CAST(c AS tinyint) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{byte_type->from_string("1")},
                                                              {byte_type->from_string("2")},
                                                              {byte_type->from_string("6")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS smallint), CAST(b as smallint), CAST(c AS smallint) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{short_type->from_string("1")},
                                                              {short_type->from_string("2")},
                                                              {short_type->from_string("6")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS double), CAST(b as double), CAST(c AS double) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{double_type->from_string("1")},
                                                              {double_type->from_string("2")},
                                                              {double_type->from_string("6.3")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS text), CAST(b as text), CAST(c AS text) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{utf8_type->from_string("1")},
                                                              {utf8_type->from_string("2")},
                                                              {utf8_type->from_string("6.3")}});
        }
        {
            auto msg = e.execute_cql("SELECT CAST(a AS ascii), CAST(b as ascii), CAST(c AS ascii) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{ascii_type->from_string("1")},
                                                              {ascii_type->from_string("2")},
                                                              {ascii_type->from_string("6.3")}});
        }
    });
}

SEASTAR_TEST_CASE(test_identity_casts) {
    return do_with_cql_env_thread([&] (auto& e) {
        const std::pair<data_type, const char*> type_value_pairs[] = {
                {ascii_type, "'val'"},
                {long_type, "0"},
                {bytes_type, "0x0000000000000003"},
                {boolean_type, "true"},
                {double_type, "0.0"},
                {float_type, "0.0"},
                {int32_type, "0"},
                {short_type, "0"},
                {utf8_type, "'val'"},
                {timestamp_type, "'2011-02-03 04:05+0000'"},
                {byte_type, "0"},
                {uuid_type, "123e4567-e89b-12d3-a456-426655440000"},
                {timeuuid_type, "123e4567-e89b-12d3-a456-426655440000"},
                {simple_date_type, "'2011-02-03'"},
                {time_type, "'08:12:54.123456789'"},
                {inet_addr_type, "'192.168.1.1'"},
                {varint_type, "0"},
                {decimal_type, "0.0"},
                {duration_type, "5h23m10s"},
        };

        for (const auto& [type, value] : type_value_pairs) {
            const auto type_name = type->cql3_type_name();
            cquery_nofail(e, format("create table t_{} (pk int primary key, v {})", type_name, type_name));
            cquery_nofail(e, format("insert into t_{} (pk, v) values (0, {})", type_name, value));
            cquery_nofail(e, format("select cast(v as {}) from t_{} where pk = 0", type_name, type_name, value));
        }
    });
}

// FIXME: Add test with user-defined functions after they are available.
