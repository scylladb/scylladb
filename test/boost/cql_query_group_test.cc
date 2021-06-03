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
#include <experimental/source_location>

#include <seastar/net/inet_address.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include "transport/messages/result_message.hh"
#include "utils/big_decimal.hh"
#include "types/user.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "test/lib/exception_utils.hh"
#include "schema_builder.hh"

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_group_by_syntax) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e,
            "create table t1 (p1 int, p2 int, c1 int, c2 int, c3 int, npk int, primary key((p1, p2), c1, c2, c3))");
        cquery_nofail(e, "create table t2 (p1 int, p2 int, p3 int, npk int, primary key((p1, p2, p3)))");

        // Must parse correctly:
        cquery_nofail(e, "select count(c1) from t1 group by p1, p2");
        cquery_nofail(e, "select count(c1) from t1 group by p1, p2, c1");
        cquery_nofail(e, "select sum(c2) from t1 group by p1, p2");
        cquery_nofail(e, "select avg(npk) from t1 group by \"p1\", \"p2\"");
        cquery_nofail(e, "select sum(p2) from t1 group by p1, p2, c1, c2, c3");
        cquery_nofail(e, "select count(npk) from t1 where p1=1 and p2=1 group by c1, c2 order by c1 allow filtering");
        cquery_nofail(e, "select c2 from t1 where p2=2 group by p1, c1 allow filtering");
        cquery_nofail(e, "select npk from t1 where p2=2 group by p1, p2, c1 allow filtering");
        cquery_nofail(e, "select p1 from t2 group by p1, p2, p3");
        cquery_nofail(e, "select * from t2 where p1=1 group by p1, p2, p3 allow filtering");
        cquery_nofail(e, "select * from t2 where p1=1 group by p2, p3 allow filtering");
        cquery_nofail(e, "select * from t2 where p1=1 and p2=2 and p3=3 group by p1, p2, p3 allow filtering");
        cquery_nofail(e, "select * from t2 where p1=1 and p2=2 and p3=3 group by p3 allow filtering");
        cquery_nofail(e, "select * from t1 where p1>0 and p2=0 group by p1, c1 allow filtering");

        using ire = exceptions::invalid_request_exception;
        const auto unknown = exception_predicate::message_contains("unknown column");
        const auto non_primary = exception_predicate::message_contains("non-primary-key");
        const auto order = exception_predicate::message_contains("order");
        const auto partition = exception_predicate::message_contains("partition key");

        // Flag invalid columns in GROUP BY:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by xyz").get(), ire, unknown);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1, xyz").get(), ire, unknown);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by npk").get(), ire, non_primary);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1, npk").get(), ire, non_primary);
        // Even when GROUP BY lists all primary-key columns:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1, p2, c1, c2, c3, foo").get(), ire, unknown);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 group by p1, p2, c1, c2, c3, npk").get(), ire, non_primary);
        // Even when entire primary key is equality-restricted.
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 and p2=2 and p3=3 group by npk allow filtering").get(),
                ire, non_primary);
        // Flag invalid column order:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p2, p1").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where p1=1 group by p2, c2 allow filtering").get(), ire, order);
        // Even with equality restrictions:
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 group by p3 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 group by p2, p1 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p2=2 group by p1, p3, p2 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t2 where p1=1 and p2=2 and p3=3 group by p2, p1 allow filtering").get(),
                ire, order);
        // And with non-equality restrictions:
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where p1 > 0 group by p2 allow filtering").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where (c1,c2) > (0,0) group by p1, p2, c3").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t1 where p1>0 and p2=0 group by c1 allow filtering").get(), ire, order);
        // Even when GROUP BY lists all primary-key columns:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1, p2, p2, p3").get(), ire, order);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1, p2, p3, p1").get(), ire, order);
        // GROUP BY must list the entire partition key:
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1, p2").get(), ire, partition);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t2 group by p1").get(), ire, partition);
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("select * from t1 group by p1").get(), ire, partition);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_syntax_no_value_columns) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p1 int, p2 int, p3 int, primary key((p1, p2, p3)))");
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t group by p1, p2, p3, p1").get(),
                exceptions::invalid_request_exception,
                exception_predicate::message_contains("order"));
    });
}

namespace {

auto I(int32_t x) { return int32_type->decompose(x); }

auto L(int64_t x) { return long_type->decompose(x); }

auto T(const char* t) { return utf8_type->decompose(t); }

} // anonymous namespace

SEASTAR_TEST_CASE(test_group_by_aggregate_single_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, n int)");
        cquery_nofail(e, "insert into t (p, n) values (1, 10)");
        // Rows contain GROUP BY column values (later filtered in cql_server::connection).
        require_rows(e, "select sum(n) from t group by p", {{I(10), I(1)}});
        require_rows(e, "select avg(n) from t group by p", {{I(10), I(1)}});
        require_rows(e, "select count(n) from t group by p", {{L(1), I(1)}});
        cquery_nofail(e, "insert into t (p, n) values (2, 20)");
        require_rows(e, "select sum(n) from t group by p", {{I(10), I(1)}, {I(20), I(2)}});
        require_rows(e, "select avg(n) from t group by p", {{I(20), I(2)}, {I(10), I(1)}});
        require_rows(e, "select count(n) from t group by p", {{L(1), I(2)}, {L(1), I(1)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_aggregate_partition_only) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p1 int, p2 int, p3 int, v int, primary key((p1, p2, p3)))");
        cquery_nofail(e, "insert into t (p1, p2, p3, v) values (1, 1, 1, 100)");
        // Rows contain GROUP BY column values (later filtered in cql_server::connection).
        require_rows(e, "select sum(v) from t group by p1, p2, p3", {{I(100), I(1), I(1), I(1)}});
        cquery_nofail(e, "insert into t (p1, p2, p3, v) values (1, 2, 1, 100)");
        require_rows(e, "select sum(v) from t group by p1, p2, p3",
                     {{I(100), I(1), I(1), I(1)}, {I(100), I(1), I(2), I(1)}});
        require_rows(e, "select sum(v) from t where p2=2 group by p1, p3 allow filtering",
                     {{I(100), I(2), I(1), I(1)}});
        cquery_nofail(e, "insert into t (p1, p2, p3, v) values (1, 2, 2, 100)");
        require_rows(e, "select sum(v) from t group by p1, p2, p3",
                     {{I(100), I(1), I(1), I(1)}, {I(100), I(1), I(2), I(1)}, {I(100), I(1), I(2), I(2)}});
        cquery_nofail(e, "delete from t where p1=1 and p2=1 and p3=1");
        require_rows(e, "select sum(v) from t group by p1, p2, p3",
                     {{I(100), I(1), I(2), I(1)}, {I(100), I(1), I(2), I(2)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_aggregate_clustering) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p1 int, c1 int, c2 int, v int, primary key((p1), c1, c2))");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (1, 1, 1, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (1, 1, 2, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (1, 1, 3, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (2, 1, 1, 100)");
        cquery_nofail(e, "insert into t (p1, c1, c2, v) values (2, 2, 2, 100)");
        cquery_nofail(e, "delete from t where p1=1 and c1=1 and c2 =3");
        require_rows(e, "select sum(v) from t group by p1", {{I(200), I(1)}, {I(200), I(2)}});
        require_rows(e, "select sum(v) from t group by p1, c1",
                     {{I(200), I(1), I(1)}, {I(100), I(2), I(1)}, {I(100), I(2), I(2)}});
        require_rows(e, "select sum(v) from t where p1=1 and c1=1 group by c2 allow filtering",
                     {{I(100), I(1)}, {I(100), I(2)}});
        require_rows(e, "select sum(v) from t group by p1, c1, c2",
                     {{I(100), I(1), I(1), I(1)}, {I(100), I(1), I(1), I(2)},
                      {I(100), I(2), I(1), I(1)}, {I(100), I(2), I(2), I(2)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_text_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p text, c text, v int, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123', '1', 100)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123', '2', 200)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123', '3', 300)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123abc', '1', 150)");
        cquery_nofail(e, "insert into t (p, c, v) values ('123456789012345678901234567890123abc', '2', 250)");
        cquery_nofail(e, "insert into t (p, c, v) values ('ab', 'cd', 310)");
        cquery_nofail(e, "insert into t (p, c, v) values ('abc', 'd', 420)");
        require_rows(e, "select sum(v) from t group by p",
                     {{I(600), T("123456789012345678901234567890123")},
                      {I(400), T("123456789012345678901234567890123abc")},
                      {I(310), T("ab")},
                      {I(420), T("abc")}});
        require_rows(e, "select sum(v) from t where p in ('ab','abc') group by p, c allow filtering",
                     {{I(310), T("ab"), T("cd")}, {I(420), T("abc"), T("d")}});
        require_rows(e, "select sum(v) from t where p='123456789012345678901234567890123' group by c",
                     {{I(100), T("1")}, {I(200), T("2")}, {I(300), T("3")}});
        cquery_nofail(e, "create table t2 (p text, c1 text, c2 text, v int, primary key(p, c1, c2))");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', '', '', 10)");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', '', 'b', 20)");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', 'a', '', 30)");
        cquery_nofail(e, "insert into t2 (p, c1, c2, v) values (' ', 'a', 'b', 40)");
        require_rows(e, "select avg(v) from t2 group by p", {{I(25), T(" ")}});
        require_rows(e, "select avg(v) from t2 group by p, c1", {{I(15), T(" "), T("")}, {I(35), T(" "), T("a")}});
        require_rows(e, "select sum(v) from t2 where c1='' group by p, c2",
                     {{I(10), T(" "), T("")}, {I(20), T(" "), T("b")}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_non_aggregate) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, n int, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c, n) values (1, 1, 11)");
        require_rows(e, "select n from t group by p", {{I(11), I(1)}});
        cquery_nofail(e, "insert into t (p, c, n) values (2, 1, 21)");
        require_rows(e, "select n from t group by p", {{I(11), I(1)}, {I(21), I(2)}});
        cquery_nofail(e, "delete from t where p=1");
        require_rows(e, "select n from t group by p", {{I(21), I(2)}});
        cquery_nofail(e, "insert into t (p, c, n) values (1, 1, 11)");
        cquery_nofail(e, "insert into t (p, c, n) values (1, 2, 12)");
        cquery_nofail(e, "insert into t (p, c, n) values (1, 3, 13)");
        require_rows(e, "select n from t group by p", {{I(11), I(1)}, {I(21), I(2)}});
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_group_by_null_clustering) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, sv int static, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, sv) values (1, 100)"); // c will be NULL.
        require_rows(e, "select sv from t where p=1 group by c", {{I(100), std::nullopt}});
        return make_ready_future<>();
    });
}
