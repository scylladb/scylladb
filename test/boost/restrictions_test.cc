/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <boost/range/adaptors.hpp>
#include <experimental/source_location>
#include <fmt/format.h>
#include <seastar/testing/thread_test_case.hh>

#include "cql3/cql_config.hh"
#include "cql3/values.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/exception_utils.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace {

using std::experimental::source_location;
using boost::adaptors::transformed;

std::unique_ptr<cql3::query_options> to_options(
        const cql3::cql_config& cfg,
        std::optional<std::vector<sstring_view>> names,
        std::vector<cql3::raw_value> values) {
    static auto& d = cql3::query_options::DEFAULT;
    return std::make_unique<cql3::query_options>(
            cfg,
            d.get_consistency(), std::move(names), std::move(values), d.skip_metadata(),
            d.get_specific_options(), d.get_cql_serialization_format());
}

/// Asserts that e.execute_prepared(id, values) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  cql3::prepared_cache_key_type id,
                  std::optional<std::vector<sstring_view>> names,
                  const std::vector<bytes_opt>& values,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const std::experimental::source_location& loc = source_location::current()) {
    // This helps compiler pick the right overload for make_value:
    const auto rvals = values | transformed([] (const bytes_opt& v) { return cql3::raw_value::make_value(v); });
    cql3::cql_config cfg;
    auto opts = to_options(cfg, std::move(names), std::vector(rvals.begin(), rvals.end()));
    try {
        assert_that(e.execute_prepared_with_qo(id, std::move(opts)).get0()).is_rows().with_rows_ignore_order(expected);
    } catch (const std::exception& e) {
        BOOST_FAIL(format("execute_prepared failed: {}\n{}:{}: originally from here",
                          e.what(), loc.file_name(), loc.line()));
    }
}

auto I(int32_t x) { return int32_type->decompose(x); }
auto F(float f) { return float_type->decompose(f); }
auto T(const char* t) { return utf8_type->decompose(t); }

auto SI(const set_type_impl::native_type& val) {
    const auto int_set_type = set_type_impl::get_instance(int32_type, true);
    return int_set_type->decompose(make_set_value(int_set_type, val));
};

auto ST(const set_type_impl::native_type& val) {
    const auto text_set_type = set_type_impl::get_instance(utf8_type, true);
    return text_set_type->decompose(make_set_value(text_set_type, val));
};

auto LI(const list_type_impl::native_type& val) {
    const auto int_list_type = list_type_impl::get_instance(int32_type, true);
    return int_list_type->decompose(make_list_value(int_list_type, val));
}

auto LF(const list_type_impl::native_type& val) {
    const auto float_list_type = list_type_impl::get_instance(float_type, true);
    return float_list_type->decompose(make_list_value(float_list_type, val));
}

auto LT(const list_type_impl::native_type& val) {
    const auto text_list_type = list_type_impl::get_instance(utf8_type, true);
    return text_list_type->decompose(make_list_value(text_list_type, val));
}

/// Creates a table t with int columns p, q, and r.  Inserts data (i,10+i,20+i) for i = 0 to n.
void create_t_with_p_q_r(cql_test_env& e, size_t n) {
    cquery_nofail(e, "create table t (p int primary key, q int, r int)");
    for (size_t i = 0; i <= n; ++i) {
        cquery_nofail(e, fmt::format("insert into t (p,q,r) values ({},{},{});", i, 10+i, 20+i));
    }
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(regular_col_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        require_rows(e, "select q from t where q=12 allow filtering", {{I(12)}});
        require_rows(e, "select q from t where q=12 and q=12 allow filtering", {{I(12)}});
        require_rows(e, "select q from t where q=12 and q=13 allow filtering", {});
        require_rows(e, "select r from t where q=12 and p=2 allow filtering", {{I(22), I(12)}});
        require_rows(e, "select p from t where q=12 and r=22 allow filtering", {{I(2), I(12), I(22)}});
        require_rows(e, "select r from t where q=12 and p=2 and r=99 allow filtering", {});
        cquery_nofail(e, "insert into t(p) values (100)");
        require_rows(e, "select q from t where q=12 allow filtering", {{I(12)}});
        auto stmt = e.prepare("select q from t where q=? allow filtering").get0();
        require_rows(e, stmt, {}, {I(12)}, {{I(12)}});
        require_rows(e, stmt, {}, {I(99)}, {});
        stmt = e.prepare("select q from t where q=:q allow filtering").get0();
        require_rows(e, stmt, {{"q"}}, {I(12)}, {{I(12)}});
        require_rows(e, stmt, {{"q"}}, {I(99)}, {});
        stmt = e.prepare("select p from t where q=? and r=? allow filtering").get0();
        require_rows(e, stmt, {}, {I(12), I(22)}, {{I(2), I(12), I(22)}});
        require_rows(e, stmt, {}, {I(11), I(21)}, {{I(1), I(11), I(21)}});
        require_rows(e, stmt, {}, {I(11), I(22)}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m frozen<map<int,int>>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {1:11, 2:12, 3:13})");
        cquery_nofail(e, "insert into t (p, m) values (2, {1:21, 2:22, 3:23})");
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m1 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 11}, {2, 12}, {3, 13}})));
        require_rows(e, "select p from t where m={1:11, 2:12, 3:13} allow filtering", {{I(1), m1}});
        require_rows(e, "select p from t where m={1:11, 2:12} allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(set_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m frozen<set<int>>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {11,12,13})");
        cquery_nofail(e, "insert into t (p, m) values (2, {21,22,23})");
        require_rows(e, "select p from t where m={21,22,23} allow filtering", {{I(2), SI({21, 22, 23})}});
        require_rows(e, "select p from t where m={21,22,23,24} allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, li frozen<list<int>>)");
        cquery_nofail(e, "insert into t (p, li) values (1, [11,12,13])");
        cquery_nofail(e, "insert into t (p, li) values (2, [21,22,23])");
        require_rows(e, "select p from t where li=[21,22,23] allow filtering", {{I(2), LI({21, 22, 23})}});
        require_rows(e, "select p from t where li=[23,22,21] allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, li frozen<list<int>>)");
        cquery_nofail(e, "insert into t (p, li) values (1, [11,12,13])");
        cquery_nofail(e, "insert into t (p, li) values (2, [21,22,23])");
        require_rows(e, "select li from t where li<[23,22,21] allow filtering",
                     {{LI({11, 12, 13})}, {LI({21, 22, 23})}});
        require_rows(e, "select li from t where li>=[11,12,13] allow filtering",
                     {{LI({11, 12, 13})}, {LI({21, 22, 23})}});
        require_rows(e, "select li from t where li>[11,12,13] allow filtering", {{LI({21, 22, 23})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(tuple_of_list) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, l1 frozen<list<int>>, l2 frozen<list<int>>, primary key(p,l1,l2))");
        cquery_nofail(e, "insert into t (p, l1, l2) values (1, [11,12], [101,102])");
        cquery_nofail(e, "insert into t (p, l1, l2) values (2, [21,22], [201,202])");
        require_rows(e, "select * from t where (l1,l2)<([],[])", {});
        require_rows(e, "select l1 from t where (l1,l2)<([20],[200])", {{LI({11, 12})}});
        require_rows(e, "select l1 from t where (l1,l2)>=([11,12],[101,102])", {{LI({11, 12})}, {LI({21, 22})}});
        require_rows(e, "select l1 from t where (l1,l2)<([11,12],[101,103])", {{LI({11, 12})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_entry_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m map<int,int>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {1:11, 2:12, 3:13})");
        cquery_nofail(e, "insert into t (p, m) values (2, {1:21, 2:22, 3:23})");
        cquery_nofail(e, "insert into t (p, m) values (3, {1:31, 2:32, 3:33})");
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m2 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 21}, {2, 22}, {3, 23}})));
        require_rows(e, "select p from t where m[1]=21 allow filtering", {{I(2), m2}});
        require_rows(e, "select p from t where m[1]=21 and m[3]=23 allow filtering", {{I(2), m2}});
        require_rows(e, "select p from t where m[99]=21 allow filtering", {});
        require_rows(e, "select p from t where m[1]=99 allow filtering", {});
        cquery_nofail(e, "delete from t where p=2");
        require_rows(e, "select p from t where m[1]=21 allow filtering", {});
        require_rows(e, "select p from t where m[1]=21 and m[3]=23 allow filtering", {});
        const auto m3 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 31}, {2, 32}, {3, 33}})));
        require_rows(e, "select m from t where m[1]=31 allow filtering", {{m3}});
        cquery_nofail(e, "update t set m={1:111} where p=3");
        require_rows(e, "select p from t where m[1]=31 allow filtering", {});
        require_rows(e, "select p from t where m[1]=21 allow filtering", {});
        const auto m3new = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 111}})));
        require_rows(e, "select p from t where m[1]=111 allow filtering", {{I(3), m3new}});
        const auto stmt = e.prepare("select p from t where m[1]=:uno and m[3]=:tres allow filtering").get0();
        const auto m1 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 11}, {2, 12}, {3, 13}})));
        require_rows(e, stmt, {{"uno", "tres"}}, {I(11), I(13)}, {{I(1), m1}});
        require_rows(e, stmt, {{"uno", "tres"}}, {I(21), I(99)}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(regular_col_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        require_rows(e, "select q from t where q>12 allow filtering", {{I(13)}});
        require_rows(e, "select q from t where q<12 allow filtering", {{I(10)}, {I(11)}});
        require_rows(e, "select q from t where q>99 allow filtering", {});
        require_rows(e, "select r from t where q<12 and q>=11 allow filtering", {{I(21), I(11)}});
        require_rows(e, "select * from t where q<11 and q>11 allow filtering", {});
        require_rows(e, "select q from t where q<=12 and r>=21 allow filtering", {{I(11), I(21)}, {I(12), I(22)}});
        cquery_nofail(e, "insert into t(p) values (4)");
        require_rows(e, "select q from t where q<12 allow filtering", {{I(10)}, {I(11)}});
        require_rows(e, "select q from t where q>10 allow filtering", {{I(11)}, {I(12)}, {I(13)}});
        require_rows(e, "select q from t where q<12 and q>10 allow filtering", {{I(11)}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(regular_col_slice_reversed) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, primary key(p, c)) with clustering order by (c desc)");
        cquery_nofail(e, "insert into t(p,c) values (1,11)");
        require_rows(e, "select c from t where c>10", {{I(11)}});
        cquery_nofail(e, "insert into t(p,c) values (1,12)");
        require_rows(e, "select c from t where c>10", {{I(11)}, {I(12)}});
        require_rows(e, "select c from t where c<100", {{I(11)}, {I(12)}});
    }).get();
}

#if 0 // TODO: enable when supported.
SEASTAR_THREAD_TEST_CASE(regular_col_neq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        require_rows(e, "select q from t where q!=10 allow filtering", {{I(11)}, {I(12)}, {I(13)}});
        require_rows(e, "select q from t where q!=10 and q!=13 allow filtering", {{I(11)}, {I(12)}});
        require_rows(e, "select r from t where q!=11 and r!=22 allow filtering", {{I(10), I(20)}, {I(13), I(23)}});
    }).get();
}
#endif // 0

namespace {
/// The `op` argument can be one of: "<", "<=", ">", ">=", "=". Remove float args to use "!=".
future<> test_lhs_null_with_operator(sstring op) {
    return do_with_cql_env_thread([op = std::move(op)] (cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE cf (pk int primary key, flt float,"
                " dbl double, dec decimal, b boolean, ti tinyint, si smallint," 
                " i int, bi bigint, vi varint, ip inet, u uuid, tu timeuuid,"
                " t time, d date, a ascii, txt text);");
        cquery_nofail(e, "INSERT INTO cf (pk) VALUES (0);");
        
        // Now we have all types of NULLs
        require_rows(e, format("SELECT * FROM cf WHERE flt {} 99.9 ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE dbl {} 99.9 ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE dec {} 99.9 ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE b   {} true ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE ti  {} 99   ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE si  {} 999  ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE i   {} 999  ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE bi  {} 999  ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE vi  {} 999  ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE ip  {} '255.255.255.255' ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE u   {} ffffffff-e89b-12d3-a456-426655440000 ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE tu  {} ffffffff-e89b-12d3-a456-426655440000 ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE t   {} '23:59:59'   ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE d   {} '2137-01-01' ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE a   {} 'Z'          ALLOW FILTERING;", op), {});
        require_rows(e, format("SELECT * FROM cf WHERE txt {} 'Ż'          ALLOW FILTERING;", op), {});
    });
}
} // anon. namespace

// This test checks that NULLs on LHS of comparison operator never evaluate to `true` (#6295).
// The expected behavior aligns with C*, but the test also ensures that NULLs are not converted
// to empty buffers somewhere along the way (since empty buffers would lexicographically
// precede anything).
SEASTAR_THREAD_TEST_CASE(null_lhs) {
    test_lhs_null_with_operator("<").get();
    test_lhs_null_with_operator(">").get();
}

SEASTAR_THREAD_TEST_CASE(null_rhs) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (pk1 int, pk2 int, ck1 float, ck2 float, r text, primary key((pk1,pk2),ck1,ck2))");
        const auto q = [&] (const char* stmt) { return e.execute_cql(stmt).get(); };
        using ire = exceptions::invalid_request_exception;
        using exception_predicate::message_contains;
        const char* expect = "Invalid null";
        cquery_nofail(e, "insert into t (pk1, pk2, ck1, ck2) values (11, 12, 21, 22)");
        require_rows(e, "select * from t where pk1=0 and pk2=null", {});
        BOOST_REQUIRE_EXCEPTION(q("select * from t where pk1=0 and pk2=0 and (ck1,ck2)>=(0,null)"),
                                ire, message_contains(expect));
        require_rows(e, "select * from t where ck1=null", {});
        require_rows(e, "select * from t where r=null and ck1=null allow filtering", {});
        require_rows(e, "select * from t where pk1=0 and pk2=0 and ck1<null", {});
        require_rows(e, "select * from t where r>null and ck1<null allow filtering", {});
        cquery_nofail(e, "insert into t(pk1,pk2,ck1,ck2) values(11,21,101,201)");
        require_rows(e, "select * from t where r=null allow filtering", {});
        cquery_nofail(e, "insert into t(pk1,pk2,ck1,ck2,r) values(11,21,101,202,'2')");
        require_rows(e, "select * from t where r>null allow filtering", {});
        require_rows(e, "select * from t where r<=null allow filtering", {});
        require_rows(e, "select * from t where pk1=null allow filtering", {});

        cquery_nofail(e, "create table tb (p int primary key)");
        cquery_nofail(e, "insert into tb (p) values (1)");
        require_rows(e, "select * from tb where p=null", {});
    }).get();
}

/// Creates a tuple value from individual values.
bytes make_tuple(std::vector<data_type> types, std::vector<data_value> values) {
    const auto tuple_type = tuple_type_impl::get_instance(std::move(types));
    return tuple_type->decompose(
            make_tuple_value(tuple_type, tuple_type_impl::native_type(std::move(values))));
}

SEASTAR_THREAD_TEST_CASE(multi_col_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 text, c2 float, primary key (p, c1, c2))");
        cquery_nofail(e, "insert into t (p, c1, c2) values (1, 'one', 11);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (2, 'two', 12);");
        require_rows(e, "select c2 from t where p=1 and (c1,c2)=('one',11)", {{F(11)}});
        require_rows(e, "select c1 from t where p=1 and (c1)=('one')", {{T("one")}});
        require_rows(e, "select c2 from t where p=2 and (c1,c2)=('one',11)", {});
        require_rows(e, "select p from t where (c1,c2)=('two',12)", {{I(2)}});
        require_rows(e, "select c2 from t where (c1,c2)=('one',12)", {});
        require_rows(e, "select c2 from t where (c1,c2)=('two',11)", {});
        require_rows(e, "select c1 from t where (c1)=('one')", {{T("one")}});
        require_rows(e, "select c1 from t where (c1)=('x')", {});
        auto stmt = e.prepare("select p from t where (c1,c2)=:t").get0();
        require_rows(e, stmt, {{"t"}}, {make_tuple({utf8_type, float_type}, {sstring("two"), 12.f})}, {{I(2)}});
        require_rows(e, stmt, {{"t"}}, {make_tuple({utf8_type, float_type}, {sstring("x"), 12.f})}, {});
        stmt = e.prepare("select p from t where (c1,c2)=('two',?)").get0();
        require_rows(e, stmt, {}, {F(12)}, {{I(2)}});
        require_rows(e, stmt, {}, {F(99)}, {});
        stmt = e.prepare("select c1 from t where (c1)=?").get0();
        require_rows(e, stmt, {}, {make_tuple({utf8_type}, {sstring("one")})}, {{T("one")}});
        require_rows(e, stmt, {}, {make_tuple({utf8_type}, {sstring("two")})}, {{T("two")}});
        require_rows(e, stmt, {}, {make_tuple({utf8_type}, {sstring("three")})}, {});
        stmt = e.prepare("select c1 from t where (c1)=(:c1)").get0();
        require_rows(e, stmt, {{"c1"}}, {T("one")}, {{T("one")}});
        require_rows(e, stmt, {{"c1"}}, {T("two")}, {{T("two")}});
        require_rows(e, stmt, {{"c1"}}, {T("three")}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(multi_col_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 text, c2 float, primary key (p, c1, c2))");
        cquery_nofail(e, "insert into t (p, c1, c2) values (1, 'a', 11);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (2, 'b', 2);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (3, 'c', 13);");
        require_rows(e, "select c2 from t where (c1,c2)>('a',20)", {{F(2)}, {F(13)}});
        require_rows(e, "select p from t where (c1,c2)>=('a',20) and (c1,c2)<('b',3)", {{I(2)}});
        require_rows(e, "select * from t where (c1,c2)<('a',11)", {});
        require_rows(e, "select c1 from t where (c1,c2)<('a',12)", {{T("a")}});
        require_rows(e, "select c1 from t where (c1)>=('c')", {{T("c")}});
        require_rows(e, "select c1 from t where (c1,c2)<=('c',13)", {{T("a")}, {T("b")}, {T("c")}});
        require_rows(e, "select c1 from t where (c1,c2)>=('b',2) and (c1,c2)<=('b',2)", {{T("b")}});
        auto stmt = e.prepare("select c1 from t where (c1,c2)<?").get0();
        require_rows(e, stmt, {}, {make_tuple({utf8_type, float_type}, {sstring("a"), 12.f})}, {{T("a")}});
        require_rows(e, stmt, {}, {make_tuple({utf8_type, float_type}, {sstring("a"), 11.f})}, {});
        stmt = e.prepare("select c1 from t where (c1,c2)<('a',:c2)").get0();
        require_rows(e, stmt, {{"c2"}}, {F(12)}, {{T("a")}});
        require_rows(e, stmt, {{"c2"}}, {F(11)}, {});
        stmt = e.prepare("select c1 from t where (c1)>=?").get0();
        require_rows(e, stmt, {}, {make_tuple({utf8_type}, {sstring("c")})}, {{T("c")}});
        require_rows(e, stmt, {}, {make_tuple({utf8_type}, {sstring("x")})}, {});
        stmt = e.prepare("select c1 from t where (c1)>=(:c1)").get0();
        require_rows(e, stmt, {{"c1"}}, {T("c")}, {{T("c")}});
        require_rows(e, stmt, {{"c1"}}, {T("x")}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(multi_col_slice_reversed) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 int, c2 float, primary key (p, c1, c2)) "
                      "with clustering order by (c1 desc, c2 asc)");
        cquery_nofail(e, "insert into t(p,c1,c2) values (1,11,21)");
        cquery_nofail(e, "insert into t(p,c1,c2) values (1,12,22)");
        cquery_nofail(e, "insert into t(p,c1,c2) values (1,12,23)");
        require_rows(e, "select c1 from t where (c1,c2)>(10,99)", {{I(11)}, {I(12)}, {I(12)}});
        require_rows(e, "select c1 from t where (c1,c2)<(12,0)", {{I(11)}});
        require_rows(e, "select c1 from t where (c1,c2)>(12,22)", {{I(12)}});
        require_rows(e, "select c1 from t where (c1)>(12)", {});
        require_rows(e, "select c1 from t where (c1)<=(12)", {{I(11)}, {I(12)}, {I(12)}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(multi_and_single_together) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 int, c2 float, primary key (p, c1, c2)) "
                      "with clustering order by (c1 desc, c2 asc)");
        cquery_nofail(e, "insert into t(p,c1,c2) values (1,11,21)");
        cquery_nofail(e, "insert into t(p,c1,c2) values (1,12,22)");
        cquery_nofail(e, "insert into t(p,c1,c2) values (1,12,23)");
        const auto q = [&] (const char* stmt) { return e.execute_cql(stmt).get(); };
        using ire = exceptions::invalid_request_exception;
        const auto expected = exception_predicate::message_contains("Mixing");
        // TODO: one day mixing single- and multi-column will become allowed, and we'll have to change these:
        BOOST_REQUIRE_EXCEPTION(q("select c1 from t where c2=21 and (c1)>(10)"), ire, expected); // #7710
        BOOST_REQUIRE_EXCEPTION(q("select c2 from t where (c1,c2)>(10,99) and c2>=23"), ire, expected);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(set_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<set<int>>, c frozen<set<int>>, s set<text>, st set<int> static, primary key (p, c))");
        require_rows(e, "select * from t where c contains 222 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, s) values ({1}, {11, 12}, {'a1', 'b1'})");
        cquery_nofail(e, "insert into t (p, c, s) values ({2}, {21, 22}, {'a2', 'b1'})");
        cquery_nofail(e, "insert into t (p, c, s) values ({1, 3}, {31, 32}, {'a3', 'b3'})");
        require_rows(e, "select * from t where s contains 'xyz' allow filtering", {});
        require_rows(e, "select * from t where p contains 999 allow filtering", {});
        require_rows(e, "select p from t where p contains 3 allow filtering", {{SI({1, 3})}});
        require_rows(e, "select p from t where p contains 1 allow filtering", {{SI({1, 3})}, {SI({1})}});
        require_rows(e, "select p from t where p contains null allow filtering",
                         {{SI({1, 3})}, {SI({1})}, {SI({2})}});
        require_rows(e, "select p from t where p contains 1 and s contains 'a1' allow filtering",
                     {{SI({1}), ST({"a1", "b1"})}});
        require_rows(e, "select c from t where c contains 31 allow filtering", {{SI({31, 32})}});
        require_rows(e, "select c from t where c contains null allow filtering",
                         {{SI({11, 12})}, {SI({21, 22})}, {SI({31, 32})}});
        require_rows(e, "select c from t where c contains 11 and p contains 1 allow filtering",
                     {{SI({11, 12}), SI({1})}});
        require_rows(e, "select s from t where s contains 'a1' allow filtering", {{ST({"a1", "b1"})}});
        require_rows(e, "select s from t where s contains 'b1' allow filtering",
                     {{ST({"a1", "b1"})}, {ST({"a2", "b1"})}});
        require_rows(e, "select s from t where s contains null allow filtering",
                     {{ST({"a1", "b1"})}, {ST({"a2", "b1"})}, {ST({"a3", "b3"})}});
        require_rows(e, "select s from t where s contains 'b1' and s contains '' allow filtering", {});
        require_rows(e, "select s from t where s contains 'b1' and p contains 4 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, st) values ({4}, {41}, {104})");
        require_rows(e, "select st from t where st contains 4 allow filtering", {});
        require_rows(e, "select st from t where st contains 104 allow filtering", {{SI({104})}});
        cquery_nofail(e, "insert into t (p, c, st) values ({4}, {42}, {105})");
        require_rows(e, "select c from t where st contains 104 allow filtering", {});
        require_rows(e, "select c from t where st contains 105 allow filtering",
                     {{SI({41}), SI({105})}, {SI({42}), SI({105})}});
        cquery_nofail(e, "insert into t (p, c, st) values ({5}, {52}, {104, 105})");
        require_rows(e, "select p from t where st contains 105 allow filtering",
                     {{SI({4}), SI({105})}, {SI({4}), SI({105})}, {SI({5}), SI({104, 105})}});
        require_rows(e, "select p from t where st contains null allow filtering",
                     {{SI({4}), SI({105})}, {SI({4}), SI({105})}, {SI({5}), SI({104, 105})}});
        cquery_nofail(e, "delete from t where p={4}");
        require_rows(e, "select p from t where st contains 105 allow filtering", {{SI({5}), SI({104, 105})}});
        const auto stmt = e.prepare("select p from t where p contains :p allow filtering").get0();
        require_rows(e, stmt, {{"p"}}, {I(999)}, {});
        require_rows(e, stmt, {{"p"}}, {I(1)}, {{SI({1})}, {SI({1, 3})}});
        require_rows(e, stmt, {{"p"}}, {I(2)}, {{SI({2})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<list<int>>, c frozen<list<int>>, ls list<int>, st list<text> static,"
                      "primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c) values ([1], [11,12,13])");
        cquery_nofail(e, "insert into t (p, c, ls) values ([2], [21,22,23], [102])");
        cquery_nofail(e, "insert into t (p, c, ls, st) values ([3], [21,32,33], [103], ['a', 'b'])");
        cquery_nofail(e, "insert into t (p, c, st) values ([4], [41,42,43], ['a'])");
        cquery_nofail(e, "insert into t (p, c) values ([4], [41,42])");
        require_rows(e, "select p from t where p contains 222 allow filtering", {});
        require_rows(e, "select p from t where c contains 222 allow filtering", {});
        require_rows(e, "select p from t where ls contains 222 allow filtering", {});
        require_rows(e, "select p from t where st contains 'xyz' allow filtering", {});
        require_rows(e, "select p from t where p contains 1 allow filtering", {{LI({1})}});
        require_rows(e, "select p from t where p contains 4 allow filtering", {{LI({4})}, {LI({4})}});
        require_rows(e, "select p from t where p contains null allow filtering",
                         {{LI({1})}, {LI({2})}, {LI({3})}, {LI({4})}, {LI({4})}});
        require_rows(e, "select c from t where c contains 22 allow filtering", {{LI({21,22,23})}});
        require_rows(e, "select c from t where c contains 21 allow filtering", {{LI({21,22,23})}, {LI({21,32,33})}});
        require_rows(e, "select c from t where c contains null allow filtering",
                         {{LI({11,12,13})}, {LI({21,22,23})}, {LI({21,32,33})}, {LI({41,42,43})}, {LI({41,42})}});
        require_rows(e, "select c from t where c contains 21 and ls contains 102 allow filtering",
                     {{LI({21,22,23}), LI({102})}});
        require_rows(e, "select ls from t where ls contains 102 allow filtering", {{LI({102})}});
        require_rows(e, "select ls from t where ls contains null allow filtering", {{LI({102})}, {LI({103})}});
        require_rows(e, "select st from t where st contains 'a' allow filtering",
                         {{LT({"a"})}, {LT({"a"})}, {LT({"a", "b"})}});
        require_rows(e, "select st from t where st contains null allow filtering",
                         {{LT({"a"})}, {LT({"a"})}, {LT({"a", "b"})}});
        require_rows(e, "select st from t where st contains 'b' allow filtering", {{LT({"a", "b"})}});
        cquery_nofail(e, "delete from t where p=[2]");
        require_rows(e, "select c from t where c contains 21 allow filtering", {{LI({21,32,33})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<map<int,int>>, c frozen<map<int,int>>, m map<int,int>,"
                      "s map<int,int> static, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c, m) values ({1:1}, {10:10}, {1:11, 2:12})");
        require_rows(e, "select * from t where m contains 21 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, m) values ({2:2}, {20:20}, {1:21, 2:12})");
        cquery_nofail(e, "insert into t (p, c) values ({3:3}, {30:30})");
        cquery_nofail(e, "insert into t (p, c, s) values ({3:3}, {31:31}, {3:100})");
        cquery_nofail(e, "insert into t (p, c, s) values ({4:4}, {40:40}, {4:100})");
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto p3 = my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{3, 3}})));
        require_rows(e, "select p from t where p contains 3 allow filtering", {{p3}, {p3}});
        const auto p1 = my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{1, 1}})));
        const auto p2 = my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{2, 2}})));
        const auto p4 = my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{4, 4}})));
        require_rows(e, "select p from t where p contains null allow filtering", {{p1}, {p2}, {p3}, {p3}, {p4}});
        const auto c4 = my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{40, 40}})));
        require_rows(e, "select c from t where c contains 40 allow filtering", {{c4}});
        const auto m2 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 21}, {2, 12}})));
        require_rows(e, "select m from t where m contains 21 allow filtering", {{m2}});
        const auto m1 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 11}, {2, 12}})));
        require_rows(e, "select m from t where m contains 11 allow filtering", {{m1}});
        require_rows(e, "select m from t where m contains 12 allow filtering", {{m1}, {m2}});
        require_rows(e, "select m from t where m contains null allow filtering", {{m1}, {m2}});
        require_rows(e, "select m from t where m contains 11 and m contains 12 allow filtering", {{m1}});
        cquery_nofail(e, "delete from t where p={2:2}");
        require_rows(e, "select m from t where m contains 12 allow filtering", {{m1}});
        const auto s3 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{3, 100}})));
        const auto s4 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{4, 100}})));
        require_rows(e, "select s from t where s contains 100 allow filtering", {{s3}, {s3}, {s4}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(contains_key) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e,
                      "create table t (p frozen<map<int,int>>, c frozen<map<text,int>>, m map<int,int>, "
                      "s map<int,text> static, primary key(p, c))");
        cquery_nofail(e, "insert into t (p,c,m) values ({1:11, 2:12}, {'el':11, 'twel':12}, {11:11, 12:12})");
        require_rows(e, "select * from t where p contains key 3 allow filtering", {});
        require_rows(e, "select * from t where c contains key 'x' allow filtering", {});
        require_rows(e, "select * from t where m contains key 3 allow filtering", {});
        require_rows(e, "select * from t where s contains key 3 allow filtering", {});
        cquery_nofail(e, "insert into t (p,c,m) values ({3:33}, {'th':33}, {11:33})");
        const auto int_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m1 = int_map_type->decompose(
                make_map_value(int_map_type, map_type_impl::native_type({{11, 11}, {12, 12}})));
        const auto m3 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{11, 33}})));
        require_rows(e, "select m from t where m contains key 12 allow filtering", {{m1}});
        require_rows(e, "select m from t where m contains key 11 allow filtering", {{m1}, {m3}});
        require_rows(e, "select m from t where m contains key null allow filtering", {{m1}, {m3}});
        const auto text_map_type = map_type_impl::get_instance(utf8_type, int32_type, true);
        const auto c1 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"el", 11}, {"twel", 12}})));
        require_rows(e, "select c from t where c contains key 'el' allow filtering", {{c1}});
        require_rows(e, "select c from t where c contains key 'twel' allow filtering", {{c1}});
        const auto c3 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"th", 33}})));
        require_rows(e, "select c from t where c contains key null allow filtering", {{c1}, {c3}});
        const auto p3 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{3, 33}})));
        require_rows(e, "select p from t where p contains key 3 allow filtering", {{p3}});
        require_rows(e, "select p from t where p contains key 3 and m contains key null allow filtering", {{p3, m3}});
        const auto p1 = int_map_type->decompose(
                make_map_value(int_map_type, map_type_impl::native_type({{1, 11}, {2, 12}})));
        require_rows(e, "select p from t where p contains key null allow filtering", {{p1}, {p3}});
        cquery_nofail(e, "insert into t (p,c) values ({4:44}, {'aaaa':44})");
        require_rows(e, "select m from t where m contains key 12 allow filtering", {{m1}});
        cquery_nofail(e, "delete from t where p={1:11, 2:12}");
        require_rows(e, "select m from t where m contains key 12 allow filtering", {});
        require_rows(e, "select s from t where s contains key 55 allow filtering", {});
        cquery_nofail(e, "insert into t (p,c,s) values ({5:55}, {'aaaa':55}, {55:'aaaa'})");
        cquery_nofail(e, "insert into t (p,c,s) values ({5:55}, {'aaa':55}, {55:'aaaa'})");
        const auto int_text_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
        const auto s5 = int_text_map_type->decompose(
                make_map_value(int_text_map_type, map_type_impl::native_type({{55, "aaaa"}})));
        require_rows(e, "select s from t where s contains key 55 allow filtering", {{s5}, {s5}});
        require_rows(e, "select s from t where s contains key 55 and s contains key null allow filtering",
                         {{s5}, {s5}});
        const auto c51 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"aaaa", 55}})));
        const auto c52 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"aaa", 55}})));
        require_rows(e, "select c from t where s contains key 55 allow filtering", {{c51, s5}, {c52, s5}});
        cquery_nofail(e, "insert into t (p,c,s) values ({6:66}, {'bbb':66}, {66:'bbbb', 55:'bbbb'})");
        const auto p5 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{5, 55}})));
        const auto p6 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{6, 66}})));
        const auto s6 = int_text_map_type->decompose(
                make_map_value(int_text_map_type, map_type_impl::native_type({{55, "bbbb"}, {66, "bbbb"}})));
        require_rows(e, "select p from t where s contains key 55 allow filtering", {{p5, s5}, {p5, s5}, {p6, s6}});
        const auto stmt = e.prepare("select p from t where s contains key :k allow filtering").get0();
        require_rows(e, stmt, {{"k"}}, {I(55)}, {{p5, s5}, {p5, s5}, {p6, s6}});
        require_rows(e, stmt, {{"k"}}, {I(999)}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(like) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (pk text, ck1 text, ck2 text, r text, s text static, primary key (pk, ck1, ck2))");
        require_rows(e, "select * from t where pk like 'a' allow filtering", {});
        cquery_nofail(e, "insert into t (pk, ck1, ck2) values ('pa', 'c1a', 'c2a');");
        require_rows(e, "select * from t where pk like 'a' allow filtering", {});
        require_rows(e, "select pk from t where pk like '_a' allow filtering", {{T("pa")}});
        require_rows(e, "select pk from t where pk like '_a' and ck1 like '' allow filtering", {});
        require_rows(e, "select pk from t where r like '_a' allow filtering", {});
        require_rows(e, "select pk from t where pk like '_a' and ck2 like '_2%' allow filtering",
                         {{T("pa"), T("c2a")}});
        cquery_nofail(e, "insert into t (pk, ck1, ck2, r, s) values ('pb', 'c1b', 'c2b', 'rb', 'sb');");
        require_rows(e, "select pk from t where pk like '_a' allow filtering", {{T("pa")}});
        require_rows(e, "select r from t where r like '_a' allow filtering", {});
        require_rows(e, "select r from t where r like '_b' allow filtering", {{T("rb")}});
        cquery_nofail(e, "insert into t (pk, ck1, ck2, r) values ('pb', 'c1ba', 'c2ba', 'rba');");
        require_rows(e, "select r from t where r like 'rb%' allow filtering", {{T("rb")}, {T("rba")}});
        require_rows(e, "select pk from t where s like '_b%' allow filtering",
                         {{T("pb"), T("sb")}, {T("pb"), T("sb")}});
        cquery_nofail(e, "insert into t (pk, ck1, ck2, r, s) values ('pc', 'c1c', 'c2c', 'rc', 'sc');");
        require_rows(e, "select s from t where s like 's%' allow filtering", {{T("sb")}, {T("sb")}, {T("sc")}});
        require_rows(e, "select r from t where ck1 like '' allow filtering", {});
        require_rows(e, "select ck1 from t where ck1 like '%c' allow filtering", {{T("c1c")}});
        require_rows(e, "select ck2 from t where ck2 like 'c%' allow filtering",
                         {{T("c2a")}, {T("c2b")}, {T("c2ba")}, {T("c2c")}});
        require_rows(e, "select * from t where ck1 like '' and ck2 like '_2a' allow filtering", {});
        require_rows(e, "select r from t where r='rb' and ck2 like 'c2_' allow filtering", {{T("rb"), T("c2b")}});
        const auto stmt = e.prepare("select ck1 from t where ck1 like ? allow filtering").get0();
        require_rows(e, stmt, {}, {T("%c")}, {{T("c1c")}});
        require_rows(e, stmt, {}, {T("%xyxyz")}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(scalar_in) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, r float, s text static, primary key (p, c))");
        require_rows(e, "select c from t where c in (11,12,13) allow filtering", {});
        cquery_nofail(e, "insert into t(p,c) values (1,11)");
        require_rows(e, "select c from t where c in (11,12,13)", {{I(11)}});
        cquery_nofail(e, "insert into t(p,c,r) values (1,11,21)");
        cquery_nofail(e, "insert into t(p,c,r) values (2,12,22)");
        cquery_nofail(e, "insert into t(p,c,r) values (3,13,23)");
        cquery_nofail(e, "insert into t(p,c,r) values (4,14,24)");
        cquery_nofail(e, "insert into t(p,c,r,s) values (4,15,24,'34')");
        cquery_nofail(e, "insert into t(p,c,r,s) values (5,15,25,'35')");
        require_rows(e, "select c from t where c in (11,12,13)", {{I(11)}, {I(12)}, {I(13)}});
        require_rows(e, "select c from t where c in (11)", {{I(11)}});
        require_rows(e, "select c from t where c in (999)", {});
        require_rows(e, "select c from t where c in (11,999)", {{I(11)}});
        require_rows(e, "select c from t where c in (11,12,13) and r in (21,24) allow filtering", {{I(11), F(21)}});
        require_rows(e, "select c from t where c in (11,12,13) and r in (21,22) allow filtering",
                         {{I(11), F(21)}, {I(12), F(22)}});
        require_rows(e, "select r from t where r in (999) allow filtering", {});
        require_rows(e, "select r from t where r in (1,2,3) allow filtering", {});
        require_rows(e, "select r from t where r in (22,25) allow filtering", {{F(22)}, {F(25)}});
        require_rows(e, "select r from t where r in (22,25) and c < 20 allow filtering",
                         {{F(22), I(12)}, {F(25), I(15)}});
        require_rows(e, "select r from t where r in (22,25) and s>='25' allow filtering", {{F(25), T("35")}});
        require_rows(e, "select r from t where r in (25) and s>='25' allow filtering", {{F(25), T("35")}});
        require_rows(e, "select r from t where r in (25) allow filtering", {{F(25)}});
        require_rows(e, "select r from t where r in (null,25) allow filtering", {{F(25)}});
        cquery_nofail(e, "delete from t where p=2");
        require_rows(e, "select r from t where r in (22,25) allow filtering", {{F(25)}});
        require_rows(e, "select s from t where s in ('34','35') allow filtering", {{T("34")}, {T("34")}, {T("35")}});
        require_rows(e, "select s from t where s in ('34','35','999') allow filtering",
                         {{T("34")}, {T("34")}, {T("35")}});
        require_rows(e, "select s from t where s in ('34') allow filtering", {{T("34")}, {T("34")}});
        require_rows(e, "select s from t where s in ('34','35') and r=24 allow filtering",
                         {{T("34"), F(24)}, {T("34"), F(24)}});
        const auto stmt = e.prepare("select r from t where r in ? allow filtering").get0();
        require_rows(e, stmt, {}, {LF({99.f, 88.f, 77.f})}, {});
        require_rows(e, stmt, {}, {LF({21.f})}, {{F(21)}});
        require_rows(e, stmt, {}, {LF({21.f, 22.f, 23.f})}, {{F(21)}, {F(23)}});
        require_rows(e, stmt, {}, {LF({24.f, 25.f})}, {{F(24)}, {F(24)}, {F(25)}});
        require_rows(e, stmt, {}, {LF({25.f, data_value::make_null(float_type)})}, {{F(25)}});
        require_rows(e, stmt, {}, {LF({99.f, data_value::make_null(float_type)})}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_in) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<list<int>>, c frozen<list<int>>, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c) values ([1], [11,12,13])");
        cquery_nofail(e, "insert into t (p, c) values ([2], [21,22,23])");
        cquery_nofail(e, "insert into t (p, c) values ([3], [31,32,33])");
        cquery_nofail(e, "insert into t (p, c) values ([4], [41,42,43])");
        cquery_nofail(e, "insert into t (p, c) values ([4], [])");
        cquery_nofail(e, "insert into t (p, c) values ([5], [51,52,53])");
        require_rows(e, "select c from t where c in ([11,12],[11,13])", {});
        require_rows(e, "select c from t where c in ([11,12,13],[11,13,12])", {{LI({11,12,13})}});
        require_rows(e, "select c from t where c in ([11,12,13],[11,13,12],[41,42,43])",
                         {{LI({11,12,13})}, {LI({41,42,43})}});
        require_rows(e, "select c from t where p in ([1],[2],[4]) and c in ([11,12,13], [41,42,43])",
                         {{LI({11,12,13})}, {LI({41,42,43})}});
        require_rows(e, "select c from t where c in ([],[11,13,12])", {{LI({})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(set_in) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<set<int>>, c frozen<set<int>>, r text, primary key (p, c))");
        require_rows(e, "select * from t where c in ({222})", {});
        cquery_nofail(e, "insert into t (p, c) values ({1,11}, {21,201})");
        cquery_nofail(e, "insert into t (p, c, r) values ({1,11}, {22,202}, '2')");
        require_rows(e, "select * from t where c in ({222}, {21})", {});
        require_rows(e, "select c from t where c in ({222}, {21,201})", {{SI({21, 201})}});
        require_rows(e, "select c from t where c in ({22,202}, {21,201})", {{SI({21, 201})}, {SI({22, 202})}});
        require_rows(e, "select c from t where c in ({222}, {21,201}) and r='' allow filtering", {});
        require_rows(e, "select c from t where c in ({222}, {21,201}) and r='x' allow filtering", {});
        require_rows(e, "select c from t where c in ({22,202}, {21,201}) and r='2' allow filtering",
                         {{SI({22, 202}), T("2")}});
        require_rows(e, "select c from t where c in ({22,202}, {21,201}) and p in ({1,11}, {222})",
                         {{SI({21, 201})}, {SI({22, 202})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_in) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<map<int,int>>, c frozen<map<int,int>>, r int, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c) values ({1:1}, {10:10})");
        cquery_nofail(e, "insert into t (p, c, r) values ({1:1}, {10:10,11:11}, 12)");
        require_rows(e, "select * from t where c in ({10:11},{10:11},{11:11})", {});
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto c1a = my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{10, 10}})));
        require_rows(e, "select c from t where c in ({10:11}, {10:10}, {11:11})", {{c1a}});
        const auto c1b = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{10, 10}, {11, 11}})));
        require_rows(e, "select c from t where c in ({10:11}, {10:10}, {10:10,11:11})",
                         {{c1a}, {c1b}});
        require_rows(e, "select c from t where c in ({10:11}, {10:10}, {10:10,11:11}) and r=12 allow filtering",
                         {{c1b, I(12)}});
        require_rows(e, "select c from t where c in ({10:11}, {10:10}, {10:10,11:11}) and r in (12,null) "
                         "allow filtering", {{c1b, I(12)}});
        require_rows(e, "select c from t where c in ({10:11}, {10:10}, {10:10,11:11}) and p in ({1:1},{2:2})",
                     {{c1a}, {c1b}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(multi_col_in) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (pk int, ck1 int, ck2 float, r text, primary key (pk, ck1, ck2))");
        require_rows(e, "select ck1 from t where (ck1,ck2) in ((11,21),(12,22))", {});
        cquery_nofail(e, "insert into t(pk,ck1,ck2) values (1,11,21)");
        require_rows(e, "select ck1 from t where (ck1,ck2) in ((11,21),(12,22))", {{I(11)}});
        require_rows(e, "select ck1 from t where (ck1,ck2) in ((11,21))", {{I(11)}});
        cquery_nofail(e, "insert into t(pk,ck1,ck2) values (2,12,22)");
        require_rows(e, "select ck1 from t where (ck1,ck2) in ((11,21),(12,22))", {{I(11)}, {I(12)}});
        cquery_nofail(e, "insert into t(pk,ck1,ck2) values (3,13,23)");
        require_rows(e, "select ck1 from t where (ck1,ck2) in ((11,21),(12,22))", {{I(11)}, {I(12)}});
        require_rows(e, "select ck1 from t where (ck1,ck2) in ((13,23))", {{I(13)}});
        cquery_nofail(e, "insert into t(pk,ck1,ck2,r) values (4,13,23,'a')");
        require_rows(e, "select pk from t where (ck1,ck2) in ((13,23))", {{I(3)}, {I(4)}});
        require_rows(e, "select pk from t where (ck1) in ((13),(33),(44))", {{I(3)}, {I(4)}});
        // TODO: uncomment when #6200 is fixed.
        // require_rows(e, "select pk from t where (ck1,ck2) in ((13,23)) and r='a' allow filtering",
        //                  {{I(4), I(13), F(23), T("a")}});
        cquery_nofail(e, "delete from t where pk=4");
        require_rows(e, "select pk from t where (ck1,ck2) in ((13,23))", {{I(3)}});
        auto stmt = e.prepare("select ck1 from t where (ck1,ck2) in ?").get0();
        auto bound_tuples = [] (std::vector<std::tuple<int32_t, float>> tuples) {
            const auto tuple_type = tuple_type_impl::get_instance({int32_type, float_type});
            const auto list_type = list_type_impl::get_instance(tuple_type, true);
            const auto tvals = tuples | transformed([&] (const std::tuple<int32_t, float>& t) {
                return make_tuple_value(tuple_type, tuple_type_impl::native_type({std::get<0>(t), std::get<1>(t)}));
            });
            return list_type->decompose(
                    make_list_value(list_type, std::vector<data_value>(tvals.begin(), tvals.end())));
        };
        require_rows(e, stmt, {}, {bound_tuples({{11, 21}})}, {{I(11)}});
        require_rows(e, stmt, {}, {bound_tuples({{11, 21}, {11, 99}})}, {{I(11)}});
        require_rows(e, stmt, {}, {bound_tuples({{12, 22}})}, {{I(12)}});
        require_rows(e, stmt, {}, {bound_tuples({{13, 13}, {12, 22}})}, {{I(12)}});
        require_rows(e, stmt, {}, {bound_tuples({{12, 21}})}, {});
        require_rows(e, stmt, {}, {bound_tuples({{12, 21}, {12, 21}, {13, 21}, {14, 21}})}, {});
        stmt = e.prepare("select ck1 from t where (ck1,ck2) in (?)").get0();
        auto tpl = [] (int32_t e1, float e2) {
            return make_tuple({int32_type, float_type}, {e1, e2});
        };
        require_rows(e, stmt, {}, {tpl(11, 21)}, {{I(11)}});
        require_rows(e, stmt, {}, {tpl(12, 22)}, {{I(12)}});
        require_rows(e, stmt, {}, {tpl(12, 21)}, {});
        stmt = e.prepare("select ck1 from t where (ck1,ck2) in (:t1,:t2)").get0();
        require_rows(e, stmt, {{"t1", "t2"}}, {tpl(11, 21), tpl(12, 22)}, {{I(11)}, {I(12)}});
        require_rows(e, stmt, {{"t1", "t2"}}, {tpl(11, 21), tpl(11, 21)}, {{I(11)}});
        require_rows(e, stmt, {{"t1", "t2"}}, {tpl(11, 21), tpl(99, 99)}, {{I(11)}});
        require_rows(e, stmt, {{"t1", "t2"}}, {tpl(9, 9), tpl(99, 99)}, {});
        // Parsing error:
        // stmt = e.prepare("select ck1 from t where (ck1,ck2) in ((13,23),:p1)").get0();
        stmt = e.prepare("select ck1 from t where (ck1,ck2) in ((13,23),(?,?))").get0();
        require_rows(e, stmt, {}, {I(0), F(0)}, {{I(13)}});
        require_rows(e, stmt, {}, {I(11), F(21)}, {{I(11)}, {I(13)}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(bounds) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, primary key (p, c))");
        cquery_nofail(e, "insert into t (p, c) values (1, 11);");
        cquery_nofail(e, "insert into t (p, c) values (2, 12);");
        cquery_nofail(e, "insert into t (p, c) values (3, 13);");
        require_rows(e, "select p from t where p=1 and c > 10", {{I(1)}});
        require_rows(e, "select p from t where p=1 and c = 11", {{I(1)}});
        require_rows(e, "select p from t where p=1 and (c) >= (10)", {{I(1)}});
        require_rows(e, "select p from t where p=1 and (c) = (11)", {{I(1)}});
        require_rows(e, "select c from t where p in (1,2,3) and c > 11 and c < 13", {{I(12)}});
        require_rows(e, "select c from t where p in (1,2,3) and c >= 11 and c < 13", {{I(11)}, {I(12)}});
        auto stmt = e.prepare("select c from t where p in (1,2,3) and c >= 11 and c < ?").get0();
        require_rows(e, stmt, {}, {I(13)}, {{I(11)}, {I(12)}});
        require_rows(e, stmt, {}, {I(10)}, {});
        stmt = e.prepare("select c from t where p in (1,2,3) and (c) < ?").get0();
        require_rows(e, stmt, {}, {make_tuple({int32_type}, {13})}, {{I(11)}, {I(12)}});
        require_rows(e, stmt, {}, {make_tuple({int32_type}, {11})}, {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(bounds_reversed) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (pk int, ck1 int, ck2 int, primary key (pk, ck1, ck2)) "
                      "with clustering order by (ck1 asc, ck2 desc)");
        cquery_nofail(e, "insert into t (pk,ck1,ck2) values (1,11,21);");
        cquery_nofail(e, "insert into t (pk,ck1,ck2) values (2,12,22);");
        require_rows(e, "select pk from t where pk=1 and ck1>10", {{I(1)}});
        require_rows(e, "select pk from t where pk=1 and ck1=11", {{I(1)}});
        require_rows(e, "select pk from t where pk=1 and (ck1)>=(10)", {{I(1)}});
        require_rows(e, "select pk from t where pk=1 and (ck1,ck2)>=(10,30)", {{I(1)}});
        require_rows(e, "select pk from t where pk=1 and (ck1,ck2)>=(10,30) and (ck1)<(20)", {{I(1)}});
        require_rows(e, "select pk from t where pk=1 and (ck1,ck2)>=(10,20) and (ck1,ck2)<(20,21)", {{I(1)}});
        require_rows(e, "select pk from t where pk=1 and (ck1,ck2)>=(10,20) and (ck1,ck2)<=(11,20)", {});
        require_rows(e, "select pk from t where pk=1 and (ck1)=(11)", {{I(1)}});
        require_rows(e, "select pk from t where pk=1 and (ck1,ck2)=(11,21)", {{I(1)}});
        cquery_nofail(e, "insert into t (pk,ck1,ck2) values (2,12,23);");
        require_rows(e, "select ck1 from t where pk in (1,2,3) and ck1=12 and ck2<23", {{I(12)}});
        require_rows(e, "select ck1 from t where pk in (1,2,3) and ck1=12 and ck2<24", {{I(12)}, {I(12)}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(multi_eq_on_primary) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, primary key (p, c))");
        cquery_nofail(e, "insert into t (p, c) values (1, 11);");
        cquery_nofail(e, "insert into t (p, c) values (2, 12);");
        cquery_nofail(e, "insert into t (p, c) values (3, 13);");
        require_rows(e, "select p from t where p=1 and p=1", {{I(1)}});
        require_rows(e, "select p from t where p=1 and p=2", {});
        require_rows(e, "select c from t where c=11 and c=11", {{I(11)}});
        require_rows(e, "select c from t where c=11 and c=999", {});

        cquery_nofail(e, "create table t2 (pk1 int, pk2 int, ck1 int, ck2 int, primary key ((pk1, pk2), ck1, ck2))");
        cquery_nofail(e, "insert into t2 (pk1, pk2, ck1, ck2) values (1, 12, 21, 22);");
        cquery_nofail(e, "insert into t2 (pk1, pk2, ck1, ck2) values (1, 12, 210, 220);");
        require_rows(e, "select pk1 from t2 where pk1=1 and pk1=1 and pk2=12", {{I(1)}, {I(1)}});
        require_rows(e, "select pk1 from t2 where pk1=0 and pk1=1 and pk2=12", {});
        require_rows(e, "select pk1 from t2 where pk1=1 and pk2=12 and ck1=21 and ck1=21", {{I(1)}});
        require_rows(e, "select pk1 from t2 where pk1=1 and pk2=12 and ck1=21 and ck1=21 and ck1=21", {{I(1)}});
        require_rows(e, "select pk1 from t2 where pk1=1 and pk2=12 and ck1=21 and ck1=210", {});
        require_rows(e, "select pk1 from t2 where pk1=1 and pk2=12 and ck1=21 and ck1=210 and ck1=210", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(token) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, q int, r int, primary key ((p, q)))");
        cquery_nofail(e, "insert into t (p,q,r) values (1,11,101);");
        cquery_nofail(e, "insert into t (p,q,r) values (2,12,102);");
        cquery_nofail(e, "insert into t (p,q,r) values (3,13,103);");
        require_rows(e, "select p from t where token(p,q) = token(1,11)", {{I(1)}});
        require_rows(e, "select p from t where token(p,q) >= token(1,11) and token(p,q) <= token(1,11)", {{I(1)}});

        // WARNING: the following two cases rely on having no token collisions, which cannot be guaranteed.
        // Keeping them because (absent collisions) they complete code coverage, guarding against
        // hard-to-trigger bugs.
        require_rows(e, "select p from t where token(p,q) = token(1,11) and token(p,q) = token(2,12)", {});
        require_rows(e, "select p from t where token(p,q) <= token(1,11) and token(p,q) = token(2,12)", {{I(2)}});

        require_rows(e, "select p from t where token(p,q) > token(9,9) and token(p,q) < token(9,9)", {});
        const auto min_bounds = format("select p from t where token(p,q) > {:d} and token(p,q) < {:d}",
               std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min());
        require_rows(e, min_bounds, {{I(1)}, {I(2)}, {I(3)}});
        require_rows(e, "select p from t where token(p,q) <= token(1,11) and r<102 allow filtering",
                         {{I(1), I(101)}});
        require_rows(e, "select p from t where token(p,q) = token(2,12) and r<102 allow filtering", {});
        const auto stmt = e.prepare("select p from t where token(p,q) = token(1,?)").get0();
        require_rows(e, stmt, {}, {I(11)}, {{I(1)}});
        require_rows(e, stmt, {}, {I(10)}, {});

        const auto q = [&] (const char* stmt) { return e.execute_cql(stmt).get(); };
        using ire = exceptions::invalid_request_exception;
        using exception_predicate::message_contains;
        const char* expect = "cannot be restricted by both a normal relation and a token relation";

        BOOST_REQUIRE_EXCEPTION(q("select p from t where p = 0 and token(p, q) <= token(1,11)"), ire, message_contains(expect));
        BOOST_REQUIRE_EXCEPTION(q("select p from t where token(p, q) <= token(1,11) and p = 0"), ire, message_contains(expect));
        BOOST_REQUIRE_EXCEPTION(q("select p from t where p in (0,1) and token(p, q) <= token(1,11)"), ire, message_contains(expect));
        BOOST_REQUIRE_EXCEPTION(q("select p from t where token(p, q) <= token(1,11) and p in (0,1)"), ire, message_contains(expect));
        BOOST_REQUIRE_EXCEPTION(q("select p from t where p = 0 and q = 0 and token(p, q) <= token(1,11)"), ire, message_contains(expect));
        BOOST_REQUIRE_EXCEPTION(q("select p from t where token(p, q) <= token(1,11) and p = 0 and q = 0"), ire, message_contains(expect));
    }).get();
}

SEASTAR_THREAD_TEST_CASE(tuples) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, q tuple<int>, r tuple<int,int>)");
        cquery_nofail(e, "insert into t (p,q,r) values (1,(1),(1,1));");
        cquery_nofail(e, "insert into t (p,q,r) values (2,(2),(2,2));");
        require_rows(e, "select r from t where r=(1,1) allow filtering",
                     {{make_tuple({int32_type, int32_type}, {1, 1})}});
        require_rows(e, "select r from t where r<(2,2) allow filtering",
                     {{make_tuple({int32_type, int32_type}, {1, 1})}});
        require_rows(e, "select r from t where r<(1,99) allow filtering",
                     {{make_tuple({int32_type, int32_type}, {1, 1})}});
        require_rows(e, "select r from t where r<=(2, 2) allow filtering",
                     {{make_tuple({int32_type, int32_type}, {1, 1})}, {make_tuple({int32_type, int32_type}, {2, 2})}});
        require_rows(e, "select q from t where q=(1) allow filtering", {{make_tuple({int32_type}, {1})}});
        require_rows(e, "select q from t where q>=(1) allow filtering",
                     {{make_tuple({int32_type}, {1})}, {make_tuple({int32_type}, {2})}});
        require_rows(e, "select q from t where q>=(2) allow filtering", {{make_tuple({int32_type}, {2})}});
        require_rows(e, "select q from t where q>(99) allow filtering", {});
    }).get();
}
