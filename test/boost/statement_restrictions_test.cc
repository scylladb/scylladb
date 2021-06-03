/*
 * Copyright (C) 2021-present ScyllaDB
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


#include <seastar/testing/test_case.hh>

#include <vector>

#include "cql3/relation.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/util.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"

using namespace cql3;

namespace {

/// Returns statement_restrictions::get_clustering_bounds() of where_clause, with reasonable defaults in
/// boilerplate.
query::clustering_row_ranges slice(
        const std::vector<relation_ptr>& where_clause, cql_test_env& env,
        const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    variable_specifications bound_names;
    return restrictions::statement_restrictions(
            env.local_db(),
            env.local_db().find_schema(keyspace_name, table_name),
            statements::statement_type::SELECT,
            where_clause,
            bound_names,
            /*contains_only_static_columns=*/false,
            /*for_view=*/false,
            /*allow_filtering=*/true)
            .get_clustering_bounds(query_options({}));
}

/// Overload that parses the WHERE clause from string.  Named differently to disambiguate when where_clause is
/// brace-initialized.
query::clustering_row_ranges slice_parse(
        sstring_view where_clause, cql_test_env& env,
        const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    return slice(cql3::util::where_clause_to_relations(where_clause), env, table_name, keyspace_name);
}

auto I(int32_t x) { return int32_type->decompose(x); }

auto T(const char* t) { return utf8_type->decompose(t); }

const auto open_ended = query::clustering_range::make_open_ended_both_sides();

auto singular(std::vector<bytes> values) {
    return query::clustering_range::make_singular(clustering_key_prefix(move(values)));
}

constexpr bool inclusive = true, exclusive = false;

auto left_open(std::vector<bytes> lb) {
    return query::clustering_range::make_starting_with({clustering_key_prefix(move(lb)), exclusive});
}

auto left_closed(std::vector<bytes> lb) {
    return query::clustering_range::make_starting_with({clustering_key_prefix(move(lb)), inclusive});
}

auto right_open(std::vector<bytes> ub) {
    return query::clustering_range::make_ending_with({clustering_key_prefix(move(ub)), exclusive});
}

auto right_closed(std::vector<bytes> ub) {
    return query::clustering_range::make_ending_with({clustering_key_prefix(move(ub)), inclusive});
}

auto left_open_right_closed(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(move(lb)), ckub(move(ub));
    return query::clustering_range({{cklb, exclusive}}, {{ckub, inclusive}});
}

auto left_closed_right_open(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(move(lb)), ckub(move(ub));
    return query::clustering_range({{cklb, inclusive}}, {{ckub, exclusive}});
}

auto both_open(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(move(lb)), ckub(move(ub));
    return query::clustering_range({{cklb, exclusive}}, {{ckub, exclusive}});
}

auto both_closed(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(move(lb)), ckub(move(ub));
    return query::clustering_range({{cklb, inclusive}}, {{ckub, inclusive}});
}

} // anonymous namespace

SEASTAR_TEST_CASE(slice_empty_restriction) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(p int, c int, primary key(p,c))");
        BOOST_CHECK_EQUAL(slice(/*where_clause=*/{}, e), std::vector{open_ended});
    });
}

SEASTAR_TEST_CASE(slice_one_column) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(p int, c text, primary key(p,c))");

        BOOST_CHECK_EQUAL(slice_parse("p=1", e), std::vector{open_ended});

        BOOST_CHECK_EQUAL(slice_parse("c='123'", e), std::vector{singular({T("123")})});
        BOOST_CHECK_EQUAL(slice_parse("c='a' and c='a'", e), std::vector{singular({T("a")})});
        BOOST_CHECK_EQUAL(slice_parse("c='a' and c='b'", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c like '123'", e), std::vector{open_ended});

        BOOST_CHECK_EQUAL(slice_parse("c in ('x','y','z')", e),
                          (std::vector{singular({T("x")}), singular({T("y")}), singular({T("z")})}));
        BOOST_CHECK_EQUAL(slice_parse("c in ('x')", e), std::vector{singular({T("x")})});
        BOOST_CHECK_EQUAL(slice_parse("c in ()", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c in ('x','y') and c in ('a','b')", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c in ('x','y') and c='z'", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c in ('x','y') and c='x'", e), std::vector{singular({T("x")})});
        BOOST_CHECK_EQUAL(slice_parse("c in ('a','b','c','b','a')", e), std::vector({
                    singular({T("a")}), singular({T("b")}), singular({T("c")})}));

        BOOST_CHECK_EQUAL(slice_parse("c>'x'", e), std::vector{left_open({T("x")})});
        BOOST_CHECK_EQUAL(slice_parse("c>='x'", e), std::vector{left_closed({T("x")})});
        BOOST_CHECK_EQUAL(slice_parse("c<'x'", e), std::vector{right_open({T("x")})});
        BOOST_CHECK_EQUAL(slice_parse("c<='x'", e), std::vector{right_closed({T("x")})});
    });
}

SEASTAR_TEST_CASE(slice_two_columns) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(p int, c1 int, c2 text, primary key(p,c1,c2))");

        BOOST_CHECK_EQUAL(slice_parse("c1=123 and c2='321'", e), std::vector{singular({I(123), T("321")})});
        BOOST_CHECK_EQUAL(slice_parse("c1=123", e), std::vector{singular({I(123)})});
        BOOST_CHECK_EQUAL(slice_parse("c1=123 and c2 like '321'", e), std::vector{singular({I(123)})});
        BOOST_CHECK_EQUAL(slice_parse("c1=123 and c1=123", e), std::vector{singular({I(123)})});
        BOOST_CHECK_EQUAL(slice_parse("c2='abc'", e), std::vector{open_ended});
        BOOST_CHECK_EQUAL(slice_parse("c1=0 and c1=1 and c2='a'", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c1=0 and c2='a' and c1=0", e), std::vector{singular({I(0), T("a")})});

        BOOST_CHECK_EQUAL(slice_parse("c2='abc' and c1 in (1,2,3)", e),
                          (std::vector{
                              singular({I(1), T("abc")}),
                              singular({I(2), T("abc")}),
                              singular({I(3), T("abc")})}));
        BOOST_CHECK_EQUAL(slice_parse("c1 in (1,2) and c2='x'", e),
                          (std::vector{
                              singular({I(1), T("x")}),
                              singular({I(2), T("x")})}));
        BOOST_CHECK_EQUAL(slice_parse("c1 in (1,2) and c2 in ('x','y')", e),
                          (std::vector{
                              singular({I(1), T("x")}), singular({I(1), T("y")}),
                              singular({I(2), T("x")}), singular({I(2), T("y")})}));
        BOOST_CHECK_EQUAL(slice_parse("c1 in (1) and c1 in (1) and c2 in ('x', 'y')", e),
                          (std::vector{singular({I(1), T("x")}), singular({I(1), T("y")})}));
        BOOST_CHECK_EQUAL(slice_parse("c1 in (1) and c1 in (2) and c2 in ('x')", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c1 in (1) and c2='x'", e), std::vector{singular({I(1), T("x")})});
        BOOST_CHECK_EQUAL(slice_parse("c1 in () and c2='x'", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c2 in ('x','y')", e), std::vector{open_ended});
        BOOST_CHECK_EQUAL(slice_parse("c1 in (1,2,3)", e),
                          (std::vector{singular({I(1)}), singular({I(2)}), singular({I(3)})}));
        BOOST_CHECK_EQUAL(slice_parse("c1 in (1)", e), std::vector{singular({I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("c1 in ()", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("c2 like 'a' and c1 in (1,2)", e),
                          (std::vector{singular({I(1)}), singular({I(2)})}));

        BOOST_CHECK_EQUAL(slice_parse("c1=123 and c2>'321'", e), std::vector{
                left_open_right_closed({I(123), T("321")}, {I(123)})});
        BOOST_CHECK_EQUAL(slice_parse("c1<123 and c2>'321'", e), std::vector{right_open({I(123)})});
        BOOST_CHECK_EQUAL(slice_parse("c1>=123 and c2='321'", e), std::vector{left_closed({I(123)})});
    });
}

SEASTAR_TEST_CASE(slice_multi_column) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(p int, c1 int, c2 int, c3 int, primary key(p,c1,c2,c3))");
        BOOST_CHECK_EQUAL(slice_parse("(c1)=(1)", e), std::vector{singular({I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2)=(1,2)", e), std::vector{singular({I(1), I(2)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)=(1,2,3)", e), std::vector{singular({I(1), I(2), I(3)})});
        // TODO: Uncomment when supported:
        // BOOST_CHECK_EQUAL(slice_parse("(c1)=(1) and (c1)=(2)", e), query::clustering_row_ranges{});
        // BOOST_CHECK_EQUAL(slice_parse("(c1,c2)=(1,2) and (c1,c2)>(2)", e), query::clustering_row_ranges{});
        // BOOST_CHECK_EQUAL(slice_parse("(c1,c2)=(1,2) and (c1,c2)=(1,2)", e),
        //                   std::vector{singular({I(1), I(2)})});

        BOOST_CHECK_EQUAL(slice_parse("(c1)<(1)", e), std::vector{right_open({I(1)})});
        // TODO: Uncomment when supported:
        // BOOST_CHECK_EQUAL(slice_parse("(c1)<(1) and (c1)<=(3)", e), std::vector{right_open({I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1)>(0) and (c1)<=(1)", e), std::vector{
                left_open_right_closed({I(0)}, {I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2)>=(1,2)", e), std::vector{left_closed({I(1), I(2)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2)>=(1,2) and (c1)<(9)", e), std::vector{
                left_closed_right_open({I(1), I(2)}, {I(9)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2)>=(1,2) and (c1,c2)<=(11,12)", e),
                          std::vector{both_closed({I(1), I(2)}, {I(11), I(12)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,2,3)", e), std::vector{left_open({I(1), I(2), I(3)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,2,3) and (c1,c2,c3)<(1,2,3)", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,2,3) and (c1,c2,c3)<(10,20,30)", e),
                          std::vector{both_open({I(1), I(2), I(3)}, {I(10), I(20), I(30)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,2,3) and (c1,c2)<(10,20)", e),
                          std::vector{both_open({I(1), I(2), I(3)}, {I(10), I(20)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>=(1,2,3) and (c1,c2)<(1,2)", e), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2)>(1,2) and (c1,c2,c3)<=(1,2,3)", e), query::clustering_row_ranges{});

        BOOST_CHECK_EQUAL(slice_parse("(c1) IN ((1))", e), std::vector{singular({I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1) IN ((1),(10))", e), (std::vector{
                    singular({I(1)}), singular({I(10)})}));
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2) IN ((1,2),(10,20))", e), (std::vector{
                    singular({I(1), I(2)}), singular({I(10), I(20)})}));
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2) IN ((10,20),(1,2))", e), (std::vector{
                    singular({I(1), I(2)}), singular({I(10), I(20)})}));
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3) IN ((1,2,3),(10,20,30))", e), (std::vector{
                    singular({I(1), I(2), I(3)}), singular({I(10), I(20), I(30)})}));
    });
}

SEASTAR_TEST_CASE(slice_multi_column_mixed_order) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        // First two columns ascending:
        cquery_nofail(
                e,
                "create table t1(p int, c1 int, c2 int, c3 int, c4 int, primary key(p,c1,c2,c3,c4)) "
                "with clustering order by (c1 asc, c2 asc, c3 desc, c4 desc)");

        // Not mixed order:
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2)>(1,1) and (c1,c2)<(9,9)", e, "t1"), (std::vector{
                    both_open({I(1), I(1)}, {I(9), I(9)})}));

        // Same upper/lower bound lengths:
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2,c3)<(9,9,9)", e, "t1"), (std::vector{
                    // 1<c1<9
                    both_open({I(1)}, {I(9)}),
                    // or (c1=1 and c2>1)
                    left_open_right_closed({I(1), I(1)}, {I(1)}),
                    // or (c1=1 and c2=1 and c3>1)
                    left_closed_right_open({I(1), I(1)}, {I(1), I(1), I(1)}),
                    // or (c1=9 and c2<9)
                    left_closed_right_open({I(9)}, {I(9), I(9)}),
                    // or (c1=9 and c2=9 and c3<9)
                    left_open_right_closed({I(9), I(9), I(9)}, {I(9), I(9)})}));

        // Common initial values in lower/upper bound:
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2,c3)<(1,9,9)", e, "t1"), (std::vector{
                    // c1=1 and c2>1 and c2<9
                    both_open({I(1), I(1)}, {I(1), I(9)}),
                    // or c1=1 and c2=1 and c3>1
                    left_closed_right_open({I(1), I(1)}, {I(1), I(1), I(1)}),
                    // or c1=1 and c2=9 and c3<9
                    left_open_right_closed({I(1), I(9), I(9)}, {I(1), I(9)})}));
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>=(1,1,1) and (c1,c2,c3)<(1,9,9)", e, "t1"), (std::vector{
                    // c1=1 and c2>1 and c2<9
                    both_open({I(1), I(1)}, {I(1), I(9)}),
                    // or c1=1 and c2=1 and c3>=1
                    both_closed({I(1), I(1)}, {I(1), I(1), I(1)}),
                    // or c1=1 and c2=9 and c3<9
                    left_open_right_closed({I(1), I(9), I(9)}, {I(1), I(9)})}));
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2,c3)<=(1,9,9)", e, "t1"), (std::vector{
                    // c1=1 and c2>1 and c2<9
                    both_open({I(1), I(1)}, {I(1), I(9)}),
                    // or c1=1 and c2=1 and c3>1
                    left_closed_right_open({I(1), I(1)}, {I(1), I(1), I(1)}),
                    // or c1=1 and c2=9 and c3<=9
                    both_closed({I(1), I(9), I(9)}, {I(1), I(9)})}));
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>=(1,1,1) and (c1,c2,c3)<=(1,9,9)", e, "t1"), (std::vector{
                    // c1=1 and c2>1 and c2<9
                    both_open({I(1), I(1)}, {I(1), I(9)}),
                    // or c1=1 and c2=1 and c3>=1
                    both_closed({I(1), I(1)}, {I(1), I(1), I(1)}),
                    // or c1=1 and c2=9 and c3<=9
                    both_closed({I(1), I(9), I(9)}, {I(1), I(9)})}));
        // Same result for same inequalities in different order:
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)<=(1,9,9) and (c1,c2,c3)>=(1,1,1)", e, "t1"), (std::vector{
                    both_open({I(1), I(1)}, {I(1), I(9)}),
                    both_closed({I(1), I(1)}, {I(1), I(1), I(1)}),
                    both_closed({I(1), I(9), I(9)}, {I(1), I(9)})}));
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2,c3)<(1,1,9)", e, "t1"), std::vector{
                // c1=1 and c2=1 and 9>c3>1
                both_open({I(1), I(1), I(9)}, {I(1), I(1), I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2,c3)<(1,1,1)", e, "t1"),
                          query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2,c3)<=(1,1,1)", e, "t1"),
                          query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>=(1,1,1) and (c1,c2,c3)<(1,1,1)", e, "t1"),
                          query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>=(1,1,1) and (c1,c2,c3)<=(1,1,1)", e, "t1"), std::vector{
                singular({I(1), I(1), I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2)<(1,1)", e, "t1"), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>=(1,1,1) and (c1,c2)<(1,1)", e, "t1"), query::clustering_row_ranges{});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>(1,1,1) and (c1,c2)<=(1,1)", e, "t1"), std::vector{
                // c1=1 and c2=1 and c3>1
                left_closed_right_open({I(1), I(1)}, {I(1), I(1), I(1)})});

        // Equality:
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)=(1,1,1)", e, "t1"), std::vector{singular({I(1), I(1), I(1)})});
        // TODO: Uncomment when supported.
        // BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)=(1,1,1) and (c1,c2,c3)=(1,1,1)", e, "t1"), std::vector{
        //         singular({I(1), I(1), I(1)})});
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2,c3)>=(1,1,1) and (c1,c2,c3)<=(1,1,1)", e, "t1"), std::vector{
                singular({I(1), I(1), I(1)})});

        // First two columns descending:
        cquery_nofail(
                e,
                "create table t2(p int, c1 int, c2 int, c3 int, primary key(p,c1,c2,c3)) "
                "with clustering order by (c1 desc, c2 desc, c3 asc)");
        BOOST_CHECK_EQUAL(slice_parse("(c1,c2)>(1,1) and (c1,c2)<(9,9)", e, "t2"), std::vector{
                both_open({I(9), I(9)}, {I(1), I(1)})});

        // Alternating desc and asc:
        cquery_nofail(
                e,
                "create table t3 (p int, a int, b int, c int, d int, PRIMARY KEY (p, a, b, c, d)) "
                "with clustering order by (a desc, b asc, c desc, d asc);");
        BOOST_CHECK_EQUAL(slice_parse("(a,b,c,d)>=(0,1,2,3) and (a,b)<=(0,1)", e, "t3"), (std::vector{
                    // a=0 and b=1 and c>2
                    left_closed_right_open({I(0), I(1)}, {I(0), I(1), I(2)}),
                    // or a=0 and b=1 and c=2 and d>=3
                    both_closed({I(0), I(1), I(2), I(3)}, {I(0), I(1), I(2)})}));
        BOOST_CHECK_EQUAL(slice_parse("(a,b)>=(0,1)", e, "t3"), (std::vector{
                    // a>0
                    right_open({I(0)}),
                    // or a=0 and b>=1
                    both_closed({I(0), I(1)}, {I(0)})}));
        BOOST_CHECK_EQUAL(slice_parse("(a,b)>=SCYLLA_CLUSTERING_BOUND(0,1)", e, "t3"), std::vector{
                left_closed({I(0), I(1)})});
    });
}

SEASTAR_TEST_CASE(slice_single_column_mixed_order) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(
                e,
                "create table t (p int, a int, b int, c int, d int, PRIMARY KEY (p, a, b, c, d)) "
                "with clustering order by (a desc, b asc, c desc, d asc);");
        BOOST_CHECK_EQUAL(slice_parse("a in (1,2,3,2,1)", e), (std::vector{
                    singular({I(3)}), singular({I(2)}), singular({I(1)})}));
        BOOST_CHECK_EQUAL(slice_parse("a in (1,2,3,2,1) and b in (1,2,1)", e), (std::vector{
                    singular({I(3), I(1)}), singular({I(3), I(2)}),
                    singular({I(2), I(1)}), singular({I(2), I(2)}),
                    singular({I(1), I(1)}), singular({I(1), I(2)})}));
    });
}
