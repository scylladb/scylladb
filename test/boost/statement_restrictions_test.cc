/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include <vector>
#include <bit>

#include <fmt/ranges.h>

#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/util.hh"
#include "index/secondary_index_manager.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/test_utils.hh"

BOOST_AUTO_TEST_SUITE(statement_restrictions_test)

using namespace cql3;

namespace {

/// Returns statement_restrictions::get_clustering_bounds() of where_clause, with reasonable defaults in
/// boilerplate.
query::clustering_row_ranges slice(
        const expr::expression_list& where_clause, cql_test_env& env,
        const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    prepare_context ctx;
    return restrictions::analyze_statement_restrictions(
            env.data_dictionary(),
            env.local_db().find_schema(keyspace_name, table_name),
            statements::statement_type::SELECT,
            expr::conjunction{where_clause},
            ctx,
            /*contains_only_static_columns=*/false,
            /*for_view=*/false,
            /*allow_filtering=*/true,
            restrictions::check_indexes::yes)
            ->get_clustering_bounds(query_options({}));
}

/// Overload that parses the WHERE clause from string.  Named differently to disambiguate when where_clause is
/// brace-initialized.
query::clustering_row_ranges slice_parse(
        std::string_view where_clause, cql_test_env& env,
        const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    return slice(boolean_factors(cql3::util::where_clause_to_relations(where_clause, cql3::dialect{})), env, table_name, keyspace_name);
}

auto I(int32_t x) { return int32_type->decompose(x); }

auto T(const char* t) { return utf8_type->decompose(t); }

const auto open_ended = query::clustering_range::make_open_ended_both_sides();

auto singular(std::vector<bytes> values) {
    return query::clustering_range::make_singular(clustering_key_prefix(std::move(values)));
}

constexpr bool inclusive = true, exclusive = false;

auto left_open(std::vector<bytes> lb) {
    return query::clustering_range::make_starting_with({clustering_key_prefix(std::move(lb)), exclusive});
}

auto left_closed(std::vector<bytes> lb) {
    return query::clustering_range::make_starting_with({clustering_key_prefix(std::move(lb)), inclusive});
}

auto right_open(std::vector<bytes> ub) {
    return query::clustering_range::make_ending_with({clustering_key_prefix(std::move(ub)), exclusive});
}

auto right_closed(std::vector<bytes> ub) {
    return query::clustering_range::make_ending_with({clustering_key_prefix(std::move(ub)), inclusive});
}

auto left_open_right_closed(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(std::move(lb)), ckub(std::move(ub));
    return query::clustering_range({{cklb, exclusive}}, {{ckub, inclusive}});
}

auto left_closed_right_open(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(std::move(lb)), ckub(std::move(ub));
    return query::clustering_range({{cklb, inclusive}}, {{ckub, exclusive}});
}

auto both_open(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(std::move(lb)), ckub(std::move(ub));
    return query::clustering_range({{cklb, exclusive}}, {{ckub, exclusive}});
}

auto both_closed(std::vector<bytes> lb, std::vector<bytes> ub) {
    clustering_key_prefix cklb(std::move(lb)), ckub(std::move(ub));
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

// Regression test: verifies that index selection (find_idx), uses_secondary_indexing,
// and need_filtering produce consistent results across all supported index types:
//   - regular_values (standard column EQ)
//   - keys (set CONTAINS, map CONTAINS KEY)
//   - collection_values (map/list CONTAINS)
//   - keys_and_values (map subscript EQ)
//   - full (frozen collection EQ; round-trips to regular_values via serialization)
//   - local vs global index scoring
SEASTAR_TEST_CASE(index_selection) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.idx_test ("
                         "    pk1 int, pk2 int,"
                         "    ck1 int, ck2 int,"
                         "    v1 int, v2 int, v3 int,"
                         "    s1 set<int>,"
                         "    m1 map<int, text>,"
                         "    l1 list<int>,"
                         "    fs frozen<set<int>>,"
                         "    PRIMARY KEY ((pk1, pk2), ck1, ck2)"
                         ")");
        // 10 indexes covering all target types.
        cquery_nofail(e, "CREATE INDEX idx_v1         ON ks.idx_test(v1)");
        cquery_nofail(e, "CREATE INDEX idx_v2         ON ks.idx_test(v2)");
        cquery_nofail(e, "CREATE INDEX idx_v3_local   ON ks.idx_test((pk1,pk2), v3)");
        cquery_nofail(e, "CREATE INDEX idx_ck1        ON ks.idx_test(ck1)");
        cquery_nofail(e, "CREATE INDEX idx_s1         ON ks.idx_test(s1)");          // keys (rewritten from VALUES for sets)
        cquery_nofail(e, "CREATE INDEX idx_m1_values  ON ks.idx_test(VALUES(m1))");  // collection_values
        cquery_nofail(e, "CREATE INDEX idx_m1_keys    ON ks.idx_test(KEYS(m1))");    // keys
        cquery_nofail(e, "CREATE INDEX idx_m1_entries ON ks.idx_test(ENTRIES(m1))"); // keys_and_values
        cquery_nofail(e, "CREATE INDEX idx_l1         ON ks.idx_test(l1)");          // collection_values
        cquery_nofail(e, "CREATE INDEX idx_fs         ON ks.idx_test(FULL(fs))");    // full -> round-trips to regular_values

        auto schema = e.local_db().find_schema("ks", "idx_test");
        auto& sim = e.data_dictionary().find_column_family(schema).get_index_manager();

        struct expected {
            std::string_view where_clause;
            std::optional<sstring> index_name; // nullopt = no index selected
            bool uses_secondary_indexing;
            bool need_filtering;
        };

        // Build statement_restrictions from a WHERE clause string and return the
        // index-selection result.
        auto check = [&](std::string_view where_clause) -> expected {
            prepare_context ctx;
            auto factors = where_clause.empty()
                ? expr::expression_list{}
                : boolean_factors(cql3::util::where_clause_to_relations(where_clause, cql3::dialect{}));
            auto sr = restrictions::analyze_statement_restrictions(
                    e.data_dictionary(),
                    schema,
                    statements::statement_type::SELECT,
                    expr::conjunction{std::move(factors)},
                    ctx,
                    /*contains_only_static_columns=*/false,
                    /*for_view=*/false,
                    /*allow_filtering=*/true,
                    restrictions::check_indexes::yes);
            auto [idx, restrictions_expr] = sr->find_idx(sim);
            return {where_clause,
                    idx ? std::optional(idx->metadata().name()) : std::nullopt,
                    sr->uses_secondary_indexing(),
                    sr->need_filtering()};
        };

        auto none = std::optional<sstring>{};
        auto idx = [](const char* name) { return std::optional<sstring>(name); };

        auto verify = [](const expected& got, const expected& want) {
            BOOST_CHECK_MESSAGE(got.index_name == want.index_name,
                    fmt::format("WHERE {}: index_name: got {} want {}",
                                want.where_clause,
                                got.index_name.value_or("(none)"),
                                want.index_name.value_or("(none)")));
            BOOST_CHECK_MESSAGE(got.uses_secondary_indexing == want.uses_secondary_indexing,
                    fmt::format("WHERE {}: uses_secondary_indexing: got {} want {}",
                                want.where_clause,
                                got.uses_secondary_indexing,
                                want.uses_secondary_indexing));
            BOOST_CHECK_MESSAGE(got.need_filtering == want.need_filtering,
                    fmt::format("WHERE {}: need_filtering: got {} want {}",
                                want.where_clause,
                                got.need_filtering,
                                want.need_filtering));
        };

        // --- A. Regular column EQ (target_type: regular_values) ---
        verify(check("v1 = 1"), {"", idx("idx_v1"), true, false});
        verify(check("v2 = 1"), {"", idx("idx_v2"), true, false});
        // WHERE-clause order tiebreak: first column in WHERE wins for equal scores.
        verify(check("v1 = 1 AND v2 = 1"), {"", idx("idx_v1"), true, true});
        verify(check("v2 = 1 AND v1 = 1"), {"", idx("idx_v2"), true, true});
        // Slices (GT/LT) are not supported by standard secondary indexes.
        verify(check("v1 > 1"), {"", none, false, true});

        // --- B. Local vs global index scoring ---
        // Local index with full PK scores 2, global scores 1.
        verify(check("pk1 = 1 AND pk2 = 1 AND v3 = 1"), {"", idx("idx_v3_local"), true, false});
        // Local (score 2) beats global (score 1) even when global column appears first.
        verify(check("pk1 = 1 AND pk2 = 1 AND v1 = 1 AND v3 = 1"), {"", idx("idx_v3_local"), true, true});
        // Local index without full PK gets score 0 and is never picked.
        verify(check("v3 = 1"), {"", none, false, true});

        // --- C. CK column index (search group ordering) ---
        verify(check("ck1 = 1"), {"", idx("idx_ck1"), true, false});
        // CK group is iterated before non-PK group, regardless of WHERE order.
        verify(check("ck1 = 1 AND v1 = 1"), {"", idx("idx_ck1"), true, true});
        verify(check("v1 = 1 AND ck1 = 1"), {"", idx("idx_ck1"), true, true});

        // --- D. Set CONTAINS (target_type: keys, rewritten from VALUES for sets) ---
        verify(check("s1 CONTAINS 1"), {"", idx("idx_s1"), true, false});

        // --- E. Map indexes ---
        // CONTAINS on map values (target_type: collection_values).
        verify(check("m1 CONTAINS 'one'"), {"", idx("idx_m1_values"), true, false});
        // CONTAINS KEY on map keys (target_type: keys).
        verify(check("m1 CONTAINS KEY 1"), {"", idx("idx_m1_keys"), true, false});
        // Subscript EQ on map entries (target_type: keys_and_values).
        verify(check("m1[1] = 'one'"), {"", idx("idx_m1_entries"), true, false});

        // --- F. List CONTAINS (target_type: collection_values) ---
        verify(check("l1 CONTAINS 1"), {"", idx("idx_l1"), true, false});

        // --- G. Frozen collection (FULL index, round-trips to regular_values) ---
        verify(check("fs = {1}"), {"", idx("idx_fs"), true, false});
        // Same with full PK (local index on v3 is available but idx_fs is global, score 1).
        verify(check("pk1 = 1 AND pk2 = 1 AND fs = {1}"), {"", idx("idx_fs"), true, false});

        // --- H. Double CONTAINS on same column: CollectionYes && CollectionYes = No ---
        verify(check("s1 CONTAINS 1 AND s1 CONTAINS 2"), {"", none, false, true});

        // --- I. Collection + regular column tiebreak (WHERE-clause order) ---
        verify(check("s1 CONTAINS 1 AND v1 = 1"), {"", idx("idx_s1"), true, true});
        verify(check("v1 = 1 AND s1 CONTAINS 1"), {"", idx("idx_v1"), true, true});

        // --- J. CK group beats collection in non-PK group ---
        verify(check("m1 CONTAINS 'one' AND ck1 = 1"), {"", idx("idx_ck1"), true, true});

        // --- K. Edge cases ---
        // Full PK only: no secondary index needed.
        verify(check("pk1 = 1 AND pk2 = 1"), {"", none, false, false});
        // No restrictions at all.
        verify(check(""), {"", none, false, false});
        // Token restriction with a regular column: index is used.
        verify(check("token(pk1, pk2) > 0 AND v1 = 1"), {"", idx("idx_v1"), true, false});
    });
}

// Exhaustive combinatorial test: iterates over all 2^N subsets of N restriction
// fragments and, for each subset, verifies a broad set of statement_restrictions
// public APIs. This catches any refactoring that accidentally changes observable
// behaviour for *any* combination of restriction types.
//
// Restriction fragments (15 independent bits, 2^15 = 32768 combinations):
//   bit 0:  pk1 = 1
//   bit 1:  pk2 = 2
//   bit 2:  ck1 = 3              (single-column EQ)
//   bit 3:  ck2 > 4              (single-column slice)
//   bit 4:  ck1 IN (3, 6)        (single-column IN; includes CK1_EQ value 3)
//   bit 5:  (ck1, ck2) = (7, 8)  (multi-column EQ)
//   bit 6:  (ck1, ck2) > (9, 10) (multi-column slice)
//   bit 7:  (ck1, ck2) IN ((11, 12), (13, 14))  (multi-column IN)
//   bit 8:  v1 = 15              (global index comb_v1, target: regular_values)
//   bit 9:  v3 = 16              (local  index comb_v3_local, target: regular_values)
//   bit 10: s1 CONTAINS 17       (global index comb_s1, target: keys — set)
//   bit 11: m1 CONTAINS 'alpha'  (global index comb_m1_values, target: collection_values)
//   bit 12: m2 CONTAINS KEY 18   (global index comb_m2_keys, target: keys — map)
//   bit 13: m3[19] = 'beta'      (global index comb_m3_entries, target: keys_and_values)
//   bit 14: fs = {20, 21}        (global index comb_fs, target: full — frozen collection)
SEASTAR_TEST_CASE(combinatorial_restrictions) {
    // ASAN's fake-stack shadow buffer for the large lambda below is ~248 KiB;
    // bump the thread stack so it doesn't overflow under sanitized builds.
    seastar::thread_attributes tattr;
#if defined(__SANITIZE_ADDRESS__) || __has_feature(address_sanitizer)
    tattr.stack_size = 2 * 1024 * 1024;
#endif
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.comb ("
                         "    pk1 int, pk2 int,"
                         "    ck1 int, ck2 int,"
                         "    v1 int, v2 int, v3 int,"
                         "    s1 set<int>,"
                         "    m1 map<int, text>,"
                         "    m2 map<int, text>,"
                         "    m3 map<int, text>,"
                         "    fs frozen<set<int>>,"
                         "    PRIMARY KEY ((pk1, pk2), ck1, ck2)"
                         ")");
        cquery_nofail(e, "CREATE INDEX comb_pk1        ON ks.comb(pk1)");
        cquery_nofail(e, "CREATE INDEX comb_v1         ON ks.comb(v1)");
        cquery_nofail(e, "CREATE INDEX comb_v3_local   ON ks.comb((pk1,pk2), v3)");
        cquery_nofail(e, "CREATE INDEX comb_s1         ON ks.comb(s1)");
        cquery_nofail(e, "CREATE INDEX comb_ck1        ON ks.comb(ck1)");
        cquery_nofail(e, "CREATE INDEX comb_m1_values  ON ks.comb(VALUES(m1))");
        cquery_nofail(e, "CREATE INDEX comb_m2_keys    ON ks.comb(KEYS(m2))");
        cquery_nofail(e, "CREATE INDEX comb_m3_entries ON ks.comb(ENTRIES(m3))");
        cquery_nofail(e, "CREATE INDEX comb_fs         ON ks.comb(FULL(fs))");

        auto schema = e.local_db().find_schema("ks", "comb");
        auto& sim = e.data_dictionary().find_column_family(schema).get_index_manager();
        const auto& pk1_def = *schema->get_column_definition("pk1");
        const auto& pk2_def = *schema->get_column_definition("pk2");
        const auto& ck1_def = *schema->get_column_definition("ck1");
        const auto& ck2_def = *schema->get_column_definition("ck2");
        const auto& v1_def  = *schema->get_column_definition("v1");
        const auto& v3_def  = *schema->get_column_definition("v3");
        const auto& s1_def  = *schema->get_column_definition("s1");
        const auto& m1_def  = *schema->get_column_definition("m1");
        const auto& m2_def  = *schema->get_column_definition("m2");
        const auto& m3_def  = *schema->get_column_definition("m3");
        const auto& fs_def  = *schema->get_column_definition("fs");

        // Every restriction fragment is an independent bit in the mask.
        // This includes CK restriction variants, giving us exhaustive
        // coverage of all 2^15 = 32768 combinations.
        enum frag : unsigned {
            PK1        = 1u << 0,   // pk1 = 1  (global index, regular_values)
            PK2        = 1u << 1,   // pk2 = 2  (no index)
            CK1_EQ     = 1u << 2,   // ck1 = 3
            CK2_SLICE  = 1u << 3,   // ck2 > 4
            CK1_IN     = 1u << 4,   // ck1 IN (3, 6)
            MULTI_EQ   = 1u << 5,   // (ck1, ck2) = (7, 8)
            MULTI_SLICE= 1u << 6,   // (ck1, ck2) > (9, 10)
            MULTI_IN   = 1u << 7,   // (ck1, ck2) IN ((11, 12), (13, 14))
            V1         = 1u << 8,   // v1 = 15  (global index, regular_values)
            V3         = 1u << 9,   // v3 = 16  (local index, regular_values)
            S1         = 1u << 10,  // s1 CONTAINS 17  (global index, keys — set)
            M_VAL      = 1u << 11,  // m1 CONTAINS 'alpha' (global index, collection_values)
            M_KEY      = 1u << 12,  // m2 CONTAINS KEY 18 (global index, keys — map)
            M_ENT      = 1u << 13,  // m3[19] = 'beta' (global index, keys_and_values)
            FS         = 1u << 14,  // fs = {20, 21} (global index, full)
        };
        constexpr unsigned N_FRAG = 15;
        constexpr unsigned FRAG_TOTAL = 1u << N_FRAG;

        constexpr unsigned SINGLE_CK_MASK = CK1_EQ | CK2_SLICE | CK1_IN;
        constexpr unsigned MULTI_CK_MASK  = MULTI_EQ | MULTI_SLICE | MULTI_IN;

        struct fragment_info {
            unsigned bit;
            const char* clause;
        };
        // Each fragment uses unique values so that conjunction intersections
        // are predictable.  CK1_IN includes the CK1_EQ value (3) to ensure
        // the intersection is non-empty when both are present.
        const fragment_info fragments[] = {
            {PK1,         "pk1 = 1"},
            {PK2,         "pk2 = 2"},
            {CK1_EQ,      "ck1 = 3"},
            {CK2_SLICE,   "ck2 > 4"},
            {CK1_IN,      "ck1 IN (3, 6)"},
            {MULTI_EQ,    "(ck1, ck2) = (7, 8)"},
            {MULTI_SLICE, "(ck1, ck2) > (9, 10)"},
            {MULTI_IN,    "(ck1, ck2) IN ((11, 12), (13, 14))"},
            {V1,          "v1 = 15"},
            {V3,          "v3 = 16"},
            {S1,          "s1 CONTAINS 17"},
            {M_VAL,       "m1 CONTAINS 'alpha'"},
            {M_KEY,       "m2 CONTAINS KEY 18"},
            {M_ENT,       "m3[19] = 'beta'"},
            {FS,          "fs = {20, 21}"},
        };

        unsigned total_tested = 0;
        unsigned total_illegal = 0;

        for (unsigned mask = 0; mask < FRAG_TOTAL; ++mask) {
            // --- Illegality detection ---
            // Rule 1: Mixing single-column and multi-column CK restrictions
            //         is always illegal.
            // Rule 2: At most one multi-column CK restriction type allowed.
            bool has_single_ck = (mask & SINGLE_CK_MASK) != 0;
            bool has_multi_ck  = (mask & MULTI_CK_MASK) != 0;
            unsigned multi_ck_count = std::popcount(mask & MULTI_CK_MASK);
            bool is_illegal = (has_single_ck && has_multi_ck)
                           || (multi_ck_count > 1);

            // Build WHERE clause from all set bits.
            std::string where_clause;
            for (auto& f : fragments) {
                if (mask & f.bit) {
                    if (!where_clause.empty()) {
                        where_clause += " AND ";
                    }
                    where_clause += f.clause;
                }
            }

            auto ctx_msg = [&](std::string_view api) {
                return fmt::format("mask=0x{:04x} WHERE [{}]: {}",
                                   mask, where_clause, api);
            };

            prepare_context ctx;
            auto where_expr = where_clause.empty()
                ? expr::expression(expr::conjunction{})
                : cql3::util::where_clause_to_relations(where_clause, cql3::dialect{});

            shared_ptr<const restrictions::statement_restrictions> sr;
            try {
                sr = restrictions::analyze_statement_restrictions(
                        e.data_dictionary(),
                        schema,
                        statements::statement_type::SELECT,
                        where_expr,
                        ctx,
                        /*contains_only_static_columns=*/false,
                        /*for_view=*/false,
                        /*allow_filtering=*/true,
                        restrictions::check_indexes::yes);
            } catch (const exceptions::invalid_request_exception&) {
            }

            if (is_illegal) {
                BOOST_CHECK_MESSAGE(!sr,
                    ctx_msg("expected exception for illegal CK combination"));
                ++total_illegal;
                ++total_tested;
                continue;
            }
            BOOST_REQUIRE_MESSAGE(sr,
                ctx_msg("unexpected exception for legal CK combination"));

            // --- Derived CK properties ---
            bool has_multi_column = has_multi_ck;

            // Which CK columns are restricted?
            bool has_ck1 = (mask & (CK1_EQ | CK1_IN | MULTI_EQ | MULTI_SLICE | MULTI_IN)) != 0;
            bool has_ck2 = (mask & (CK2_SLICE | MULTI_EQ | MULTI_SLICE | MULTI_IN)) != 0;
            bool has_any_ck = has_ck1 || has_ck2;
            unsigned ck_count = (has_ck1 ? 1u : 0u) + (has_ck2 ? 1u : 0u);

            // clustering_key_restrictions_has_IN: any IN binop present
            bool has_ck_in = (mask & (CK1_IN | MULTI_IN)) != 0;

            // clustering_key_restrictions_has_only_eq: no non-EQ binop in CK
            // restrictions.  Vacuously true when no CK restrictions.
            bool has_only_eq = !(mask & (CK2_SLICE | CK1_IN | MULTI_SLICE | MULTI_IN));

            // clustering_key_restrictions_need_filtering (internal predicate,
            // before ORing with has_partition_key_unrestricted_components):
            // For multi-column restrictions, always false.
            // For single-column: true when there's a CK gap (ck2 restricted
            // without ck1 being restricted by EQ or IN).
            bool ck_need_filtering_internal = !has_multi_column
                && (mask & CK2_SLICE)
                && !(mask & (CK1_EQ | CK1_IN));

            // has_eq_restriction_on_column: recognizes column_value and
            // tuple_constructor LHS with oper_t::EQ.
            // CK1_EQ → true for ck1.  MULTI_EQ → true for both ck1 and ck2.
            // IN, slice → false.
            bool ck1_has_eq = (mask & CK1_EQ) || (mask & MULTI_EQ);
            bool ck2_has_eq = (mask & MULTI_EQ) != 0;

            // comb_pk1 index selection:
            // Requires: pk1 restricted with EQ and PK incomplete
            // (_is_key_range).  PK restrictions are iterated first in
            // _index_restrictions, so comb_pk1 (global, score 1) beats
            // all same-score indexes that come later.
            bool selects_comb_pk1 = (mask & PK1) && !(mask & PK2);

            // comb_ck1 index selection:
            // Requires: !full_pk, single-column EQ on ck1 (not IN — index
            // only supports EQ), no CK1_IN (makes conjunction unsupported),
            // no multi-column.
            // In legal combos, CK1_EQ set → no MULTI_* possible.
            // When pk1 is also restricted, comb_pk1 takes priority (PK
            // group comes before CK group in _index_restrictions).
            bool selects_comb_ck1 = (mask & CK1_EQ) && !(mask & CK1_IN);

            // Index clustering range multiplier:
            // CK1_IN alone produces 2 IN values → 2 ranges.
            // CK1_EQ + CK1_IN: intersection narrows to 1.
            // Multi-column: not added to prefix, multiplier 1.
            unsigned idx_range_multiplier = ((mask & CK1_IN) && !(mask & CK1_EQ)) ? 2 : 1;

            // --- Partition key APIs ---
            bool has_pk1 = (mask & PK1) != 0;
            bool has_pk2 = (mask & PK2) != 0;
            bool full_pk = has_pk1 && has_pk2;

            BOOST_CHECK_MESSAGE(
                sr->partition_key_restrictions_is_empty() == (!has_pk1 && !has_pk2),
                ctx_msg("partition_key_restrictions_is_empty"));

            BOOST_CHECK_MESSAGE(
                sr->partition_key_restrictions_is_all_eq() == true,
                ctx_msg("partition_key_restrictions_is_all_eq"));

            BOOST_CHECK_MESSAGE(
                sr->has_partition_key_unrestricted_components() == (!has_pk1 || !has_pk2),
                ctx_msg("has_partition_key_unrestricted_components"));

            unsigned pk_restricted = (has_pk1 ? 1u : 0u) + (has_pk2 ? 1u : 0u);
            BOOST_CHECK_MESSAGE(
                sr->partition_key_restrictions_size() == pk_restricted,
                ctx_msg(fmt::format("partition_key_restrictions_size: got {} want {}",
                                    sr->partition_key_restrictions_size(), pk_restricted)));

            BOOST_CHECK_MESSAGE(
                sr->has_token_restrictions() == false,
                ctx_msg("has_token_restrictions"));

            BOOST_CHECK_MESSAGE(
                sr->key_is_in_relation() == false,
                ctx_msg("key_is_in_relation"));

            // is_key_range: true unless full PK is specified with EQ
            BOOST_CHECK_MESSAGE(
                sr->is_key_range() == !full_pk,
                ctx_msg("is_key_range"));

            // --- Clustering key APIs ---
            BOOST_CHECK_MESSAGE(
                sr->has_clustering_columns_restriction() == has_any_ck,
                ctx_msg("has_clustering_columns_restriction"));

            BOOST_CHECK_MESSAGE(
                sr->clustering_columns_restrictions_size() == ck_count,
                ctx_msg(fmt::format("clustering_columns_restrictions_size: got {} want {}",
                                    sr->clustering_columns_restrictions_size(), ck_count)));

            BOOST_CHECK_MESSAGE(
                sr->has_unrestricted_clustering_columns() == (ck_count < 2),
                ctx_msg("has_unrestricted_clustering_columns"));

            BOOST_CHECK_MESSAGE(
                sr->clustering_key_restrictions_has_IN() == has_ck_in,
                ctx_msg("clustering_key_restrictions_has_IN"));

            BOOST_CHECK_MESSAGE(
                sr->clustering_key_restrictions_has_only_eq() == has_only_eq,
                ctx_msg("clustering_key_restrictions_has_only_eq"));

            // ck_restrictions_need_filtering:
            //   = has_any_ck && (!full_pk || ck_need_filtering_internal)
            // The internal predicate captures column-gap / non-prefix issues;
            // has_partition_key_unrestricted_components() is ORed in by the
            // outer ck_restrictions_need_filtering().
            bool ck_needs_filter = has_any_ck && (!full_pk || ck_need_filtering_internal);
            BOOST_CHECK_MESSAGE(
                sr->ck_restrictions_need_filtering() == ck_needs_filter,
                ctx_msg("ck_restrictions_need_filtering"));

            // --- Non-primary-key APIs ---
            bool has_v1    = (mask & V1) != 0;
            bool has_v3    = (mask & V3) != 0;
            bool has_s1    = (mask & S1) != 0;
            bool has_m_val = (mask & M_VAL) != 0;
            bool has_m_key = (mask & M_KEY) != 0;
            bool has_m_ent = (mask & M_ENT) != 0;
            bool has_fs    = (mask & FS) != 0;
            bool has_nonpk = has_v1 || has_v3 || has_s1
                          || has_m_val || has_m_key || has_m_ent || has_fs;

            BOOST_CHECK_MESSAGE(
                sr->has_non_primary_key_restriction() == has_nonpk,
                ctx_msg("has_non_primary_key_restriction"));

            // --- Per-column restriction checks ---
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&pk1_def) == has_pk1,
                ctx_msg("is_restricted(pk1)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&pk2_def) == has_pk2,
                ctx_msg("is_restricted(pk2)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&ck1_def) == has_ck1,
                ctx_msg("is_restricted(ck1)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&ck2_def) == has_ck2,
                ctx_msg("is_restricted(ck2)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&v1_def) == has_v1,
                ctx_msg("is_restricted(v1)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&v3_def) == has_v3,
                ctx_msg("is_restricted(v3)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&s1_def) == has_s1,
                ctx_msg("is_restricted(s1)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&m1_def) == has_m_val,
                ctx_msg("is_restricted(m1)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&m2_def) == has_m_key,
                ctx_msg("is_restricted(m2)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&m3_def) == has_m_ent,
                ctx_msg("is_restricted(m3)"));
            BOOST_CHECK_MESSAGE(
                sr->is_restricted(&fs_def) == has_fs,
                ctx_msg("is_restricted(fs)"));

            // has_eq_restriction_on_column:
            //   pk1/pk2 always EQ when present.
            //   ck1/ck2 depend on the CK restriction type.
            //   v1/v3/fs are EQ, s1 is CONTAINS, m1 CONTAINS, m2 CONTAINS KEY,
            //   m3[1]='a' is subscript EQ (not recognized), fs is regular EQ.
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(pk1_def) == has_pk1,
                ctx_msg("has_eq_restriction_on_column(pk1)"));
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(pk2_def) == has_pk2,
                ctx_msg("has_eq_restriction_on_column(pk2)"));
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(ck1_def) == ck1_has_eq,
                ctx_msg("has_eq_restriction_on_column(ck1)"));
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(ck2_def) == ck2_has_eq,
                ctx_msg("has_eq_restriction_on_column(ck2)"));
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(v1_def) == has_v1,
                ctx_msg("has_eq_restriction_on_column(v1)"));
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(v3_def) == has_v3,
                ctx_msg("has_eq_restriction_on_column(v3)"));
            // s1 CONTAINS is not EQ:
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(s1_def) == false,
                ctx_msg("has_eq_restriction_on_column(s1)"));
            // m1 CONTAINS is not EQ:
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(m1_def) == false,
                ctx_msg("has_eq_restriction_on_column(m1)"));
            // m2 CONTAINS KEY is not EQ:
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(m2_def) == false,
                ctx_msg("has_eq_restriction_on_column(m2)"));
            // m3[1] = 'a' is a subscript EQ — not recognized by
            // has_eq_restriction_on_column (needs column_value/tuple_constructor LHS).
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(m3_def) == false,
                ctx_msg("has_eq_restriction_on_column(m3)"));
            // fs = {1,2} is a regular EQ on frozen collection:
            BOOST_CHECK_MESSAGE(
                sr->has_eq_restriction_on_column(fs_def) == has_fs,
                ctx_msg("has_eq_restriction_on_column(fs)"));

            // --- Index selection ---
            auto [idx_opt, idx_expr] = sr->find_idx(sim);

            // Determine expected index.  The scoring algorithm:
            //   - do_find_idx iterates _index_restrictions (PK group, then
            //     CK group, then non-PK group in WHERE-clause order).
            //   - Multi-column restrictions are skipped (line 1358).
            //   - Score: local index with full PK = 2, global = 1, local
            //     without full PK = 0.
            //   - Strict > for tiebreaking → first with highest score wins.
            //
            // The PK index (comb_pk1) can be selected when:
            //   (a) pk1 is restricted with EQ, and
            //   (b) PK is incomplete (_is_key_range), which triggers
            //       _uses_secondary_indexing via _has_queriable_pk_index.
            // PK restrictions are iterated first in _index_restrictions, so
            // comb_pk1 (score 1) beats all later global indexes (tie → first
            // wins).
            //
            // The CK index (comb_ck1) can only be selected when:
            //   (a) ck1 is restricted with single-column EQ (not IN — the
            //       index only supports EQ, not IN),
            //   (b) PK is incomplete (_is_key_range), and
            //   (c) no multi-column CK restriction (_has_multi_column blocks
            //       the CK-index path at line 1151).
            //   (d) CK1_EQ + CK1_IN conjunction makes the index unsupported
            //       (IN child fails is_supported_by).
            //   (e) pk1 is NOT restricted (otherwise comb_pk1 wins first).
            //
            // When comb_ck1 qualifies it is iterated after PK but before
            // non-PK in _index_restrictions, and its score 1 ties with any
            // non-PK global, so it wins.

            std::optional<sstring> expected_idx;

            if (selects_comb_pk1) {
                expected_idx = "comb_pk1";
            } else if (selects_comb_ck1 && !full_pk) {
                expected_idx = "comb_ck1";
            } else if (full_pk && has_v3) {
                // Local index scores 2, beats any global (score 1).
                expected_idx = "comb_v3_local";
            } else if (has_v1) {
                expected_idx = "comb_v1";
            } else if (has_s1) {
                // v3 without full_pk scores 0 and is skipped.
                expected_idx = "comb_s1";
            } else if (has_m_val) {
                expected_idx = "comb_m1_values";
            } else if (has_m_key) {
                expected_idx = "comb_m2_keys";
            } else if (has_m_ent) {
                expected_idx = "comb_m3_entries";
            } else if (has_fs) {
                expected_idx = "comb_fs";
            }
            // else: no indexable column (v3 alone without full_pk scores 0)

            bool uses_idx = expected_idx.has_value();

            BOOST_CHECK_MESSAGE(
                (idx_opt ? std::optional(idx_opt->metadata().name()) : std::nullopt) == expected_idx,
                ctx_msg(fmt::format("find_idx: got {} want {}",
                                    idx_opt ? idx_opt->metadata().name() : "(none)",
                                    expected_idx.value_or("(none)"))));
            BOOST_CHECK_MESSAGE(
                sr->uses_secondary_indexing() == uses_idx,
                ctx_msg(fmt::format("uses_secondary_indexing: got {} want {}",
                                    sr->uses_secondary_indexing(), uses_idx)));

            // --- need_filtering ---
            // Filtering is needed when:
            //   - PK is not fully specified (partial PK needs filtering unless
            //     using index)
            //   - CK has a gap (ck2 without ck1) needs filtering
            //   - Non-PK restrictions that aren't consumed by the index need
            //     filtering
            //   - When using an index, remaining restrictions beyond the
            //     indexed one need filtering
            //   - Multi-column CK restrictions that can't be converted to
            //     bounds need filtering
            // The exact logic is complex; we check invariants.
            bool need_filt = sr->need_filtering();

            // 1. If no restrictions at all, no filtering needed.
            if (mask == 0) {
                BOOST_CHECK_MESSAGE(!need_filt, ctx_msg("need_filtering: empty should be false"));
            }
            // 2. If only the full PK is specified (no CK, no non-PK), no filtering.
            if (mask == (PK1 | PK2)) {
                BOOST_CHECK_MESSAGE(!need_filt, ctx_msg("need_filtering: full PK only should be false"));
            }
            // 3. Single indexed column (no CK): no filtering needed.
            if (mask == PK1 || mask == V1 || mask == S1) {
                BOOST_CHECK_MESSAGE(!need_filt, ctx_msg("need_filtering: single indexed column should be false"));
            }
            // 4. If using index + has extra non-PK restrictions, filtering is needed.
            if (uses_idx) {
                int non_pk_indexed_count = (has_v1 ? 1 : 0) + (has_v3 ? 1 : 0) + (has_s1 ? 1 : 0)
                                         + (has_m_val ? 1 : 0) + (has_m_key ? 1 : 0) + (has_m_ent ? 1 : 0)
                                         + (has_fs ? 1 : 0);
                if (non_pk_indexed_count > 1) {
                    BOOST_CHECK_MESSAGE(need_filt,
                        ctx_msg("need_filtering: multiple non-PK restrictions with index should need filtering"));
                }
            }
            // 5. Partial PK with no index needs filtering.
            if ((has_pk1 != has_pk2) && !uses_idx) {
                BOOST_CHECK_MESSAGE(need_filt,
                    ctx_msg("need_filtering: partial PK without index should need filtering"));
            }
            // 6. CK gap (ck2 without ck1, single-column only) needs filtering.
            if (ck_need_filtering_internal) {
                BOOST_CHECK_MESSAGE(need_filt,
                    ctx_msg("need_filtering: CK gap should need filtering"));
            }
            // 7. Non-PK restriction without index needs filtering.
            if (has_nonpk && !uses_idx) {
                BOOST_CHECK_MESSAGE(need_filt,
                    ctx_msg("need_filtering: non-PK restriction without index should need filtering"));
            }

            // --- pk_restrictions_need_filtering ---
            bool pk_needs_filter = sr->pk_restrictions_need_filtering();
            if (!uses_idx && has_pk1 != has_pk2) {
                BOOST_CHECK_MESSAGE(pk_needs_filter,
                    ctx_msg("pk_restrictions_need_filtering: partial PK should need filtering"));
            }
            if (!has_pk1 && !has_pk2) {
                BOOST_CHECK_MESSAGE(!pk_needs_filter,
                    ctx_msg("pk_restrictions_need_filtering: no PK should be false"));
            }
            if (full_pk) {
                BOOST_CHECK_MESSAGE(!pk_needs_filter,
                    ctx_msg("pk_restrictions_need_filtering: full PK should be false"));
            }

            // --- is_empty ---
            BOOST_CHECK_MESSAGE(
                sr->is_empty() == (mask == 0),
                ctx_msg("is_empty"));

            // --- get_not_null_columns: none of our fragments use IS NOT NULL ---
            BOOST_CHECK_MESSAGE(
                sr->get_not_null_columns().empty(),
                ctx_msg("get_not_null_columns should be empty"));

            // --- get_partition_key_ranges ---
            // Always returns exactly 1 range.  Full PK → singular range
            // (specific partition).  Otherwise → open-ended range.
            {
                auto pk_ranges = sr->get_partition_key_ranges(query_options({}));
                BOOST_CHECK_MESSAGE(pk_ranges.size() == 1,
                    ctx_msg(fmt::format("get_partition_key_ranges: {} ranges, want 1",
                                        pk_ranges.size())));
                if (full_pk) {
                    BOOST_CHECK_MESSAGE(pk_ranges.size() == 1 && pk_ranges[0].is_singular(),
                        ctx_msg("get_partition_key_ranges: full PK should yield singular range"));
                } else {
                    BOOST_CHECK_MESSAGE(pk_ranges.size() == 1 && !pk_ranges[0].is_singular(),
                        ctx_msg("get_partition_key_ranges: incomplete PK should yield non-singular range"));
                }
            }

            // --- get_clustering_bounds ---
            // Expected range count:
            //   Empty CK restrictions → 1 open-ended range.
            //   Multi-column: MULTI_IN → 2 singular, else → 1.
            //   Single-column: CK1_IN without CK1_EQ → 2 (IN values expand),
            //     else → 1 (CK1_EQ narrows intersection to 1 value).
            {
                auto ck_bounds = sr->get_clustering_bounds(query_options({}));
                unsigned expected_ck_bounds;
                if (!(mask & (SINGLE_CK_MASK | MULTI_CK_MASK))) {
                    // No CK restrictions → 1 open-ended range.
                    expected_ck_bounds = 1;
                } else if (mask & MULTI_CK_MASK) {
                    expected_ck_bounds = (mask & MULTI_IN) ? 2 : 1;
                } else {
                    expected_ck_bounds = ((mask & CK1_IN) && !(mask & CK1_EQ)) ? 2 : 1;
                }
                BOOST_CHECK_MESSAGE(ck_bounds.size() == expected_ck_bounds,
                    ctx_msg(fmt::format("get_clustering_bounds: {} ranges, want {}",
                                        ck_bounds.size(), expected_ck_bounds)));
                // With no CK restrictions the range should be open-ended.
                if (!(mask & (SINGLE_CK_MASK | MULTI_CK_MASK))) {
                    BOOST_CHECK_MESSAGE(
                        ck_bounds.size() == 1
                            && !ck_bounds[0].start()
                            && !ck_bounds[0].end(),
                        ctx_msg("get_clustering_bounds: no CK should be open-ended"));
                }
            }

            // --- Index table range APIs ---
            if (uses_idx) {
                bool is_local_idx = (expected_idx == "comb_v3_local");

                if (is_local_idx) {
                    // --- get_local_index_clustering_ranges ---
                    // Local index CK prefix = (indexed_col, base_ck1, ...).
                    // The indexed column is always EQ (1 value); CK IN values
                    // multiply via the base CK appended to the prefix.
                    auto local_ranges = sr->get_local_index_clustering_ranges(query_options({}));
                    unsigned expected_local = 1 * idx_range_multiplier;
                    BOOST_CHECK_MESSAGE(!local_ranges.empty(),
                        ctx_msg("get_local_index_clustering_ranges should not be empty"));
                    BOOST_CHECK_MESSAGE(local_ranges.size() == expected_local,
                        ctx_msg(fmt::format("get_local_index_clustering_ranges: {} ranges, want {}",
                                            local_ranges.size(), expected_local)));
                } else {
                    // --- get_global_index_clustering_ranges ---
                    // Global index CK prefix = (token, pk1, pk2, ...base CK...).
                    // With full PK: CK IN values expand the prefix.
                    // Without full PK: prefix is empty → 1 open-ended range.
                    auto global_ranges = sr->get_global_index_clustering_ranges(query_options({}));
                    BOOST_CHECK_MESSAGE(!global_ranges.empty(),
                        ctx_msg("get_global_index_clustering_ranges should not be empty"));
                    if (full_pk) {
                        unsigned expected_global = 1 * idx_range_multiplier;
                        BOOST_CHECK_MESSAGE(global_ranges.size() == expected_global,
                            ctx_msg(fmt::format(
                                "get_global_index_clustering_ranges (full PK): {} ranges, want {}",
                                global_ranges.size(), expected_global)));
                    } else {
                        // Without full PK the prefix has no token/PK entries,
                        // so we get 1 open-ended range.
                        BOOST_CHECK_MESSAGE(global_ranges.size() == 1,
                            ctx_msg(fmt::format(
                                "get_global_index_clustering_ranges (!full PK): {} ranges, want 1",
                                global_ranges.size())));
                    }

                    // --- get_global_index_token_clustering_ranges ---
                    // For modern (non-v1) indexes the token column is
                    // long_type, so this dispatches to the same
                    // get_single_column_clustering_bounds as
                    // get_global_index_clustering_ranges.
                    auto token_ranges = sr->get_global_index_token_clustering_ranges(query_options({}));
                    BOOST_CHECK_MESSAGE(token_ranges.size() == global_ranges.size(),
                        ctx_msg(fmt::format(
                            "get_global_index_token_clustering_ranges: {} ranges, want {} (same as global)",
                            token_ranges.size(), global_ranges.size())));
                }
            }

            ++total_tested;
        }

        BOOST_TEST_MESSAGE(fmt::format("Tested {} restriction combinations ({} legal, {} illegal, 2^{} = {} total)",
                                       total_tested, total_tested - total_illegal, total_illegal, N_FRAG, FRAG_TOTAL));
    }, {}, tattr);
}

/// Helper to get statement_restrictions from a parsed WHERE clause string.
static shared_ptr<const restrictions::statement_restrictions> make_restrictions(
        std::string_view where_clause, cql_test_env& env,
        const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    prepare_context ctx;
    auto factors = where_clause.empty()
            ? expr::expression_list{}
            : boolean_factors(cql3::util::where_clause_to_relations(where_clause, cql3::dialect{}));
    return restrictions::analyze_statement_restrictions(
            env.data_dictionary(),
            env.local_db().find_schema(keyspace_name, table_name),
            statements::statement_type::SELECT,
            expr::conjunction{std::move(factors)},
            ctx,
            /*contains_only_static_columns=*/false,
            /*for_view=*/false,
            /*allow_filtering=*/true,
            restrictions::check_indexes::yes);
}

/// Extract (column_name, operator) pairs from each boolean factor of a conjunction expression.
/// Each factor must be a binary_operator whose LHS is a column_value or subscript.
static std::vector<std::pair<sstring, expr::oper_t>> factor_ops(const expr::expression& e) {
    std::vector<std::pair<sstring, expr::oper_t>> result;
    for (auto& factor : expr::boolean_factors(e)) {
        BOOST_REQUIRE_MESSAGE(expr::is<expr::binary_operator>(factor),
            fmt::format("expected binary_operator, got: {}", factor));
        auto& binop = expr::as<expr::binary_operator>(factor);
        const auto& cv = expr::get_subscripted_column(binop.lhs);
        result.emplace_back(cv.col->name_as_text(), binop.op);
    }
    return result;
}

// Test that restrictions are correctly routed to per-column maps and that each
// per-column entry contains exactly the right boolean factors (verified by column
// name and operator, not just count).  This is the higher-level replacement for
// the old extract_single_column_restrictions_for_column test.
SEASTAR_TEST_CASE(per_column_restriction_routing) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.trc(pk1 int, pk2 int, ck1 int, ck2 int, v1 int, v2 int, v3 int, "
                         "primary key((pk1, pk2), ck1, ck2))");

        auto schema = e.local_db().find_schema("ks", "trc");

        using op = expr::oper_t;
        using col_op = std::pair<sstring, expr::oper_t>;

        // Multiple single-column restrictions on regular columns are correctly
        // accumulated per-column, while unrestricted columns don't appear.
        {
            auto sr = make_restrictions(
                "pk1=1 AND pk2=2 AND ck1=3 AND ck2=4 AND v1=5 AND v1<10 AND v1>0 AND v2=6",
                e, "trc");

            // --- Non-PK per-column map ---
            auto& npk = sr->get_non_pk_restriction();
            BOOST_CHECK_EQUAL(npk.size(), 2u);

            auto* v1_def = schema->get_column_definition("v1");
            auto* v2_def = schema->get_column_definition("v2");
            auto* v3_def = schema->get_column_definition("v3");

            BOOST_REQUIRE(npk.contains(v1_def));
            BOOST_REQUIRE(npk.contains(v2_def));
            BOOST_CHECK(!npk.contains(v3_def));

            // v1 should have EQ, LT, GT (in WHERE-clause order).
            BOOST_CHECK_EQUAL(factor_ops(npk.at(v1_def)),
                (std::vector<col_op>{{"v1", op::EQ}, {"v1", op::LT}, {"v1", op::GT}}));
            // v2 should have a single EQ.
            BOOST_CHECK_EQUAL(factor_ops(npk.at(v2_def)),
                (std::vector<col_op>{{"v2", op::EQ}}));

            // --- PK expression: pk1=1 AND pk2=2 ---
            BOOST_CHECK_EQUAL(factor_ops(sr->get_partition_key_restrictions()),
                (std::vector<col_op>{{"pk1", op::EQ}, {"pk2", op::EQ}}));

            // --- CK expression: ck1=3 AND ck2=4 ---
            BOOST_CHECK_EQUAL(factor_ops(sr->get_clustering_columns_restrictions()),
                (std::vector<col_op>{{"ck1", op::EQ}, {"ck2", op::EQ}}));
        }

        // Multi-column CK restriction doesn't appear in single-column non-PK map.
        {
            auto sr = make_restrictions(
                "pk1=1 AND pk2=2 AND (ck1, ck2) > (0, 0) AND v1=5",
                e, "trc");

            auto& npk = sr->get_non_pk_restriction();
            BOOST_CHECK_EQUAL(npk.size(), 1u);

            auto* v1_def = schema->get_column_definition("v1");
            BOOST_REQUIRE(npk.contains(v1_def));
            BOOST_CHECK_EQUAL(factor_ops(npk.at(v1_def)),
                (std::vector<col_op>{{"v1", op::EQ}}));

            // CK expression should have 1 factor: the multi-column (ck1, ck2) > (0, 0).
            // The multi-column restriction's LHS is a tuple_constructor, not a single
            // column_value, so we verify only the count and operator here.
            auto ck_factors = expr::boolean_factors(sr->get_clustering_columns_restrictions());
            BOOST_CHECK_EQUAL(ck_factors.size(), 1u);
            BOOST_REQUIRE(expr::is<expr::binary_operator>(ck_factors[0]));
            BOOST_CHECK(expr::as<expr::binary_operator>(ck_factors[0]).op == op::GT);
        }

        // Unrestricted table: all maps are empty.
        {
            auto sr = make_restrictions("", e, "trc");
            BOOST_CHECK(sr->get_non_pk_restriction().empty());
            BOOST_CHECK(restrictions::is_empty_restriction(sr->get_clustering_columns_restrictions()));
        }
    });
}

BOOST_AUTO_TEST_SUITE_END()
