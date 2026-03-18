/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include <vector>

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
        const std::vector<expr::expression>& where_clause, cql_test_env& env,
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
            .get_clustering_bounds(query_options({}));
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

expr::tuple_constructor
column_definitions_as_tuple_constructor(const std::vector<const column_definition*>& defs) {
    std::vector<expr::expression> columns;
    std::vector<data_type> column_types;
    columns.reserve(defs.size());
    for (auto& def : defs) {
        columns.push_back(expr::column_value{def});
        column_types.push_back(def->type);
    }
    data_type ttype = tuple_type_impl::get_instance(std::move(column_types));
    return expr::tuple_constructor{std::move(columns), std::move(ttype)};
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
//
// This test is placed before the predicate-based refactoring series to ensure
// it does not change observable index-selection behaviour.
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
                ? std::vector<expr::expression>{}
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
            auto [idx, restrictions_expr] = sr.find_idx(sim);
            return {where_clause,
                    idx ? std::optional(idx->metadata().name()) : std::nullopt,
                    sr.uses_secondary_indexing(),
                    sr.need_filtering()};
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
// public APIs.  This catches any refactoring that accidentally changes observable
// behaviour for *any* combination of restriction types.
//
// Restriction fragments (each on a distinct column, so all are mutually compatible):
//   bit 0:  pk1 = 1
//   bit 1:  pk2 = 1
//   bit 2:  ck1 = 1
//   bit 3:  ck2 > 0
//   bit 4:  v1 = 1              (global index comb_v1, target: regular_values)
//   bit 5:  v3 = 1              (local  index comb_v3_local, target: regular_values)
//   bit 6:  s1 CONTAINS 1       (global index comb_s1, target: keys — set)
//   bit 7:  m1 CONTAINS 'a'     (global index comb_m1_values, target: collection_values)
//   bit 8:  m2 CONTAINS KEY 1   (global index comb_m2_keys, target: keys — map)
//   bit 9:  m3[1] = 'a'         (global index comb_m3_entries, target: keys_and_values)
//   bit 10: fs = {1, 2}         (global index comb_fs, target: full — frozen collection)
SEASTAR_TEST_CASE(combinatorial_restrictions) {
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

        enum frag : unsigned {
            PK1   = 1u << 0,   // pk1 = 1
            PK2   = 1u << 1,   // pk2 = 1
            CK1   = 1u << 2,   // ck1 = 1
            CK2   = 1u << 3,   // ck2 > 0
            V1    = 1u << 4,   // v1 = 1  (global index, regular_values)
            V3    = 1u << 5,   // v3 = 1  (local index, regular_values)
            S1    = 1u << 6,   // s1 CONTAINS 1  (global index, keys — set)
            M_VAL = 1u << 7,   // m1 CONTAINS 'a' (global index, collection_values)
            M_KEY = 1u << 8,   // m2 CONTAINS KEY 1 (global index, keys — map)
            M_ENT = 1u << 9,   // m3[1] = 'a' (global index, keys_and_values)
            FS    = 1u << 10,  // fs = {1,2} (global index, full)
        };
        constexpr unsigned N = 11;
        constexpr unsigned total = 1u << N;

        struct fragment_info {
            unsigned bit;
            const char* clause;
        };
        const fragment_info fragments[] = {
            {PK1,   "pk1 = 1"},
            {PK2,   "pk2 = 1"},
            {CK1,   "ck1 = 1"},
            {CK2,   "ck2 > 0"},
            {V1,    "v1 = 1"},
            {V3,    "v3 = 1"},
            {S1,    "s1 CONTAINS 1"},
            {M_VAL, "m1 CONTAINS 'a'"},
            {M_KEY, "m2 CONTAINS KEY 1"},
            {M_ENT, "m3[1] = 'a'"},
            {FS,    "fs = {1, 2}"},
        };

        for (unsigned mask = 0; mask < total; ++mask) {
            // Build WHERE clause by joining active fragments with AND.
            std::string where_clause;
            for (auto& f : fragments) {
                if (mask & f.bit) {
                    if (!where_clause.empty()) {
                        where_clause += " AND ";
                    }
                    where_clause += f.clause;
                }
            }

            prepare_context ctx;
            auto factors = where_clause.empty()
                ? std::vector<expr::expression>{}
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

            auto ctx_msg = [&](std::string_view api) {
                return fmt::format("mask=0x{:03x} WHERE [{}]: {}", mask, where_clause, api);
            };

            // --- Partition key APIs ---
            bool has_pk1 = (mask & PK1) != 0;
            bool has_pk2 = (mask & PK2) != 0;
            bool full_pk = has_pk1 && has_pk2;

            BOOST_CHECK_MESSAGE(
                sr.partition_key_restrictions_is_empty() == (!has_pk1 && !has_pk2),
                ctx_msg("partition_key_restrictions_is_empty"));

            BOOST_CHECK_MESSAGE(
                sr.partition_key_restrictions_is_all_eq() == true,
                ctx_msg("partition_key_restrictions_is_all_eq"));

            BOOST_CHECK_MESSAGE(
                sr.has_partition_key_unrestricted_components() == (!has_pk1 || !has_pk2),
                ctx_msg("has_partition_key_unrestricted_components"));

            unsigned pk_restricted = (has_pk1 ? 1u : 0u) + (has_pk2 ? 1u : 0u);
            BOOST_CHECK_MESSAGE(
                sr.partition_key_restrictions_size() == pk_restricted,
                ctx_msg(fmt::format("partition_key_restrictions_size: got {} want {}",
                                    sr.partition_key_restrictions_size(), pk_restricted)));

            BOOST_CHECK_MESSAGE(
                sr.has_token_restrictions() == false,
                ctx_msg("has_token_restrictions"));

            BOOST_CHECK_MESSAGE(
                sr.key_is_in_relation() == false,
                ctx_msg("key_is_in_relation"));

            // is_key_range: true unless full PK is specified with EQ
            BOOST_CHECK_MESSAGE(
                sr.is_key_range() == !full_pk,
                ctx_msg("is_key_range"));

            // --- Clustering key APIs ---
            bool has_ck1 = (mask & CK1) != 0;
            bool has_ck2_slice = (mask & CK2) != 0;
            bool has_any_ck = has_ck1 || has_ck2_slice;

            BOOST_CHECK_MESSAGE(
                sr.has_clustering_columns_restriction() == has_any_ck,
                ctx_msg("has_clustering_columns_restriction"));

            unsigned ck_restricted = (has_ck1 ? 1u : 0u) + (has_ck2_slice ? 1u : 0u);
            BOOST_CHECK_MESSAGE(
                sr.clustering_columns_restrictions_size() == ck_restricted,
                ctx_msg(fmt::format("clustering_columns_restrictions_size: got {} want {}",
                                    sr.clustering_columns_restrictions_size(), ck_restricted)));

            BOOST_CHECK_MESSAGE(
                sr.has_unrestricted_clustering_columns() == (ck_restricted < 2),
                ctx_msg("has_unrestricted_clustering_columns"));

            BOOST_CHECK_MESSAGE(
                sr.clustering_key_restrictions_has_IN() == false,
                ctx_msg("clustering_key_restrictions_has_IN"));

            // clustering_key_restrictions_has_only_eq: true iff all CK restrictions are EQ.
            // CK2 is a slice (>), so if CK2 is present, it's not all-eq.
            BOOST_CHECK_MESSAGE(
                sr.clustering_key_restrictions_has_only_eq() == !has_ck2_slice,
                ctx_msg("clustering_key_restrictions_has_only_eq"));

            // ck_restrictions_need_filtering: CK slice on ck2 without ck1 prefix needs filtering.
            // Also: ck2 > 0 alone (without ck1 prefix) needs filtering.
            bool ck_needs_filter = has_any_ck && ((!has_pk1 || !has_pk2) || (has_ck2_slice && !has_ck1));
            BOOST_CHECK_MESSAGE(
                sr.ck_restrictions_need_filtering() == ck_needs_filter,
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
                sr.has_non_primary_key_restriction() == has_nonpk,
                ctx_msg("has_non_primary_key_restriction"));

            // --- Per-column restriction checks ---
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&pk1_def) == has_pk1,
                ctx_msg("is_restricted(pk1)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&pk2_def) == has_pk2,
                ctx_msg("is_restricted(pk2)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&ck1_def) == has_ck1,
                ctx_msg("is_restricted(ck1)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&ck2_def) == has_ck2_slice,
                ctx_msg("is_restricted(ck2)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&v1_def) == has_v1,
                ctx_msg("is_restricted(v1)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&v3_def) == has_v3,
                ctx_msg("is_restricted(v3)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&s1_def) == has_s1,
                ctx_msg("is_restricted(s1)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&m1_def) == has_m_val,
                ctx_msg("is_restricted(m1)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&m2_def) == has_m_key,
                ctx_msg("is_restricted(m2)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&m3_def) == has_m_ent,
                ctx_msg("is_restricted(m3)"));
            BOOST_CHECK_MESSAGE(
                sr.is_restricted(&fs_def) == has_fs,
                ctx_msg("is_restricted(fs)"));

            // has_eq_restriction_on_column: pk1/pk2/ck1/v1/v3 are EQ, ck2 is slice, s1 is CONTAINS
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(pk1_def) == has_pk1,
                ctx_msg("has_eq_restriction_on_column(pk1)"));
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(pk2_def) == has_pk2,
                ctx_msg("has_eq_restriction_on_column(pk2)"));
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(ck1_def) == has_ck1,
                ctx_msg("has_eq_restriction_on_column(ck1)"));
            // ck2 > 0 is a slice, not EQ:
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(ck2_def) == false,
                ctx_msg("has_eq_restriction_on_column(ck2)"));
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(v1_def) == has_v1,
                ctx_msg("has_eq_restriction_on_column(v1)"));
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(v3_def) == has_v3,
                ctx_msg("has_eq_restriction_on_column(v3)"));
            // s1 CONTAINS is not EQ:
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(s1_def) == false,
                ctx_msg("has_eq_restriction_on_column(s1)"));
            // m1 CONTAINS is not EQ:
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(m1_def) == false,
                ctx_msg("has_eq_restriction_on_column(m1)"));
            // m2 CONTAINS KEY is not EQ:
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(m2_def) == false,
                ctx_msg("has_eq_restriction_on_column(m2)"));
            // m3[1] = 'a' is a subscript EQ.  has_eq_restriction_on_column
            // only recognizes column_value/tuple_constructor in the LHS, not
            // subscript expressions, so it returns false for m3.
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(m3_def) == false,
                ctx_msg("has_eq_restriction_on_column(m3)"));
            // fs = {1,2} is a regular EQ on frozen collection:
            BOOST_CHECK_MESSAGE(
                sr.has_eq_restriction_on_column(fs_def) == has_fs,
                ctx_msg("has_eq_restriction_on_column(fs)"));

            // --- Index selection ---
            auto [idx_opt, idx_expr] = sr.find_idx(sim);

            // Determine expected index.  The scoring algorithm:
            //   - Search groups: PK columns → CK columns → non-PK columns
            //   - Within each group, columns are iterated in WHERE-clause order
            //   - Score: local index with full PK = 2, global = 1, local without full PK = 0
            //   - Strict > for tiebreaking (first column with highest score wins)
            //   - Each restriction fragment is on a distinct column with a single predicate,
            //     so the CollectionYes/UsualYes distinction doesn't cause cancellation.
            //
            // Fragment order (= WHERE clause order):
            //   pk1, pk2, ck1, ck2, v1, v3, s1, m1, m2, m3, fs
            //
            // PK group: pk1, pk2 — no indexes on PK columns
            // CK group: ck1 (comb_ck1, global, score 1); ck2 has no index
            // Non-PK group (in WHERE order):
            //   v1  → comb_v1         (global, score 1)
            //   v3  → comb_v3_local   (local,  score = full_pk ? 2 : 0)
            //   s1  → comb_s1         (global, score 1)
            //   m1  → comb_m1_values  (global, score 1)
            //   m2  → comb_m2_keys    (global, score 1)
            //   m3  → comb_m3_entries (global, score 1)
            //   fs  → comb_fs         (global, score 1)
            //
            // The only index that can beat score 1 is comb_v3_local (score 2)
            // when full_pk is true.  Among score-1 candidates, the first one
            // visited (CK group before non-PK, then WHERE-clause order within
            // non-PK) wins due to strict > tiebreaking.

            std::optional<sstring> expected_idx;

            if (has_ck1 && !full_pk) {
                // When PK is incomplete (_is_key_range=true), a queriable CK index
                // triggers _uses_secondary_indexing at line 1151.  comb_ck1 (global,
                // score 1) is found first in the CK search group, beating any
                // non-PK global candidates (same score, visited later).
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
                sr.uses_secondary_indexing() == uses_idx,
                ctx_msg(fmt::format("uses_secondary_indexing: got {} want {}",
                                    sr.uses_secondary_indexing(), uses_idx)));

            // --- need_filtering ---
            // Filtering is needed when:
            //   - PK is not fully specified (partial PK needs filtering unless using index)
            //     Actually: pk_restrictions_need_filtering is true when PK has restrictions
            //     but they don't fully specify the PK (and no token restriction)
            //   - CK has a gap (ck2 without ck1) needs filtering
            //   - Non-PK restrictions that aren't consumed by the index need filtering
            //   - When using an index, remaining restrictions beyond the indexed one need filtering
            // The exact logic is complex; we check it indirectly.
            bool need_filt = sr.need_filtering();

            // Some invariants we can always check:
            // 1. If no restrictions at all, no filtering needed.
            if (mask == 0) {
                BOOST_CHECK_MESSAGE(!need_filt, ctx_msg("need_filtering: empty should be false"));
            }
            // 2. If only the full PK is specified (no CK, no non-PK), no filtering.
            if (mask == (PK1 | PK2)) {
                BOOST_CHECK_MESSAGE(!need_filt, ctx_msg("need_filtering: full PK only should be false"));
            }
            // 3. Single indexed column: no filtering needed.
            if (mask == V1 || mask == S1) {
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
            // 6. CK gap (ck2 without ck1) needs filtering.
            if (has_ck2_slice && !has_ck1) {
                BOOST_CHECK_MESSAGE(need_filt,
                    ctx_msg("need_filtering: CK gap should need filtering"));
            }
            // 7. Non-PK restriction without index needs filtering.
            if (has_nonpk && !uses_idx) {
                BOOST_CHECK_MESSAGE(need_filt,
                    ctx_msg("need_filtering: non-PK restriction without index should need filtering"));
            }

            // --- pk_restrictions_need_filtering ---
            // PK filtering is needed when PK is partially specified.
            // But when using secondary indexing, the PK restrictions are not evaluated
            // directly, so pk_restrictions_need_filtering may be false.
            bool pk_needs_filter = sr.pk_restrictions_need_filtering();
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
                sr.is_empty() == (mask == 0),
                ctx_msg("is_empty"));

            // --- get_not_null_columns: none of our fragments use IS NOT NULL ---
            BOOST_CHECK_MESSAGE(
                sr.get_not_null_columns().empty(),
                ctx_msg("get_not_null_columns should be empty"));

            // --- Index table range APIs ---
            // These are only meaningful when a secondary index is selected.
            if (uses_idx) {
                bool is_local_idx = (expected_idx == "comb_v3_local");

                if (is_local_idx) {
                    // Local index: get_local_index_clustering_ranges should work.
                    // Local index CK = (indexed_column, base_ck1, base_ck2, ...).
                    auto ranges = sr.get_local_index_clustering_ranges(query_options({}), *sr.get_view_schema());
                    BOOST_CHECK_MESSAGE(!ranges.empty(),
                        ctx_msg("get_local_index_clustering_ranges should not be empty"));
                    // With a single EQ on the indexed column, expect exactly 1 range.
                    BOOST_CHECK_MESSAGE(ranges.size() == 1,
                        ctx_msg(fmt::format("get_local_index_clustering_ranges: {} ranges, want 1",
                                            ranges.size())));
                } else {
                    // Global index: get_global_index_clustering_ranges should work.
                    // Global index CK = (token, pk1, pk2, ..., ck1, ck2, ...).
                    auto ranges = sr.get_global_index_clustering_ranges(query_options({}), *sr.get_view_schema());
                    BOOST_CHECK_MESSAGE(!ranges.empty(),
                        ctx_msg("get_global_index_clustering_ranges should not be empty"));
                    if (full_pk) {
                        // Full PK: token + all PK columns in prefix → expect 1 range.
                        BOOST_CHECK_MESSAGE(ranges.size() == 1,
                            ctx_msg(fmt::format(
                                "get_global_index_clustering_ranges (full PK): {} ranges, want 1",
                                ranges.size())));
                    }
                }

            }
        }

        BOOST_TEST_MESSAGE(fmt::format("Tested all {} restriction combinations", total));
    });
}


// Currently expression doesn't have operator==().
// Implementing it is ugly, because there are shared pointers and the term base class.
// For testing purposes checking stringified expressions is enough.
static bool expression_eq(const expr::expression& e1, const expr::expression& e2) {
    return to_string(e1) == to_string(e2);
}

static void assert_expr_vec_eq(
    const std::vector<expr::expression>& v1,
    const std::vector<expr::expression>& v2,
    const std::source_location& loc = std::source_location::current()) {

    if (std::equal(v1.begin(), v1.end(), v2.begin(), v2.end(), expression_eq)) {
        return;
    }

    std::string error_msg = fmt::format("Location: {}:{}, Expression vectors not equal! [{}] != [{}]",
                                        loc.file_name(), loc.line(), fmt::join(v1, ", "), fmt::join(v2, ", "));

    BOOST_FAIL(error_msg);
}

// Unit tests for extract_column_restrictions function
BOOST_AUTO_TEST_CASE(expression_extract_column_restrictions) {
    using namespace expr;

    auto make_column = [](const char* name, column_kind kind, int id) -> column_definition {
        column_definition definition(name, int32_type, kind, id);

        // column_definition has to have column_specifiction because to_string uses it for column name
        ::shared_ptr<column_identifier> identifier = ::make_shared<column_identifier>(name, true);
        column_specification specification("ks", "cf", std::move(identifier), int32_type);
        definition.column_specification = make_lw_shared<column_specification>(
            std::move(specification));

        return definition;
    };

    column_definition col_pk1 = make_column("pk1", column_kind::partition_key, 0);
    column_definition col_pk2 = make_column("pk2", column_kind::partition_key, 1);
    column_definition col_ck1 = make_column("ck1", column_kind::clustering_key, 0);
    column_definition col_ck2 = make_column("ck2", column_kind::clustering_key, 1);
    column_definition col_r1 = make_column("r2", column_kind::regular_column, 0);
    column_definition col_r2 = make_column("r2", column_kind::regular_column, 1);
    column_definition col_r3 = make_column("r3", column_kind::regular_column, 2);

    // Empty input test
    assert_expr_vec_eq(cql3::restrictions::extract_single_column_restrictions_for_column(conjunction{}, col_pk1), {});

    // BIG_WHERE test
    // big_where contains:
    // WHERE pk1 = 0 AND pk2 = 0 AND ck1 = 0 AND ck2 = 0 AND r1 = 0 AND r2 = 0
    // AND (pk1, pk2) < (0, 0) AND (pk1, ck2, r1) = (0, 0, 0) AND (r1, r2) > 0
    // AND ((c1, c2) < (0, 0) AND r1 < 0)
    // AND pk2 > 0 AND r2 > 0
    // AND token(pk1, pk2) > 0 AND token(pk1, pk2) < 0
    // AND TRUE AND FALSE
    // AND token(pk1, pk2)
    // AND pk1 AND pk2
    // AND (pk1, pk2)
    std::vector<expression> big_where;
    expr::constant zero_value = constant(raw_value::make_value(I(0)), int32_type);

    expression pk1_restriction(binary_operator(column_value(&col_pk1), oper_t::EQ, zero_value));
    expression pk2_restriction(binary_operator(column_value(&col_pk2), oper_t::EQ, zero_value));
    expression pk2_restriction2(binary_operator(column_value(&col_pk2), oper_t::GT, zero_value));
    expression ck1_restriction(binary_operator(column_value(&col_ck1), oper_t::EQ, zero_value));
    expression ck2_restriction(binary_operator(column_value(&col_ck2), oper_t::EQ, zero_value));
    expression r1_restriction(binary_operator(column_value(&col_r1), oper_t::EQ, zero_value));
    expression r1_restriction2(binary_operator(column_value(&col_r1), oper_t::LT, zero_value));
    expression r1_restriction3(binary_operator(column_value(&col_r1), oper_t::GT, zero_value));
    expression r2_restriction(binary_operator(column_value(&col_r2), oper_t::EQ, zero_value));

    auto make_multi_column_restriction = [](std::vector<const column_definition*> columns, oper_t oper) -> expression {
        tuple_constructor column_tuple(column_definitions_as_tuple_constructor(columns));

        std::vector<managed_bytes_opt> zeros_tuple_elems(columns.size(), managed_bytes_opt(I(0)));
        data_type tup_type = tuple_type_impl::get_instance(std::vector<data_type>(columns.size(), int32_type));
        managed_bytes tup_bytes = tuple_type_impl::build_value_fragmented(std::move(zeros_tuple_elems));
        constant zeros_tuple(raw_value::make_value(std::move(tup_bytes)), std::move(tup_type));

        return binary_operator(column_tuple, oper, std::move(zeros_tuple));
    };

    expression pk1_pk2_restriction = make_multi_column_restriction({&col_pk1, &col_pk2}, oper_t::LT);
    expression pk1_ck2_r1_restriction = make_multi_column_restriction({&col_pk1, &col_ck2, &col_r1}, oper_t::EQ);
    expression r1_r2_restriction = make_multi_column_restriction({&col_r1, &col_r2}, oper_t::GT);

    std::vector<expression> conjunction_elems;
    expression ck1_ck2_restriction = make_multi_column_restriction({&col_ck1, &col_ck2}, oper_t::LT);
    expression conjunction_expr = conjunction{std::vector{ck1_ck2_restriction, r1_restriction2}};

    function_call token_expr = function_call {
        .func = functions::function_name::native_function("token"),
        .args = {column_value(&col_pk1), column_value(&col_pk2)}
    };
    expression token_lt_restriction = binary_operator(token_expr, oper_t::LT, zero_value);
    expression token_gt_restriction = binary_operator(token_expr, oper_t::GT, zero_value);

    expression true_restriction = constant::make_bool(true);
    expression false_restriction = constant::make_bool(false);
    expression pk1_expr = column_value(&col_pk1);
    expression pk2_expr = column_value(&col_pk1);
    data_type ttype = tuple_type_impl::get_instance({int32_type, int32_type});
    expression pk1_pk2_expr = tuple_constructor{{expression{column_value{&col_pk1}},
                                                 expression{column_value{&col_pk2}}},
                                                std::move(ttype)};

    big_where.push_back(pk1_restriction);
    big_where.push_back(pk2_restriction);
    big_where.push_back(ck1_restriction);
    big_where.push_back(ck2_restriction);
    big_where.push_back(r1_restriction);
    big_where.push_back(r2_restriction);
    big_where.push_back(pk1_pk2_restriction);
    big_where.push_back(pk1_ck2_r1_restriction);
    big_where.push_back(r1_r2_restriction);
    big_where.push_back(conjunction_expr);
    big_where.push_back(pk2_restriction2);
    big_where.push_back(r1_restriction3);
    big_where.push_back(token_lt_restriction);
    big_where.push_back(token_gt_restriction);
    big_where.push_back(true_restriction);
    big_where.push_back(false_restriction);
    big_where.push_back(token_expr);
    big_where.push_back(pk1_expr);
    big_where.push_back(pk2_expr);
    big_where.push_back(pk1_pk2_expr);

    expression big_where_expr = conjunction{std::move(big_where)};

    assert_expr_vec_eq(restrictions::extract_single_column_restrictions_for_column(big_where_expr, col_pk1),
        {pk1_restriction});

    assert_expr_vec_eq(restrictions::extract_single_column_restrictions_for_column(big_where_expr, col_pk2),
        {pk2_restriction, pk2_restriction2});

    assert_expr_vec_eq(restrictions::extract_single_column_restrictions_for_column(big_where_expr, col_ck1),
        {ck1_restriction});

    assert_expr_vec_eq(restrictions::extract_single_column_restrictions_for_column(big_where_expr, col_ck2),
        {ck2_restriction});

    assert_expr_vec_eq(restrictions::extract_single_column_restrictions_for_column(big_where_expr, col_r1),
        {r1_restriction, r1_restriction2, r1_restriction3});

    assert_expr_vec_eq(restrictions::extract_single_column_restrictions_for_column(big_where_expr, col_r2),
        {r2_restriction});

    assert_expr_vec_eq(restrictions::extract_single_column_restrictions_for_column(big_where_expr, col_r3),
        {});
}

BOOST_AUTO_TEST_SUITE_END()
