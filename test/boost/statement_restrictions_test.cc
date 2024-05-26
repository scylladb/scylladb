/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "test/lib/scylla_test_case.hh"

#include <vector>

#include <fmt/ranges.h>

#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/util.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/test_utils.hh"

using namespace cql3;

namespace {

/// Returns statement_restrictions::get_clustering_bounds() of where_clause, with reasonable defaults in
/// boilerplate.
query::clustering_row_ranges slice(
        const std::vector<expr::expression>& where_clause, cql_test_env& env,
        const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    prepare_context ctx;
    return restrictions::statement_restrictions(
            env.data_dictionary(),
            env.local_db().find_schema(keyspace_name, table_name),
            statements::statement_type::SELECT,
            expr::conjunction{where_clause},
            ctx,
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
    return slice(boolean_factors(cql3::util::where_clause_to_relations(where_clause)), env, table_name, keyspace_name);
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

// Currently expression doesn't have operator==().
// Implementing it is ugly, because there are shared pointers and the term base class.
// For testing purposes checking stringified expressions is enough.
static bool expression_eq(const expr::expression& e1, const expr::expression& e2) {
    return to_string(e1) == to_string(e2);
}

static void assert_expr_vec_eq(
    const std::vector<expr::expression>& v1,
    const std::vector<expr::expression>& v2,
    const seastar::compat::source_location& loc = seastar::compat::source_location::current()) {

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
    assert_expr_vec_eq(extract_single_column_restrictions_for_column(conjunction{}, col_pk1), {});

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

    assert_expr_vec_eq(extract_single_column_restrictions_for_column(big_where_expr, col_pk1),
        {pk1_restriction});

    assert_expr_vec_eq(extract_single_column_restrictions_for_column(big_where_expr, col_pk2),
        {pk2_restriction, pk2_restriction2});

    assert_expr_vec_eq(extract_single_column_restrictions_for_column(big_where_expr, col_ck1),
        {ck1_restriction});

    assert_expr_vec_eq(extract_single_column_restrictions_for_column(big_where_expr, col_ck2),
        {ck2_restriction});

    assert_expr_vec_eq(extract_single_column_restrictions_for_column(big_where_expr, col_r1),
        {r1_restriction, r1_restriction2, r1_restriction3});

    assert_expr_vec_eq(extract_single_column_restrictions_for_column(big_where_expr, col_r2),
        {r2_restriction});

    assert_expr_vec_eq(extract_single_column_restrictions_for_column(big_where_expr, col_r3),
        {});
}
