/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>

#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/util.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "utils/rjson.hh"
#include "vector_search/filter.hh"

BOOST_AUTO_TEST_SUITE(filter_test)

using namespace cql3;

namespace {

/// Helper to create statement_restrictions from a WHERE clause string
restrictions::statement_restrictions make_restrictions(
        std::string_view where_clause, cql_test_env& env, const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    prepare_context ctx;
    return restrictions::analyze_statement_restrictions(env.data_dictionary(), env.local_db().find_schema(keyspace_name, table_name),
            statements::statement_type::SELECT, expr::conjunction{expr::boolean_factors(cql3::util::where_clause_to_relations(where_clause, cql3::dialect{}))},
            ctx,
            /*selects_only_static_columns=*/false,
            /*for_view=*/false,
            /*allow_filtering=*/true, restrictions::check_indexes::yes);
}

/// Helper to get JSON string from restrictions
sstring get_restrictions_json(const restrictions::statement_restrictions& restr, bool allow_filtering = false) {
    return rjson::print(vector_search::to_json(restr, query_options({}), allow_filtering));
}

} // anonymous namespace

SEASTAR_TEST_CASE(to_json_empty_restrictions) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto schema = e.local_db().find_schema("ks", "t");
        restrictions::statement_restrictions restr(schema, false);
        auto json = rjson::print(vector_search::to_json(restr, query_options({}), false));

        BOOST_CHECK_EQUAL(json, "{}");
    });
}

SEASTAR_TEST_CASE(to_json_with_allow_filtering) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_single_column_eq) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=42", e);
        auto json = get_restrictions_json(restr, false);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":42}],"allow_filtering":false})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_single_column_lt) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck<100", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"<","lhs":"ck","rhs":100}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_single_column_gt) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck>50", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":">","lhs":"ck","rhs":50}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_single_column_lte) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck<=75", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"<=","lhs":"ck","rhs":75}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_single_column_gte) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck>=25", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":">=","lhs":"ck","rhs":25}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_single_column_in) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck in (1, 2, 3)", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"IN","lhs":"ck","rhs":[1,2,3]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_string_value) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk text, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk='hello'", e);
        auto json = get_restrictions_json(restr, false);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":"hello"}],"allow_filtering":false})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_multi_column_eq) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck1 int, ck2 int, v vector<float, 3>, primary key(pk, ck1, ck2))");

        auto restr = make_restrictions("pk=1 and (ck1, ck2)=(10, 20)", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"()==()","lhs":["ck1","ck2"],"rhs":[10,20]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_multi_column_lt) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck1 int, ck2 int, v vector<float, 3>, primary key(pk, ck1, ck2))");

        auto restr = make_restrictions("pk=1 and (ck1, ck2)<(10, 20)", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"()<()","lhs":["ck1","ck2"],"rhs":[10,20]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_multi_column_gt) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck1 int, ck2 int, v vector<float, 3>, primary key(pk, ck1, ck2))");

        auto restr = make_restrictions("pk=1 and (ck1, ck2)>(10, 20)", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"()>()","lhs":["ck1","ck2"],"rhs":[10,20]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_multi_column_lte) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck1 int, ck2 int, v vector<float, 3>, primary key(pk, ck1, ck2))");

        auto restr = make_restrictions("pk=1 and (ck1, ck2)<=(10, 20)", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"()<=()","lhs":["ck1","ck2"],"rhs":[10,20]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_multi_column_gte) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck1 int, ck2 int, v vector<float, 3>, primary key(pk, ck1, ck2))");

        auto restr = make_restrictions("pk=1 and (ck1, ck2)>=(10, 20)", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"()>=()","lhs":["ck1","ck2"],"rhs":[10,20]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_multi_column_in) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck1 int, ck2 int, v vector<float, 3>, primary key(pk, ck1, ck2))");

        auto restr = make_restrictions("pk=1 and (ck1, ck2) in ((1, 2), (3, 4))", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"()IN()","lhs":["ck1","ck2"],"rhs":[[1,2],[3,4]]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_multiple_restrictions) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck>=10 and ck<100", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":">=","lhs":"ck","rhs":10},{"type":"<","lhs":"ck","rhs":100}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_with_boolean_value) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck boolean, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("ck=true", e);
        auto json = get_restrictions_json(restr, true);

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"ck","rhs":true}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

BOOST_AUTO_TEST_SUITE_END()
