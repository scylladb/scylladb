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

    auto where_expr = expr::conjunction{expr::boolean_factors(cql3::util::where_clause_to_relations(where_clause, cql3::dialect{}))};
    size_t max_bind_index = 0;
    bool has_bind_markers = false;
    expr::for_each_expression<expr::bind_variable>(where_expr, [&](const expr::bind_variable& bv) {
        has_bind_markers = true;
        max_bind_index = std::max(max_bind_index, static_cast<size_t>(bv.bind_index));
    });
    if (has_bind_markers) {
        std::vector<shared_ptr<cql3::column_identifier>> bind_names(max_bind_index + 1);
        ctx.set_bound_variables(bind_names);
    }

    return restrictions::analyze_statement_restrictions(env.data_dictionary(), env.local_db().find_schema(keyspace_name, table_name),
            statements::statement_type::SELECT, where_expr,
            ctx,
            /*selects_only_static_columns=*/false,
            /*for_view=*/false,
            /*allow_filtering=*/true, restrictions::check_indexes::yes);
}

query_options make_query_options(std::vector<raw_value> values) {
    return query_options(raw_value_vector_with_unset(std::move(values)));
}

/// Helper to get JSON string from restrictions
sstring get_restrictions_json(const restrictions::statement_restrictions& restr, bool allow_filtering = false) {
    return rjson::print(vector_search::prepare_filter(restr, allow_filtering).to_json(query_options({})));
}

} // anonymous namespace

SEASTAR_TEST_CASE(to_json_empty_restrictions) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto schema = e.local_db().find_schema("ks", "t");
        restrictions::statement_restrictions restr(schema, false);
        auto json = rjson::print(vector_search::prepare_filter(restr, false).to_json(query_options({})));

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

SEASTAR_TEST_CASE(to_json_bind_marker_partition_key) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=?", e);
        auto filter = vector_search::prepare_filter(restr, false);

        std::vector<raw_value> bind_values = {raw_value::make_value(int32_type->decompose(42))};
        auto options = make_query_options(std::move(bind_values));
        auto json = rjson::print(filter.to_json(options));

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":42}],"allow_filtering":false})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_bind_marker_clustering_key) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=? and ck>?", e);
        auto filter = vector_search::prepare_filter(restr, true);

        std::vector<raw_value> bind_values = {
            raw_value::make_value(int32_type->decompose(1)),
            raw_value::make_value(int32_type->decompose(50))};
        auto options = make_query_options(std::move(bind_values));
        auto json = rjson::print(filter.to_json(options));

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":">","lhs":"ck","rhs":50}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_bind_marker_different_values) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=?", e);
        auto filter = vector_search::prepare_filter(restr, false);

        std::vector<raw_value> bind_values1 = {raw_value::make_value(int32_type->decompose(100))};
        auto options1 = make_query_options(std::move(bind_values1));
        auto json1 = rjson::print(filter.to_json(options1));
        auto expected1 = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":100}],"allow_filtering":false})json";
        BOOST_CHECK_EQUAL(json1, expected1);

        std::vector<raw_value> bind_values2 = {raw_value::make_value(int32_type->decompose(200))};
        auto options2 = make_query_options(std::move(bind_values2));
        auto json2 = rjson::print(filter.to_json(options2));
        auto expected2 = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":200}],"allow_filtering":false})json";
        BOOST_CHECK_EQUAL(json2, expected2);
    });
}

SEASTAR_TEST_CASE(to_json_bind_marker_string_value) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk text, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=?", e);
        auto filter = vector_search::prepare_filter(restr, false);

        std::vector<raw_value> bind_values = {raw_value::make_value(utf8_type->decompose("hello_world"))};
        auto options = make_query_options(std::move(bind_values));
        auto json = rjson::print(filter.to_json(options));

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":"hello_world"}],"allow_filtering":false})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_mixed_literals_and_bind_markers) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck>?", e);
        auto filter = vector_search::prepare_filter(restr, true);

        std::vector<raw_value> bind_values = {raw_value::make_value(int32_type->decompose(25))};
        auto options = make_query_options(std::move(bind_values));
        auto json = rjson::print(filter.to_json(options));

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":">","lhs":"ck","rhs":25}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_bind_marker_in_list) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=1 and ck in ?", e);
        auto filter = vector_search::prepare_filter(restr, true);

        auto list_type = list_type_impl::get_instance(int32_type, true);
        auto list_val = make_list_value(list_type, {data_value(10), data_value(20), data_value(30)});

        std::vector<raw_value> bind_values = {raw_value::make_value(list_val.serialize_nonnull())};
        auto options = make_query_options(std::move(bind_values));
        auto json = rjson::print(filter.to_json(options));

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"IN","lhs":"ck","rhs":[10,20,30]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_bind_marker_multi_column) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck1 int, ck2 int, v vector<float, 3>, primary key(pk, ck1, ck2))");

        auto restr = make_restrictions("pk=1 and (ck1, ck2)>?", e);
        auto filter = vector_search::prepare_filter(restr, true);

        auto tuple_type = tuple_type_impl::get_instance({int32_type, int32_type});
        auto tuple_val = make_tuple_value(tuple_type, {data_value(10), data_value(20)});

        std::vector<raw_value> bind_values = {raw_value::make_value(tuple_val.serialize_nonnull())};
        auto options = make_query_options(std::move(bind_values));
        auto json = rjson::print(filter.to_json(options));

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":1},{"type":"()>()","lhs":["ck1","ck2"],"rhs":[10,20]}],"allow_filtering":true})json";
        BOOST_CHECK_EQUAL(json, expected);
    });
}

SEASTAR_TEST_CASE(to_json_no_bind_markers_uses_cache) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(pk int, ck int, v vector<float, 3>, primary key(pk, ck))");

        auto restr = make_restrictions("pk=42", e);
        auto filter = vector_search::prepare_filter(restr, false);

        auto options1 = query_options({});
        auto json1 = rjson::print(filter.to_json(options1));

        std::vector<raw_value> bind_values = {raw_value::make_value(int32_type->decompose(999))};
        auto options2 = query_options(db::consistency_level::ONE, raw_value_vector_with_unset(std::move(bind_values)), query_options::specific_options::DEFAULT);
        auto json2 = rjson::print(filter.to_json(options2));

        auto expected = R"json({"restrictions":[{"type":"==","lhs":"pk","rhs":42}],"allow_filtering":false})json";
        BOOST_CHECK_EQUAL(json1, expected);
        BOOST_CHECK_EQUAL(json2, expected);
    });
}

BOOST_AUTO_TEST_SUITE_END()
