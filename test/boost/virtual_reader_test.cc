/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <stdint.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include "index/secondary_index_manager.hh"
#include "db/size_estimates_virtual_reader.hh"
#include "db/system_keyspace.hh"
#include "db/view/view_builder.hh"
#include <seastar/core/future-util.hh>

BOOST_AUTO_TEST_SUITE(virtual_reader_test)

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_query_size_estimates_virtual_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto ranges = db::size_estimates::test_get_local_ranges(e.local_db(), e.get_system_keyspace().local()).get();
        auto start_token1 = utf8_type->to_string(ranges[3].start);
        auto start_token2 = utf8_type->to_string(ranges[5].start);
        auto end_token1 = utf8_type->to_string(ranges[3].end);
        auto end_token2 = utf8_type->to_string(ranges[55].end);

        // Should not timeout.
        e.execute_cql("select * from system.size_estimates;").discard_result().get();

        auto rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks';").get();
        assert_that(rs).is_rows().with_size(0);

        e.execute_cql("create table cf1(pk text PRIMARY KEY, v int);").discard_result().get();
        e.execute_cql("create table cf2(pk text PRIMARY KEY, v int);").discard_result().get();

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks';").get();
        assert_that(rs).is_rows().with_size(512);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' limit 100;").get();
        assert_that(rs).is_rows().with_size(100);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name = 'cf1';").get();
        assert_that(rs).is_rows().with_size(256);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name > 'cf1';").get();
        assert_that(rs).is_rows().with_size(256);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name >= 'cf1';").get();
        assert_that(rs).is_rows().with_size(512);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name < 'cf2';").get();
        assert_that(rs).is_rows().with_size(256);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name <= 'cf2';").get();
        assert_that(rs).is_rows().with_size(512);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name in ('cf1', 'cf2');").get();
        assert_that(rs).is_rows().with_size(512);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name >= 'cf1' and table_name <= 'cf1';").get();
        assert_that(rs).is_rows().with_size(256);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name >= 'cf1' and table_name <= 'cf2';").get();
        assert_that(rs).is_rows().with_size(512);

        rs = e.execute_cql("select * from system.size_estimates where keyspace_name = 'ks' and table_name > 'cf1' and table_name < 'cf2';").get();
        assert_that(rs).is_rows().with_size(0);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}';", start_token1)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start >= '{}';", start_token1)).get();
        assert_that(rs).is_rows().with_size(253);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start > '{}';", start_token1)).get();
        assert_that(rs).is_rows().with_size(252);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start <= '{}';", start_token1)).get();
        assert_that(rs).is_rows().with_size(4);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start < '{}';", start_token1)).get();
        assert_that(rs).is_rows().with_size(3);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start >= '{}' and range_start <= '{}';", start_token1, start_token1)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start >= '{}' and range_start <= '{}';", start_token1, start_token2)).get();
        assert_that(rs).is_rows().with_size(3);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start > '{}' and range_start < '{}';", start_token1, start_token2)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start in ('{}', '{}');", start_token1, start_token2)).get();
        assert_that(rs).is_rows().with_size(2);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start > '{}' and range_start <= '{}';", start_token1, start_token2)).get();
        assert_that(rs).is_rows().with_size(2);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start >= '{}' and range_start < '{}';", start_token1, start_token2)).get();
        assert_that(rs).is_rows().with_size(2);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}' and range_end = '{}';", start_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}' and range_end >= '{}';", start_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}' and range_end > '{}';", start_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(0);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}' and range_end <= '{}';", start_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}' and range_end < '{}';", start_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(0);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}' and range_end >= '{}' and range_end <= '{}';", start_token1, end_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and table_name = 'cf1' and range_start = '{}' and range_end > '{}' and range_end < '{}';", start_token1, end_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(0);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and (table_name, range_start, range_end) = ('cf1', '{}', '{}');", start_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(1);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and (table_name, range_start, range_end) >= ('cf1', '{}', '{}') and (table_name) <= ('cf2');", start_token1, end_token1)).get();
        assert_that(rs).is_rows().with_size(509);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and (table_name, range_start, range_end) >= ('cf1', '{}', '{}') "
                                      "and (table_name, range_start) <= ('cf2', '{}');", start_token1, end_token1, start_token2)).get();
        assert_that(rs).is_rows().with_size(259);

        rs = e.execute_cql(fmt::format("select * from system.size_estimates where keyspace_name = 'ks' "
                                      "and (table_name, range_start) < ('cf2', '{}');", start_token1)).get();
        assert_that(rs).is_rows().with_size(259);
    });
}

SEASTAR_TEST_CASE(test_query_view_built_progress_virtual_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto rand = [] { return dht::token::get_random_token(); };
        auto next_token = rand();
        auto next_token_str = next_token.to_sstring();
        e.get_system_keyspace().local().register_view_for_building("ks", "v1", rand()).get();
        e.get_system_keyspace().local().register_view_for_building("ks", "v2", rand()).get();
        e.get_system_keyspace().local().register_view_for_building("ks", "v3", rand()).get();
        e.get_system_keyspace().local().update_view_build_progress("ks", "v3", next_token).get();
        e.get_system_keyspace().local().register_view_for_building("ks", "v4", rand()).get();
        e.get_system_keyspace().local().update_view_build_progress("ks", "v4", next_token).get();
        e.get_system_keyspace().local().register_view_for_building("ks", "v5", rand()).get();
        e.get_system_keyspace().local().register_view_for_building("ks", "v6", rand()).get();
        e.get_system_keyspace().local().remove_view_build_progress_across_all_shards("ks", "v5").get();
        e.get_system_keyspace().local().remove_view_build_progress_across_all_shards("ks", "v6").get();
        auto rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks'").get();
        assert_that(rs).is_rows().with_rows_ignore_order({
                { {utf8_type->decompose(sstring("ks"))}, {utf8_type->decompose(sstring("v1"))}, {int32_type->decompose(0)}, { } },
                { {utf8_type->decompose(sstring("ks"))}, {utf8_type->decompose(sstring("v2"))}, {int32_type->decompose(0)}, { } },
                { {utf8_type->decompose(sstring("ks"))}, {utf8_type->decompose(sstring("v3"))}, {int32_type->decompose(0)}, {utf8_type->decompose(next_token_str)} },
                { {utf8_type->decompose(sstring("ks"))}, {utf8_type->decompose(sstring("v4"))}, {int32_type->decompose(0)}, {utf8_type->decompose(next_token_str)} }
        });
        rs = e.execute_cql("select * from system.views_builds_in_progress").get();
        assert_that(rs).is_rows().with_size(4);
        rs = e.execute_cql("select * from system.views_builds_in_progress limit 1").get();
        assert_that(rs).is_rows().with_size(1);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name = 'v2'").get();
        assert_that(rs).is_rows().with_size(1);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name > 'v2'").get();
        assert_that(rs).is_rows().with_size(2);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name >= 'v2'").get();
        assert_that(rs).is_rows().with_size(3);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name  < 'v2'").get();
        assert_that(rs).is_rows().with_size(1);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name  <= 'v2'").get();
        assert_that(rs).is_rows().with_size(2);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name in ('v1', 'v2', 'v3')").get();
        assert_that(rs).is_rows().with_size(3);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name >= 'v2' and view_name  <= 'v2'").get();
        assert_that(rs).is_rows().with_size(1);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name >= 'v2' and view_name  <= 'v3'").get();
        assert_that(rs).is_rows().with_size(2);
        rs = e.execute_cql("select * from system.views_builds_in_progress where keyspace_name = 'ks' and view_name > 'v1' and view_name  < 'v2'").get();
        assert_that(rs).is_rows().with_size(0);
    });
}

BOOST_AUTO_TEST_SUITE_END()
