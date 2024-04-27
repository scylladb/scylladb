/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/map.hpp>

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_delete_single_column_in_view_clustering_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b))").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where a is not null and b is not null and d is not null "
                      "primary key (a, d, b)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        });

        e.execute_cql("delete c from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, { } });
        });

        e.execute_cql("delete d from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b from mv").get();
        assert_that(msg).is_rows()
            .with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_clustering_key_eq_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(fmt::format("create materialized view vcf as select * from cf "
                                 "where a is not null and b = 1 and c is not null "
                                 "primary key {}", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1) and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_clustering_key_slice_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(fmt::format("create materialized view vcf as select * from cf "
                                 "where a is not null and b >= 1 and c is not null "
                                 "primary key {}", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 2, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1) and b >= 1 and b <= 4").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

// Another test for issue #2367.
SEASTAR_TEST_CASE(test_clustering_key_in_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(fmt::format("create materialized view vcf as select * from cf "
                                 "where a is not null and b IN (1, 2) and c is not null "
                                 "primary key {}", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 2, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            eventually([&] {
            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1) and b >= 1 and b <= 4").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_clustering_key_multi_column_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(fmt::format("create materialized view vcf as select * from cf "
                                 "where a is not null and (b, c) >= (1, 0) "
                                 "primary key {}", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, -1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1)").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_clustering_key_filtering_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(fmt::format("create materialized view vcf as select * from cf "
                                 "where a is not null and b is not null and c = 1 "
                                 "primary key {}", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, -1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1)").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

// This test reproduces issue #4340 - creating a materialized view without
// any clustering key used to fail. In this trivial example, we have a base
// table with no clustering key, and the view just keeps the same primary key.
// This should work.
SEASTAR_TEST_CASE(test_no_clustering_key_1) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, primary key (a))").get();
        // Before #4340 was fixed, we couldn't even create the following view:
        e.execute_cql("create materialized view mv as select a, b from cf "
                      "where a is not null "
                      "primary key (a)").get();
        // Let's check that after fixing #4340, we can not only create the
        // view, it also works as expected:
        e.execute_cql("insert into cf (a, b, c) values (1, 2, 3)").get();
        BOOST_TEST_PASSPOINT();
        auto msg = e.execute_cql("select * from cf").get();
        assert_that(msg).is_rows().with_size(1)
                .with_row({{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}});
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from mv").get();
            assert_that(msg).is_rows().with_size(1)
                    .with_row({{int32_type->decompose(1)}, {int32_type->decompose(2)}});
        });
    });
}

// This is a second reproducer for issue #4340. Here we create a more useful
// materialized view than the trivial one in the previous test, but in the
// view, put all key columns as partition keys, none of them in clustering
// keys. There is no reason why this shouldn't be allowed.
SEASTAR_TEST_CASE(test_no_clustering_key_2) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, primary key (a))").get();
        // Before #4340 was fixed, we couldn't even create the following view:
        e.execute_cql("create materialized view mv as select a, b from cf "
                      "where a is not null and b is not null "
                      "primary key ((a,b))").get();
        // Let's check that after fixing #4340, we can not only create the
        // view, it also works as expected:
        e.execute_cql("insert into cf (a, b, c) values (1, 2, 3)").get();
        BOOST_TEST_PASSPOINT();
        auto msg = e.execute_cql("select * from cf").get();
        assert_that(msg).is_rows().with_size(1)
                .with_row({{int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)}});
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from mv where a = 1 and b = 2").get();
            assert_that(msg).is_rows().with_size(1)
                    .with_row({{int32_type->decompose(1)}, {int32_type->decompose(2)}});
        });
    });
}
