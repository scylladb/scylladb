/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>

#include "db/view/view_builder.hh"

#include "test/lib/eventually.hh"
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

BOOST_AUTO_TEST_SUITE(view_schema_pkey_test)

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_partition_key_compound_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
            e.execute_cql(fmt::format("create materialized view vcf as select * from cf "
                                 "where a = 1 and b = 1 and c is not null "
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
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_partition_key_restrictions_not_include_all) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
        e.execute_cql("create materialized view vcf as select a, b, c from cf "
                      "where a = 1 and b = 1 and c is not null "
                      "primary key ((a, b), c)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

        eventually([&] {
        auto msg = e.execute_cql("select a, b, c from vcf").get();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 0 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 1").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get();
        assert_that(msg).is_rows().with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_partition_key_and_clustering_key_filtering_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(fmt::format("create materialized view vcf as select * from cf "
                                 "where a = 1 and b is not null and c = 1 "
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
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_base_non_pk_columns_in_view_partition_key_are_non_emtpy) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p1 int, p2 text, c text, v text, primary key ((p1, p2), c))").get();
        e.execute_cql("insert into cf (p1, p2, c, v) values (1, '', '', '')").get();

        size_t id = 0;
        auto make_view_name = [&id] { return format("vcf_{:d}", id++); };

        auto views_matching = {
            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key (p1, v, c, p2)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((p2, v), c, p1)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, p2), c, p1)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((c, v), p1, p2)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, c), p1, p2)"
        };
        for (auto&& view : views_matching) {
            auto name = make_view_name();
            auto f = e.local_view_builder().wait_until_built("ks", name);
            e.execute_cql(fmt::format(fmt::runtime(view), name)).get();
            f.get();
            auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get();
            assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                            {int32_type->decompose(1)},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                    });
        }
    });
}

BOOST_AUTO_TEST_SUITE_END()
