/*
 * Copyright (C) 2016-present ScyllaDB
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


#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/map.hpp>

#include "database.hh"
#include "types/user.hh"
#include "db/view/view_builder.hh"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "types/set.hh"
#include "types/list.hh"

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_compound_partition_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p1 int, p2 int, v int, primary key ((p1, p2)))").get();
        auto s = e.local_db().find_schema(sstring("ks"), sstring("cf"));
        auto& p1 = *s->get_column_definition(to_bytes(sstring("p1")));
        auto& p2 = *s->get_column_definition(to_bytes(sstring("p2")));

        for (auto&& cdef : s->all_columns_in_select_order()) {
            auto maybe_failed = [&cdef] (auto&& f) {
                try {
                    f.get();
                } catch (...) {
                    if (!cdef.is_partition_key()) {
                        BOOST_FAIL("MV creation should have failed");
                    }
                }
            };
            maybe_failed(e.execute_cql(sprint(
                        "create materialized view mv1_%s as select * from cf "
                        "where %s is not null and p1 is not null %s "
                        "primary key (%s, p1 %s)",
                        cdef.name_as_text(), cdef.name_as_text(), cdef == p2 ? "" : "and p2 is not null",
                        cdef.name_as_text(), cdef == p2 ? "" : ", p2")));
            maybe_failed(e.execute_cql(sprint(
                        "create materialized view mv2_%s as select * from cf "
                        "where %s is not null and p1 is not null %s "
                        "primary key (%s, p2 %s)",
                        cdef.name_as_text(), cdef.name_as_text(), cdef == p2 ? "" : "and p2 is not null",
                        cdef.name_as_text(), cdef == p1 ? "" : ", p1")));
            maybe_failed(e.execute_cql(sprint(
                        "create materialized view mv3_%s as select * from cf "
                        "where %s is not null and p1 is not null %s "
                        "primary key ((%s, p1), p2)",
                        cdef.name_as_text(), cdef.name_as_text(), cdef == p2 ? "" : "and p2 is not null", cdef.name_as_text())));
        }

        e.execute_cql("insert into cf (p1, p2, v) values (0, 2, 5)").get();

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv1_p2 where p2 = 2").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(5)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv2_p1 where p2 = 2 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(5)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1 from mv1_v where v = 5").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p2 from mv3_v where v = 5 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(2)},
                });
        });

        e.execute_cql("insert into cf (p1, p2, v) values (0, 2, 8)").get();

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv1_p2 where p2 = 2").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(8)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv2_p1 where p2 = 2 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(8)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1 from mv1_v where v = 5").get0();
        assert_that(msg).is_rows()
                .with_size(0);
        });

        eventually([&] {
        auto msg = e.execute_cql("select p2 from mv3_v where v = 5 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(0);
        });
        eventually([&] {
        auto msg = e.execute_cql("select p2 from mv3_v where v = 8 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(2)},
                });
        });
    });
}

SEASTAR_TEST_CASE(test_partition_key_only_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p1 int, p2 int, primary key ((p1, p2)));").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where p1 is not null and p2 is not null primary key (p2, p1)").get();

        e.execute_cql("insert into cf (p1, p2) values (1, 1)").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(1)}, {int32_type->decompose(1)} });
        });
    });
}

SEASTAR_TEST_CASE(test_delete_single_column_in_view_partition_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b))").get();
        e.execute_cql("create materialized view mv as select * from cf "
                              "where a is not null and b is not null and d is not null "
                              "primary key (d, a, b)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        });

        e.execute_cql("delete c from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, { } });
        });


        e.execute_cql("delete d from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b from mv").get0();
        assert_that(msg).is_rows()
                .with_size(0);
        });

    });
}

SEASTAR_TEST_CASE(test_partition_key_filtering_unrestricted_part) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b is not null and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

// Test that although normal SELECT queries limit the ability to query
// for slices (ranges) of partition keys, because there is no way to implement
// such queries efficiently, the same slices *do* work for the SELECT statement
// defining a materialized view - there the condition is tested for each
// partition separately, so the performance concerns do not hold.
// This verifies part of issue #2367. See also test_clustering_key_in_restrictions.
SEASTAR_TEST_CASE(test_partition_key_filtering_with_slice) {
    return do_with_cql_env_thread([] (auto& e) {
        // Although the test below tests that slices are allowed in MV's
        // SELECT, let's first verify that these slices are still *not*
        // allowed in ordinary SELECT queries:

        e.execute_cql("create table cf (a int, b int, primary key (a))").get();
        try {
            e.execute_cql("select * from cf where a > 0").get();
            BOOST_FAIL("slice query of partition key");
        } catch (exceptions::invalid_request_exception&) {
            // Expecting "Only EQ and IN relation are supported on the
            // partition key (unless you use the token() function)"
        }
        e.execute_cql("drop table cf").get();

        e.execute_cql("create table cf (a int, b int, c int, primary key (a, b, c))").get();
        try {
            e.execute_cql("select * from cf where a = 1 and b > 0 and c > 0").get();
            BOOST_FAIL("slice of clustering key with non-contiguous range");
        } catch (exceptions::invalid_request_exception&) {
            // Expecting "Clustering column "c" cannot be restricted
            // (preceding column "b" is restricted by a non-EQ relation)"
        }
        e.execute_cql("drop table cf").get();

        e.execute_cql("create table cf (a int, b int, c int, primary key (a, b, c))").get();
        try {
            e.execute_cql("select * from cf where a = 1 and c = 1 and b > 0").get();
            BOOST_FAIL("slice of clustering key with non-contiguous range");
        } catch (exceptions::invalid_request_exception&) {
            // Expecting "PRIMARY KEY column "c" cannot be restricted
            // (preceding column "b" is restricted by a non-EQ relation)
        }
        e.execute_cql("drop table cf").get();


        // And now for the actual MV tests, where the slices should be allowed:
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a > 0 and b > 5 and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 1)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 10, 1, 2)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 2, 1)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 10, 2, 2)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (2, 1, 3, 1)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (2, 10, 3, 2)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} }});
            });

            e.execute_cql("insert into cf (a, b, c, d) values (3, 10, 4, 2)").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(2)} }});
            });


            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(2)} }});
            });

            e.execute_cql("update cf set d = 100 where a = 3 and b = 10 and c = 4").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(100)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(100)} }});
            });

            e.execute_cql("delete from cf where a = 3 and b = 10 and c = 4").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} }});
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_partition_key_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b is not null and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_partition_key_compound_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b = 1 and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
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
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 0 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 1").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_partition_key_and_clustering_key_filtering_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b is not null and c = 1 "
                                 "primary key %s", pk)).get();

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
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
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
            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key (p1, v, c, p2)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((p2, v), c, p1)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, p2), c, p1)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((c, v), p1, p2)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, c), p1, p2)"
        };
        for (auto&& view : views_matching) {
            auto name = make_view_name();
            auto f = e.local_view_builder().wait_until_built("ks", name);
            e.execute_cql(sprint(view, name)).get();
            f.get();
            auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get0();
            assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                            {int32_type->decompose(1)},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                    });
        }

        auto views_not_matching = {
                "create materialized view %s as select * from cf "
                "where p1 is not null and p2 is not null and c is not null and v is not null "
                "primary key (c, p1, p2, v)",

                "create materialized view %s as select * from cf "
                "where p1 is not null and p2 is not null and c is not null and v is not null "
                "primary key (p2, p1, c, v)"
        };
        for (auto&& view : views_not_matching) {
            auto name = make_view_name();
            auto f = e.local_view_builder().wait_until_built("ks", name);
            e.execute_cql(sprint(view, name)).get();
            f.get();
            auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get0();
            assert_that(msg).is_rows().is_empty();
        }
        auto name = make_view_name();
        auto f = e.local_view_builder().wait_until_built("ks", name);
        e.execute_cql(sprint("create materialized view %s as select * from cf "
                             "where p1 is not null and p2 is not null and c is not null and v is not null "
                             "primary key (v, p1, p2, c)", name)).get();
        f.get();
        auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get0();
        assert_that(msg).is_rows().is_empty();

        e.local_db().flush_all_memtables().get();
        e.execute_cql("update cf set v = 'a' where p1 = 1 and p2 = '' and c = ''").get();
        eventually([&] {
            auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get0();
            assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                            {int32_type->decompose(1)},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose("a")},
                    });
        });
        e.execute_cql("update cf set v = '' where p1 = 1 and p2 = '' and c = ''").get();
        eventually([&] {
            auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get0();
            assert_that(msg).is_rows().is_empty();
        });
        e.execute_cql("delete v from cf where p1 = 1 and p2 = '' and c = ''").get();
        eventually([&] {
            auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get0();
            assert_that(msg).is_rows().is_empty();
        });
    });
}
