/*
 * Copyright (C) 2018-present ScyllaDB
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


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <seastar/net/inet_address.hh>

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include "seastar/core/future-util.hh"
#include "seastar/core/sleep.hh"
#include "transport/messages/result_message.hh"
#include "utils/big_decimal.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/map.hh"

using namespace std::literals::chrono_literals;


SEASTAR_TEST_CASE(test_allow_filtering_check) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (p int, c int, v int, PRIMARY KEY(p, c));").get();
        e.require_table_exists("ks", "t").get();

        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j <3; ++j) {
                e.execute_cql(format("INSERT INTO t(p, c, v) VALUES ({}, {}, {})", i, j, j)).get();
            }
        }

        std::vector<sstring> queries = {
                "SELECT * FROM t WHERE p = 1",
                "SELECT * FROM t WHERE p = 1 and c > 2",
                "SELECT * FROM t WHERE p = 1 and c = 2"
        };

        for (const sstring& q : queries) {
            e.execute_cql(q).get();
            e.execute_cql(q + " ALLOW FILTERING").get();
        }

        e.execute_cql("CREATE TABLE t2 (p int PRIMARY KEY, a int, b int);").get();
        e.require_table_exists("ks", "t2").get();
        e.execute_cql("CREATE INDEX ON t2(a)").get();
        for (int i = 0; i < 5; ++i) {
            e.execute_cql(format("INSERT INTO t2 (p, a, b) VALUES ({}, {}, {})", i, i * 10, i * 100)).get();
        }

        queries = {
            "SELECT * FROM t2 WHERE p = 1",
            "SELECT * FROM t2 WHERE a = 20"
        };

        for (const sstring& q : queries) {
            e.execute_cql(q).get();
            e.execute_cql(q + " ALLOW FILTERING").get();
        }

        queries = {
            "SELECT * FROM t2 WHERE a = 20 AND b = 200"
        };

        for (const sstring& q : queries) {
            BOOST_CHECK_THROW(e.execute_cql(q).get(), exceptions::invalid_request_exception);
            e.execute_cql(q + " ALLOW FILTERING").get();
        }
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_pk_ck) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d));").get();
        e.require_table_exists("ks", "t").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (11, 12, 13, 14, 15)").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (11, 15, 16, 17, 18)").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (21, 22, 23, 24, 25)").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (31, 32, 33, 34, 35)").get();

        auto msg = e.execute_cql("SELECT * FROM t WHERE a = 11 AND b = 15 AND c = 16").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(11),
            int32_type->decompose(15),
            int32_type->decompose(16),
            int32_type->decompose(17),
            int32_type->decompose(18),
        }});

        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE a = 11 AND b = 12 AND c > 13 AND d = 14").get(), exceptions::invalid_request_exception);

        msg = e.execute_cql("SELECT * FROM t WHERE a = 11 AND b = 15 AND c = 16").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(11),
            int32_type->decompose(15),
            int32_type->decompose(16),
            int32_type->decompose(17),
            int32_type->decompose(18),
        }});

        msg = e.execute_cql("SELECT * FROM t WHERE a = 11 AND b = 15 AND c > 13 AND d >= 17 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(11),
            int32_type->decompose(15),
            int32_type->decompose(16),
            int32_type->decompose(17),
            int32_type->decompose(18),
        }});

        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE a = 11 AND b = 12 AND c > 13 AND d > 17").get(), exceptions::invalid_request_exception);

        msg = e.execute_cql("SELECT * FROM t WHERE a = 11 AND b = 15 AND c > 13 AND d >= 17 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(11),
            int32_type->decompose(15),
            int32_type->decompose(16),
            int32_type->decompose(17),
            int32_type->decompose(18),
        }});

        msg = e.execute_cql("SELECT * FROM t WHERE a <= 11 AND c > 15 AND d >= 16 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(11),
            int32_type->decompose(15),
            int32_type->decompose(16),
            int32_type->decompose(17),
            int32_type->decompose(18),
        }});

        msg = e.execute_cql("SELECT * FROM t WHERE a <= 11 AND b >= 15 AND c > 15 AND d >= 16 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(11),
            int32_type->decompose(15),
            int32_type->decompose(16),
            int32_type->decompose(17),
            int32_type->decompose(18),
        }});

        msg = e.execute_cql("SELECT * FROM t WHERE a <= 100 AND b >= 15 AND c > 0 AND d <= 100 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(11),
                int32_type->decompose(15),
                int32_type->decompose(16),
                int32_type->decompose(17),
                int32_type->decompose(18),
            },
            {
                int32_type->decompose(31),
                int32_type->decompose(32),
                int32_type->decompose(33),
                int32_type->decompose(34),
                int32_type->decompose(35),
            },
            {
                int32_type->decompose(21),
                int32_type->decompose(22),
                int32_type->decompose(23),
                int32_type->decompose(24),
                int32_type->decompose(25),
            }
        });

        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE a <= 11 AND c > 15 AND d >= 16").get(), exceptions::invalid_request_exception);
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_multi_column) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d));").get();
        e.require_table_exists("ks", "t").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (1, 1, 1, 1, 15)").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (1, 1, 1, 2, 18)").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (1, 2, 1, 2, 25)").get();
        e.execute_cql("INSERT INTO t (a,b,c,d,e) VALUES (1, 2, 1, 3, 35)").get();

        auto msg = e.execute_cql("SELECT * FROM t WHERE (c, d) = (1, 2)").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(18),
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(25),
            },
        });

        msg = e.execute_cql("SELECT * FROM t WHERE (c, d) IN ((1, 2), (1,3), (1,4))").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(18),
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(25),
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1),
                int32_type->decompose(3),
                int32_type->decompose(35),
            },
        });

        msg = e.execute_cql("SELECT * FROM t WHERE (c, d) < (1, 3)").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(15),
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(18),
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(25),
            },
        });

        msg = e.execute_cql("SELECT * FROM t WHERE (c, d) < (1, 3) AND (c, d) > (1, 1)").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(18),
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(25),
            },
        });

        // NOTICE(sarna): Currently multi-column restrictions can be applied to clustering columns only,
        // both in Scylla and C*. Cases below check that when we encounter a multi-column restriction
        // on different types of columns we fail in a sane way. If more general multi-column restrictions
        // that accept more types of columns are implemented, these cases should be replaced.
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE (a, b) < (1,2) ALLOW FILTERING").get(), exceptions::invalid_request_exception);
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE (b, c) < (1,2) ALLOW FILTERING").get(), exceptions::invalid_request_exception);
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE (d, e) < (1,2) ALLOW FILTERING").get(), exceptions::invalid_request_exception);
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE (a, b, c) IN ((1,2,3),(4,5,6)) ALLOW FILTERING").get(), exceptions::invalid_request_exception);
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE (a, c) = (3,4) ALLOW FILTERING").get(), exceptions::invalid_request_exception);
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE (b, d) > (4,5) ALLOW FILTERING").get(), exceptions::invalid_request_exception);
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_clustering_column) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (k int, c int, v int, PRIMARY KEY (k, c));").get();
        e.require_table_exists("ks", "t").get();

        e.execute_cql("INSERT INTO t (k, c, v) VALUES (1, 2, 1)").get();
        e.execute_cql("INSERT INTO t (k, c, v) VALUES (1, 3, 2)").get();
        e.execute_cql("INSERT INTO t (k, c, v) VALUES (2, 2, 3)").get();

        auto msg = e.execute_cql("SELECT * FROM t WHERE k = 1").get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1)
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(3),
                int32_type->decompose(2)
           }
        });

        msg = e.execute_cql("SELECT * FROM t WHERE k = 1 AND c > 2").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(1),
            int32_type->decompose(3),
            int32_type->decompose(2)
        }});

        msg = e.execute_cql("SELECT * FROM t WHERE k = 1 AND c = 2").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(1),
            int32_type->decompose(2),
            int32_type->decompose(1)
        }});

        msg = e.execute_cql("SELECT * FROM t WHERE c = 2").get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1)
            },
            {
                int32_type->decompose(2),
                int32_type->decompose(2),
                int32_type->decompose(3)
           }
        });

        msg = e.execute_cql("SELECT * FROM t WHERE c > 2 AND c <= 4").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(1),
            int32_type->decompose(3),
            int32_type->decompose(2)
        }});
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_two_clustering_columns) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (p int, c1 int, c2 int, data int, PRIMARY KEY (p, c1, c2))").get();

        e.execute_cql("INSERT INTO t (p, c1, c2, data) VALUES (1, 2, 3, 1)").get();
        e.execute_cql("INSERT INTO t (p, c1, c2, data) VALUES (1, 3, 4, 2)").get();
        e.execute_cql("INSERT INTO t (p, c1, c2, data) VALUES (1, 2, 5, 3)").get();
        e.execute_cql("INSERT INTO t (p, c1, c2, data) VALUES (2, 3, 4, 4)").get();

        auto res = e.execute_cql("SELECT * FROM t WHERE p = 1 and c1 < 3 and c2 > 3 ALLOW FILTERING").get0();
        assert_that(res).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(5),
                int32_type->decompose(3)
            }
        });
        // In issue #4121, we noticed that although with "SELECT *" filtering
        // was correct, when we select only a column *not* involved in the
        // filtering, one of the constraints was ignored.
        res = e.execute_cql("SELECT data FROM t WHERE p = 1 and c1 < 3 and c2 > 3 ALLOW FILTERING").get0();
        assert_that(res).is_rows().with_rows({
            {
                int32_type->decompose(3),
                // Because of issue #4126 our test code also sees as part of
                // the results additional columns which were requested just
                // for filtering, in this case c2 (=5) was necessary for the
                // filtering but c1 was not. These columns may change in the
                // future.
                int32_type->decompose(5)
            }
        });
        // Similar to the above test for issue #4121, but with more clustering
        // key components, two of them form a slice, two more need filtering.
        e.execute_cql("CREATE TABLE t2 (p int, c1 int, c2 int, c3 int, c4 int, data int, PRIMARY KEY (p, c1, c2, c3, c4))").get();
        e.execute_cql("INSERT INTO t2 (p, c1, c2, c3, c4, data) VALUES (1, 1, 2, 3, 3, 1)").get();
        e.execute_cql("INSERT INTO t2 (p, c1, c2, c3, c4, data) VALUES (1, 1, 2, 5, 8, 2)").get();
        e.execute_cql("INSERT INTO t2 (p, c1, c2, c3, c4, data) VALUES (1, 1, 2, 5, 4, 3)").get();
        e.execute_cql("INSERT INTO t2 (p, c1, c2, c3, c4, data) VALUES (1, 1, 4, 3, 4, 4)").get();
        e.execute_cql("INSERT INTO t2 (p, c1, c2, c3, c4, data) VALUES (1, 2, 4, 4, 2, 5)").get();
        res = e.execute_cql("SELECT data FROM t2 WHERE p = 1 and c1 = 1 and c2 < 3 and c3 > 4 and c4 < 7 ALLOW FILTERING").get0();
        assert_that(res).is_rows().with_rows({
            {
                int32_type->decompose(3),
                // Again, the following appear here just because of issue #4126
                int32_type->decompose(5),
                int32_type->decompose(4)
            }
        });
    });
}


SEASTAR_TEST_CASE(test_allow_filtering_static_column) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (a int, b int, c int, s int static, PRIMARY KEY(a, b));").get();
        e.require_table_exists("ks", "t").get();
        e.execute_cql("CREATE INDEX ON t(c)").get();

        e.execute_cql("INSERT INTO t (a, b, c, s) VALUES (1, 1, 1, 1)").get();
        e.execute_cql("INSERT INTO t (a, b, c) VALUES (1, 2, 1)").get();
        e.execute_cql("INSERT INTO t (a, s) VALUES (3, 3)").get();
        e.execute_cql("INSERT INTO t (a, b, c, s) VALUES (2, 1, 1, 2)").get();

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t WHERE c = 1 AND s = 2 ALLOW FILTERING").get0();
            assert_that(msg).is_rows().with_rows({{
                int32_type->decompose(2),
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1)
            }});
        });

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t WHERE c = 1 AND s = 1 ALLOW FILTERING").get0();
            assert_that(msg).is_rows().with_rows({
                {
                    int32_type->decompose(1),
                    int32_type->decompose(1),
                    int32_type->decompose(1),
                    int32_type->decompose(1)
                },
                {
                    int32_type->decompose(1),
                    int32_type->decompose(2),
                    int32_type->decompose(1),
                    int32_type->decompose(1)
               }
            });
        });
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_multiple_regular) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (a int, b int, c int, d int, e int, f list<int>, g set<int>, h map<int, text>, PRIMARY KEY(a, b));").get();
        e.require_table_exists("ks", "t").get();

        e.execute_cql("INSERT INTO t (a, b, c, d, e, f, g) VALUES (1, 1, 1, 1, 1, [1], {})").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e, f, g) VALUES (1, 2, 3, 4, 5, [1, 2], {1, 2, 3})").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e, f, g) VALUES (1, 3, 5, 1, 9, [1, 2, 3], {1, 2})").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e, f, g) VALUES (1, 4, 5, 7, 5, [], {1})").get();
        e.execute_cql("INSERT INTO t (a, b, g, h) VALUES (9, 5, {1, 2, 7}, {3: 'three'})").get();
        e.execute_cql("INSERT INTO t (a, b, g, h) VALUES (9, 6, {1, 3}, {3: 'three', 4: 'four'})").get();

        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE c = 5").get(), exceptions::invalid_request_exception);
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE d = 1").get(), exceptions::invalid_request_exception);
        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE e = 5").get(), exceptions::invalid_request_exception);

        auto my_list_type = list_type_impl::get_instance(int32_type, true);
        auto my_set_type = set_type_impl::get_instance(int32_type, true);
        auto my_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);

        auto msg = e.execute_cql("SELECT f FROM t WHERE f contains 1 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{1}}))},
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{1, 2}}))},
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{1, 2, 3}}))},
        });

        msg = e.execute_cql("SELECT f FROM t WHERE f contains 2 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{1, 2}}))},
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{1, 2, 3}}))},
        });

        msg = e.execute_cql("SELECT f FROM t WHERE f contains 2 AND f contains 3 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_list_type->decompose(make_list_value(my_list_type, list_type_impl::native_type{{1, 2, 3}}))},
        });

        msg = e.execute_cql("SELECT g FROM t WHERE g contains 7 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type{{1, 2, 7}}))},
        });

        msg = e.execute_cql("SELECT g FROM t WHERE g contains 1 and g contains 7 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type{{1, 2, 7}}))},
        });

        msg = e.execute_cql("SELECT h FROM t WHERE h contains key 3 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type{{{3, "three"}}}))},
            {my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type{{{3, "three"}, {4, "four"}}}))},
        });

        msg = e.execute_cql("SELECT h FROM t WHERE h contains 'four' ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type{{{3, "three"}, {4, "four"}}}))},
        });

        msg = e.execute_cql("SELECT h FROM t WHERE h contains key 3 and h contains 'four' ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type{{{3, "three"}, {4, "four"}}}))},
        });

        msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE c = 3 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(1),
            int32_type->decompose(2),
            int32_type->decompose(3),
            int32_type->decompose(4),
            int32_type->decompose(5)
        }});

        msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE e >= 5 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(3),
                int32_type->decompose(4),
                int32_type->decompose(5)
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(3),
                int32_type->decompose(5),
                int32_type->decompose(1),
                int32_type->decompose(9)
           },
           {
                int32_type->decompose(1),
                int32_type->decompose(4),
                int32_type->decompose(5),
                int32_type->decompose(7),
                int32_type->decompose(5)
           }
        });

        msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE c = 5 and e = 9 and d = 1 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(1),
            int32_type->decompose(3),
            int32_type->decompose(5),
            int32_type->decompose(1),
            int32_type->decompose(9)
        }});

        cql3::prepared_cache_key_type prepared_id = e.prepare("SELECT a, b, c, d, e FROM t WHERE a = ? and d = ? ALLOW FILTERING").get0();
        std::vector<cql3::raw_value> raw_values {
                cql3::raw_value::make_value(int32_type->decompose(1)),
                cql3::raw_value::make_value(int32_type->decompose(1))
        };
        msg = e.execute_prepared(prepared_id, raw_values).get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1)
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(3),
                int32_type->decompose(5),
                int32_type->decompose(1),
                int32_type->decompose(9)
           }
        });

        prepared_id = e.prepare("SELECT a, b, c, d, e FROM t WHERE a = ? and d = ? ALLOW FILTERING").get0();
        raw_values[1] = cql3::raw_value::make_value(int32_type->decompose(9));
        msg = e.execute_prepared(prepared_id, raw_values).get0();
        assert_that(msg).is_rows().with_size(0);


    });
}

SEASTAR_TEST_CASE(test_allow_filtering_desc) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (a int, b int, c int, d int, e int, PRIMARY KEY((a, b), c, d)) WITH CLUSTERING ORDER BY (c DESC);").get();
        e.require_table_exists("ks", "t").get();

        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 2, 1, 1, 1)").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 2, 3, 4, 5)").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 2, 5, 1, 9)").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 2, 6, 7, 5)").get();

        auto msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE c > 3").get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(6),
                int32_type->decompose(7),
                int32_type->decompose(5)
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(5),
                int32_type->decompose(1),
                int32_type->decompose(9)
            }
        });

        msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE c < 4").get0();
        assert_that(msg).is_rows().with_rows({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(3),
                int32_type->decompose(4),
                int32_type->decompose(5)
            },
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1),
                int32_type->decompose(1),
                int32_type->decompose(1)
            }
        });

        msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE c = 4").get0();
        assert_that(msg).is_rows().with_size(0);
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_with_secondary_index) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (a int, b int, c int, d int, e int, PRIMARY KEY(a, b));").get();
        e.require_table_exists("ks", "t").get();
        e.execute_cql("CREATE INDEX ON t(c)").get();

        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 1, 1, 1, 1)").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 2, 3, 4, 5)").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 3, 5, 1, 9)").get();
        e.execute_cql("INSERT INTO t (a, b, c, d, e) VALUES (1, 4, 5, 7, 5)").get();

        auto msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE c = 3").get0();
        assert_that(msg).is_rows().with_rows({{
            int32_type->decompose(1),
            int32_type->decompose(2),
            int32_type->decompose(3),
            int32_type->decompose(4),
            int32_type->decompose(5)
        }});

        BOOST_CHECK_THROW(e.execute_cql("SELECT * FROM t WHERE c = 5 and d = 1").get(), exceptions::invalid_request_exception);

        msg = e.execute_cql("SELECT a, b, c, d, e FROM t WHERE c = 5 and d = 1 ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows({{
                int32_type->decompose(1),
                int32_type->decompose(3),
                int32_type->decompose(5),
                int32_type->decompose(1),
                int32_type->decompose(9)
        }});

        e.execute_cql("CREATE TABLE t2 (pk1 int, pk2 int, c1 int, c2 int, v int, PRIMARY KEY ((pk1, pk2), c1, c2));").get();
        e.execute_cql("CREATE INDEX ON t2(v)").get();
        for (int i = 1; i <= 5; ++i) {
            for (int j = 1; j <= 2; ++j) {
                e.execute_cql(format("INSERT INTO t2 (pk1, pk2, c1, c2, v) VALUES ({}, {}, {}, {}, {})", j, 1, 1, 1, i)).get();
                e.execute_cql(format("INSERT INTO t2 (pk1, pk2, c1, c2, v) VALUES ({}, {}, {}, {}, {})", j, 1, 1, i, i)).get();
                e.execute_cql(format("INSERT INTO t2 (pk1, pk2, c1, c2, v) VALUES ({}, {}, {}, {}, {})", j, 1, i, i, i)).get();
                e.execute_cql(format("INSERT INTO t2 (pk1, pk2, c1, c2, v) VALUES ({}, {}, {}, {}, {})", j, i, i, i, i)).get();
            }
        }

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t2 WHERE pk1 = 1 AND c1 > 0 AND c1 < 5 AND c2 = 1 AND v = 3 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_rows({});
        });

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t2 WHERE pk1 = 1 AND  c1 > 0 AND c1 < 5 AND c2 = 3 AND v = 3 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_rows({
                {
                    int32_type->decompose(1),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3)
                },
                {
                    int32_type->decompose(1),
                    int32_type->decompose(1),
                    int32_type->decompose(1),
                    int32_type->decompose(3),
                    int32_type->decompose(3)
                },
                {
                    int32_type->decompose(1),
                    int32_type->decompose(1),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3)
                }
            });
        });

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t2 WHERE pk1 = 1 AND  c2 > 1 AND c2 < 5 AND v = 1 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_rows({});
        });

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t2 WHERE pk1 = 1 AND  c1 > 1 AND c2 > 2 AND v = 3 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_rows({
                {
                    int32_type->decompose(1),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3)
                },
                {
                    int32_type->decompose(1),
                    int32_type->decompose(1),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3)
                }
            });
        });

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t2 WHERE pk1 = 1 AND  pk2 > 1 AND c2 > 2 AND v = 3 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_rows({{
                    int32_type->decompose(1),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3),
                    int32_type->decompose(3)
            }});
        });

        eventually([&] {
            auto msg = e.execute_cql("SELECT * FROM t2 WHERE pk1 >= 2 AND pk2 <=3 AND  c1 IN(0,1,2) AND c2 IN(0,1,2) AND v < 3  ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_rows({
                {
                    int32_type->decompose(2),
                    int32_type->decompose(2),
                    int32_type->decompose(2),
                    int32_type->decompose(2),
                    int32_type->decompose(2)
                },
                {
                    int32_type->decompose(2),
                    int32_type->decompose(1),
                    int32_type->decompose(1),
                    int32_type->decompose(2),
                    int32_type->decompose(2)
                },
                {
                    int32_type->decompose(2),
                    int32_type->decompose(1),
                    int32_type->decompose(2),
                    int32_type->decompose(2),
                    int32_type->decompose(2)
                }
            });
        });

        eventually([&] {
            auto msg = cquery_nofail(e, "SELECT SUM(e) FROM t WHERE c = 5 AND b = 911 ALLOW FILTERING;");
            assert_that(msg).is_rows().with_rows({{ int32_type->decompose(0), {} }});
            msg = cquery_nofail(e, "SELECT e FROM t WHERE c = 5 AND b = 3 ALLOW FILTERING;");
            assert_that(msg).is_rows().with_rows({{ int32_type->decompose(9), int32_type->decompose(3) }});
        });
    });
}

static lw_shared_ptr<service::pager::paging_state> extract_paging_state(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    auto paging_state = rows->rs().get_metadata().paging_state();
    if (!paging_state) {
        return nullptr;
    }
    return make_lw_shared<service::pager::paging_state>(*paging_state);
};

static size_t count_rows_fetched(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    return rows->rs().result_set().size();
};


SEASTAR_TEST_CASE(test_allow_filtering_limit) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE timeline (user text, c int, liked boolean, PRIMARY KEY (user, c));").get();
        e.execute_cql(
                "BEGIN UNLOGGED BATCH \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',1,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',2,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',3,true);  \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',4,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',5,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',6,false); \n"
                "APPLY BATCH;"
        ).get();

        auto msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=true ALLOW FILTERING;").get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(3), boolean_type->decompose(true)},
        });

        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false ALLOW FILTERING;").get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)},
            { int32_type->decompose(4), boolean_type->decompose(false)},
            { int32_type->decompose(5), boolean_type->decompose(false)},
            { int32_type->decompose(6), boolean_type->decompose(false)},
        });

        auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=true LIMIT 1 ALLOW FILTERING;", std::move(qo)).get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(3), boolean_type->decompose(true)},
        });

        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false LIMIT 5 ALLOW FILTERING;", std::move(qo)).get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)},
            { int32_type->decompose(4), boolean_type->decompose(false)},
            { int32_type->decompose(5), boolean_type->decompose(false)},
            { int32_type->decompose(6), boolean_type->decompose(false)},
        });

        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false LIMIT 2 ALLOW FILTERING;", std::move(qo)).get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)}
        });

        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false LIMIT 3 ALLOW FILTERING;", std::move(qo)).get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)},
            { int32_type->decompose(4), boolean_type->decompose(false)}
        });

        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{1, nullptr, {}, api::new_timestamp()});
        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false LIMIT 3 ALLOW FILTERING;", std::move(qo)).get0();
        auto paging_state = extract_paging_state(msg);
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)}
        });

        // Some pages might be empty and in such case we should continue querying
        size_t rows_fetched = 0;
        while (rows_fetched == 0) {
            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false LIMIT 3 ALLOW FILTERING;", std::move(qo)).get0();
            rows_fetched = count_rows_fetched(msg);
            paging_state = extract_paging_state(msg);
        }
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(2), boolean_type->decompose(false)}
        });

        rows_fetched = 0;
        while (rows_fetched == 0) {
            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false LIMIT 3 ALLOW FILTERING;", std::move(qo)).get0();
            rows_fetched = count_rows_fetched(msg);
            if (rows_fetched == 0) {
                paging_state = extract_paging_state(msg);
            }
        }
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(4), boolean_type->decompose(false)}
        });

        // Assert that with LIMIT 3 and paging 1 we will not extract more than 3 values (issue #4100)
        rows_fetched = 0;
        uint64_t remaining = 1;
        while (remaining > 0) {
            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false LIMIT 3 ALLOW FILTERING;", std::move(qo)).get0();
            rows_fetched += count_rows_fetched(msg);
            paging_state = extract_paging_state(msg);
            if (!paging_state) {
                remaining = 0;
            } else if (remaining > 0) {
                remaining = paging_state->get_remaining();
            }
        }
        BOOST_REQUIRE_EQUAL(rows_fetched, 1U);
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(4), boolean_type->decompose(false)}
        });
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_per_partition_limit) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE timeline (user text, c int, liked boolean, PRIMARY KEY (user, c));").get();
        e.execute_cql(
                "BEGIN UNLOGGED BATCH \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',1,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',2,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',3,true);  \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',4,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',5,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('a',6,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('b',1,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('b',2,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('b',3,true);  \n"
                "insert INTO timeline (user, c, liked) VALUES ('b',4,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('b',5,false); \n"
                "insert INTO timeline (user, c, liked) VALUES ('b',6,false); \n"
                "APPLY BATCH;"
        ).get();

        auto msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false PER PARTITION LIMIT 2 ALLOW FILTERING;").get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)},
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)},
        });

        msg = e.execute_cql("SELECT c, liked FROM timeline PER PARTITION LIMIT 2;").get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)},
            { int32_type->decompose(1), boolean_type->decompose(false)},
            { int32_type->decompose(2), boolean_type->decompose(false)},
        });

        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE user='b' PER PARTITION LIMIT 2 LIMIT 1 ALLOW FILTERING;").get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
        });

        auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                cql3::query_options::specific_options{100, nullptr, {}, api::new_timestamp()});
        msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=true PER PARTITION LIMIT 1 ALLOW FILTERING;", std::move(qo)).get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(3), boolean_type->decompose(true)},
            { int32_type->decompose(3), boolean_type->decompose(true)},
        });

        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
            cql3::query_options::specific_options{3, nullptr, {}, api::new_timestamp()});
        msg = e.execute_cql("SELECT c, liked FROM timeline PER PARTITION LIMIT 1;", std::move(qo)).get0();
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)},
        });

        lw_shared_ptr<service::pager::paging_state> paging_state = nullptr;
        // Some pages might be empty and in such case we should continue querying
        size_t rows_fetched = 0;
        while (rows_fetched == 0) {
            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{1, paging_state, {}, api::new_timestamp()});
            msg = e.execute_cql("SELECT c, liked FROM timeline WHERE liked=false PER PARTITION LIMIT 1 ALLOW FILTERING;", std::move(qo)).get0();
            rows_fetched = count_rows_fetched(msg);
            paging_state = extract_paging_state(msg);
        }
        assert_that(msg).is_rows().with_rows({
            { int32_type->decompose(1), boolean_type->decompose(false)}
        });

        // Assert that with paging pg, PER PARTITION LIMIT ppl and 2 partitions we will not extract more than 2*X values
        for (bool allow_filtering : {false, true}) {
            for (int pg = 1; pg < 12; ++pg) {
                for (unsigned ppl = 1; ppl < 3; ++ppl) {
                    paging_state = nullptr;
                    rows_fetched = 0;
                    uint64_t remaining = 1;
                    while (remaining > 0) {
                        qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                                cql3::query_options::specific_options{pg, paging_state, {}, api::new_timestamp()});
                        sstring query = allow_filtering ?
                                fmt::format("SELECT c, liked FROM timeline WHERE liked=false PER PARTITION LIMIT {} ALLOW FILTERING;", ppl) :
                                fmt::format("SELECT c, liked FROM timeline PER PARTITION LIMIT {};", ppl);
                        msg = e.execute_cql(query, std::move(qo)).get0();
                        rows_fetched += count_rows_fetched(msg);
                        paging_state = extract_paging_state(msg);
                        if (!paging_state) {
                            remaining = 0;
                        } else if (remaining > 0) {
                            remaining = paging_state->get_remaining();
                        }
                    }
                    BOOST_REQUIRE_EQUAL(rows_fetched, 2 * ppl);
                }
            }
        }
    });
}

SEASTAR_TEST_CASE(test_allow_filtering_with_in_on_regular_column) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE t (k int, c int, v int, PRIMARY KEY (k, c));").get();
        e.require_table_exists("ks", "t").get();

        e.execute_cql("INSERT INTO t (k, c, v) VALUES (1, 2, 1)").get();
        e.execute_cql("INSERT INTO t (k, c, v) VALUES (1, 3, 2)").get();
        e.execute_cql("INSERT INTO t (k, c, v) VALUES (2, 2, 3)").get();

        auto msg = e.execute_cql("SELECT * FROM t WHERE v IN (1) ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1)
            }
        });

        msg = e.execute_cql("SELECT * FROM t WHERE v IN (2, 3) ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {
                int32_type->decompose(1),
                int32_type->decompose(3),
                int32_type->decompose(2)
            },
            {
                int32_type->decompose(2),
                int32_type->decompose(2),
                int32_type->decompose(3)
           }
        });

        msg = e.execute_cql("SELECT * FROM t WHERE c in (2, 4) AND v IN (1, 2, 3, 4, 5) ALLOW FILTERING").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {
                int32_type->decompose(1),
                int32_type->decompose(2),
                int32_type->decompose(1)
            },
            {
                int32_type->decompose(2),
                int32_type->decompose(2),
                int32_type->decompose(3)
           }
        });
    });
}

SEASTAR_TEST_CASE(test_filtering_on_empty_partition_with_a_static_row) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE t (p int, c int, s int static, PRIMARY KEY(p, c));");
        cquery_nofail(e, "INSERT INTO t (p, s) VALUES (1, 1);");
        auto msg = cquery_nofail(e, "SELECT * FROM t WHERE s = 2 ALLOW FILTERING;");
        assert_that(msg).is_rows().is_empty();
        msg = cquery_nofail(e, "SELECT * FROM t WHERE c = 1");
        assert_that(msg).is_rows().is_empty();
        cquery_nofail(e, "INSERT INTO t (p, c, s) VALUES (2, 2, 2);");
        msg = cquery_nofail(e, "SELECT * FROM t WHERE s = 1 ALLOW FILTERING");
        assert_that(msg).is_rows().with_rows({
            {int32_type->decompose(1), {}, int32_type->decompose(1)}
        });
    });
}

SEASTAR_TEST_CASE(test_filtering) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE cf (k int, v int,m int,n int,o int,p int static, PRIMARY KEY ((k,v),m,n));").get();
        e.execute_cql(
                "BEGIN UNLOGGED BATCH \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (1, 1, 1, 1, 1 ,1 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (2, 1, 2, 1, 2 ,2 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (3, 1, 3, 1, 3 ,3 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (4, 2, 1, 2, 4 ,4 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (5, 2, 2, 2, 5 ,5 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (6, 2, 3, 2, 6 ,6 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (7, 3, 1, 3, 7 ,7 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (8, 3, 2, 3, 8 ,8 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (9, 3, 3, 3, 9 ,9 ); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (10, 4, 1, 4,10,10); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (11, 4, 2, 4,11,11); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (12, 5, 3, 5,12,12); \n"
                "INSERT INTO cf (k, v, m, n, o, p) VALUES (12, 5, 4, 5,13,13); \n"
                "APPLY BATCH;"
        ).get();

        // Notice the with_serialized_columns_count() check before the set comparison.
        // Since we are dealing with the result set before serializing to the client,
        // there is an extra column that is used for the filtering, this column will
        // not be present in the responce to the client and with_serialized_columns_count()
        // verifies exactly that.

        // test filtering on partition keys
        {
            auto msg = e.execute_cql("SELECT k FROM cf WHERE v=3 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_serialized_columns_count(1).with_rows_ignore_order({
                { int32_type->decompose(7), int32_type->decompose(3)},
                { int32_type->decompose(8), int32_type->decompose(3) },
                { int32_type->decompose(9), int32_type->decompose(3) },
            });
            require_rows(e, "SELECT k FROM cf WHERE k=12 AND (m,n)>=(4,0) ALLOW FILTERING;", {
                    { int32_type->decompose(12), int32_type->decompose(4), int32_type->decompose(5)},
                });
        }

        // test filtering on clustering keys
        {
            auto msg = e.execute_cql("SELECT k FROM cf WHERE n=4 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_serialized_columns_count(1).with_rows_ignore_order({
                { int32_type->decompose(10), int32_type->decompose(4) },
                { int32_type->decompose(11), int32_type->decompose(4) },
            });
        }

        //test filtering on regular columns
        {
            auto msg = e.execute_cql("SELECT k FROM cf WHERE o>7 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_serialized_columns_count(1).with_rows_ignore_order({
                { int32_type->decompose(8),  int32_type->decompose(8) },
                { int32_type->decompose(9),  int32_type->decompose(9) },
                { int32_type->decompose(10), int32_type->decompose(10) },
                { int32_type->decompose(11), int32_type->decompose(11) },
                { int32_type->decompose(12), int32_type->decompose(12) },
                { int32_type->decompose(12), int32_type->decompose(13) },
            });
        }

        //test filtering on static columns
        {
            auto msg = e.execute_cql("SELECT k FROM cf WHERE p>=10 AND p<=12 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_serialized_columns_count(1).with_rows_ignore_order({
                { int32_type->decompose(10), int32_type->decompose(10) },
                { int32_type->decompose(11), int32_type->decompose(11) },
            });
        }
        //test filtering with count
        {
            auto msg = e.execute_cql("SELECT COUNT(k) FROM cf WHERE n>3 ALLOW FILTERING;").get0();
            assert_that(msg).is_rows().with_serialized_columns_count(1).with_size(1).with_rows_ignore_order({
                { long_type->decompose(4L), int32_type->decompose(5) },
            });
        }

    });
}
