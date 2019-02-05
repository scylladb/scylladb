/*
 * Copyright (C) 2015 ScyllaDB
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
#include <stdint.h>

#include <seastar/testing/test_case.hh>
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"

SEASTAR_TEST_CASE(test_execute_internal_insert) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        return qp.execute_internal("create table ks.cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").then([](auto rs) {
            BOOST_REQUIRE(rs->empty());
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key1"), 1, 100 }).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = 1;", { sstring("key1") }).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                auto i = rs->one().template get_as<int32_t>("r1");
                BOOST_CHECK_EQUAL(i, int32_t(100));
            });
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key2"), 2, 200 }).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf;").then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                BOOST_CHECK_EQUAL(rs->size(), 2);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_execute_internal_delete) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        return qp.execute_internal("create table ks.cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").then([](auto rs) {
            BOOST_REQUIRE(rs->empty());
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key1"), 1, 100 }).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("delete from ks.cf where p1 = ? and c1 = ?;", { sstring("key1"), 1 }).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf;").then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        });
    });
}

SEASTAR_TEST_CASE(test_execute_internal_update) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        return qp.execute_internal("create table ks.cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").then([](auto rs) {
            BOOST_REQUIRE(rs->empty());
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key1"), 1, 100 }).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = 1;", { sstring("key1") }).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                auto i = rs->one().template get_as<int32_t>("r1");
                BOOST_CHECK_EQUAL(i, int32_t(100));
            });
        }).then([&qp] {
            return qp.execute_internal("update ks.cf set r1 = ? where p1 = ? and c1 = ?;", { 200, sstring("key1"), 1 }).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = 1;", { sstring("key1") }).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                auto i = rs->one().template get_as<int32_t>("r1");
                BOOST_CHECK_EQUAL(i, int32_t(200));
            });
        });
    });
}

/*
 * Testing query with paging and consumer function.
 *
 * The following scenarios are beeing tested.
 * 1. Query of an empty table
 * 2. Insert 900 lines and query (under the page size).
 * 3. Fill up to 2200 lines and query (using multipl pages).
 * 4. Read only 1100 lines and stop using the stop iterator.
 */
SEASTAR_TEST_CASE(test_querying_with_consumer) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        int counter = 0;
        int sum = 0;
        int total = 0;
        e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
        auto& db = e.local_db();
        auto s = db.find_schema("ks", "cf");

        e.local_qp().query("SELECT * from ks.cf", [&counter] (const cql3::untyped_result_set::row& row) mutable {
            counter++;
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).get();

        BOOST_CHECK_EQUAL(counter, 0);

        for (auto i = 0; i < 900; i++) {
            total += i;
            e.local_qp().execute_internal("insert into ks.cf (k , v) values (?, ? );", { to_sstring(i), i}).get();
        }
        e.local_qp().query("SELECT * from ks.cf", [&counter, &sum] (const cql3::untyped_result_set::row& row) mutable {
            counter++;
            sum += row.get_as<int>("v");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).get();
        BOOST_CHECK_EQUAL(counter, 900);
        BOOST_CHECK_EQUAL(total, sum);
        counter = 0;
        sum = 0;
        for (auto i = 900; i < 2200; i++) {
            total += i;
            e.local_qp().execute_internal("insert into ks.cf (k , v) values (?, ? );", { to_sstring(i), i}).get();
        }
        e.local_qp().query("SELECT * from ks.cf", [&counter, &sum] (const cql3::untyped_result_set::row& row) mutable {
            counter++;
            sum += row.get_as<int>("v");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).get();
        BOOST_CHECK_EQUAL(counter, 2200);
        BOOST_CHECK_EQUAL(total, sum);
        counter = 1000;
        e.local_qp().query("SELECT * from ks.cf", [&counter] (const cql3::untyped_result_set::row& row) mutable {
            counter++;
            if (counter == 1010) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).get();
        BOOST_CHECK_EQUAL(counter, 1010);
    });
}
