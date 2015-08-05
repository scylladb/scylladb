/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <stdint.h>

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "core/future-util.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"

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
