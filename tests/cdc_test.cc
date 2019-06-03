/*
 * Copyright (C) 2019 ScyllaDB
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

#include <seastar/testing/thread_test_case.hh>

#include "tests/cql_test_env.hh"

SEASTAR_THREAD_TEST_CASE(test_with_cdc_parameter) {
    do_with_cql_env_thread([](cql_test_env& e) {
        struct expected {
            bool enabled = false;
            bool preimage = false;
            bool postimage = false;
            int ttl = 86400;
        };

        auto assert_cdc = [&] (const expected& exp) {
            BOOST_REQUIRE_EQUAL(exp.enabled,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().enabled());
            BOOST_REQUIRE_EQUAL(exp.preimage,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().preimage());
            BOOST_REQUIRE_EQUAL(exp.postimage,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().postimage());
            BOOST_REQUIRE_EQUAL(exp.ttl,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().ttl());
        };

        auto test = [&] (const sstring& create_prop,
                         const sstring& alter1_prop,
                         const sstring& alter2_prop,
                         const expected& create_expected,
                         const expected& alter1_expected,
                         const expected& alter2_expected) {
            e.execute_cql(format("CREATE TABLE ks.tbl (a int PRIMARY KEY) {}", create_prop)).get();
            assert_cdc(create_expected);
            e.execute_cql(format("ALTER TABLE ks.tbl WITH cdc = {}", alter1_prop)).get();
            assert_cdc(alter1_expected);
            e.execute_cql(format("ALTER TABLE ks.tbl WITH cdc = {}", alter2_prop)).get();
            assert_cdc(alter2_expected);
            e.execute_cql("DROP TABLE ks.tbl").get();
        };

        test("", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("WITH cdc = {'enabled':'true'}", "{'enabled':'false'}", "{'enabled':'true'}", {true}, {false}, {true});
        test("WITH cdc = {'enabled':'false'}", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("", "{'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", {false}, {true, true, true, 1}, {false});
        test("WITH cdc = {'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", "{'enabled':'true','preimage':'false','postimage':'true','ttl':'2'}", {true, true, true, 1}, {false}, {true, false, true, 2});
    }).get();
}

