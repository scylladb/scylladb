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

#include "cdc/cdc.hh"
#include "tests/cql_assertions.hh"
#include "tests/cql_test_env.hh"
#include "transport/messages/result_message.hh"

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
            if (exp.enabled) {
                e.require_table_exists("ks", cdc::log_name("tbl")).get();
                e.require_table_exists("ks", cdc::desc_name("tbl")).get();
                auto msg = e.execute_cql(format("select node_ip, shard_id from ks.{};", cdc::desc_name("tbl"))).get0();
                std::vector<std::vector<bytes_opt>> expected_rows;
                expected_rows.reserve(smp::count);
                auto ip = inet_addr_type->decompose(
                        utils::fb_utilities::get_broadcast_address().addr());
                for (int i = 0; i < static_cast<int>(smp::count); ++i) {
                    expected_rows.push_back({ip, int32_type->decompose(i)});
                }
                assert_that(msg).is_rows().with_rows_ignore_order(std::move(expected_rows));
            } else {
                e.require_table_does_not_exist("ks", cdc::log_name("tbl")).get();
                e.require_table_does_not_exist("ks", cdc::desc_name("tbl")).get();
            }
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
            e.require_table_does_not_exist("ks", cdc::log_name("tbl")).get();
            e.require_table_does_not_exist("ks", cdc::desc_name("tbl")).get();
        };

        test("", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("WITH cdc = {'enabled':'true'}", "{'enabled':'false'}", "{'enabled':'true'}", {true}, {false}, {true});
        test("WITH cdc = {'enabled':'false'}", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("", "{'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", {false}, {true, true, true, 1}, {false});
        test("WITH cdc = {'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", "{'enabled':'true','preimage':'false','postimage':'true','ttl':'2'}", {true, true, true, 1}, {false}, {true, false, true, 2});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_primary_key_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 22, 222, 2222, 22222)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 33, 333, 3333, 33333)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 44, 444, 4444, 44444)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(2, 11, 111, 1111, 11111)");
        cquery_nofail(e, "DELETE val FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444");
        cquery_nofail(e, "UPDATE ks.tbl SET val = 555 WHERE pk = 2 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11");
        auto msg = e.execute_cql(format("SELECT time, \"_pk\", \"_pk2\", \"_ck\", \"_ck2\" FROM ks.{}", cdc::log_name("tbl"))).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        auto rs = rows->rs().result_set().rows();
        std::vector<std::vector<bytes_opt>> results;
        for (auto it = rs.begin(); it != rs.end(); ++it) {
            results.push_back(*it);
        }
        std::sort(results.begin(), results.end(),
                [] (const std::vector<bytes_opt>& a, const std::vector<bytes_opt>& b) {
                    return timeuuid_type->as_less_comparator()(*a[0], *b[0]);
                });
        auto actual_i = results.begin();
        auto actual_end = results.end();
        auto assert_row = [&] (int pk, int pk2, int ck = -1, int ck2 = -1) {
            std::cerr << "check " << pk << " " << pk2 << " " << ck << " " << ck2 << std::endl;
            BOOST_REQUIRE(actual_i != actual_end);
            auto& actual_row = *actual_i;
            BOOST_REQUIRE_EQUAL(int32_type->decompose(pk), actual_row[1]);
            BOOST_REQUIRE_EQUAL(int32_type->decompose(pk2), actual_row[2]);
            if (ck != -1) {
                BOOST_REQUIRE_EQUAL(int32_type->decompose(ck), actual_row[3]);
            } else {
                BOOST_REQUIRE(!actual_row[3]);
            }
            if (ck2 != -1) {
                BOOST_REQUIRE_EQUAL(int32_type->decompose(ck2), actual_row[4]);
            } else {
                BOOST_REQUIRE(!actual_row[4]);
            }
            ++actual_i;
        };
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)
        assert_row(1, 11, 111, 1111);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 22, 222, 2222, 22222)
        assert_row(1, 22, 222, 2222);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 33, 333, 3333, 33333)
        assert_row(1, 33, 333, 3333);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 44, 444, 4444, 44444)
        assert_row(1, 44, 444, 4444);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(2, 11, 111, 1111, 11111)
        assert_row(2, 11, 111, 1111);
        // DELETE val FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(1, 11, 111, 1111);
        // DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(1, 11, 111, 1111);
        // First row for DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444
        assert_row(1, 11, 222);
        // Second row for DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444
        assert_row(1, 11, 444);
        // UPDATE ks.tbl SET val = 555 WHERE pk = 2 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(2, 11, 111, 1111);
        // DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11
        assert_row(1, 11);
        BOOST_REQUIRE(actual_i == actual_end);
    }).get();
}
