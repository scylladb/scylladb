/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"
#include "tests/test_services.hh"

#include "db/size_estimates_virtual_reader.hh"
#include "core/future-util.hh"
#include "cql3/query_processor.hh"
#include "transport/messages/result_message.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

SEASTAR_TEST_CASE(test_query_virtual_table) {
    return do_with_cql_env([] (auto& e) {
        auto ranges = db::size_estimates::size_estimates_mutation_reader::get_local_ranges().get0();
        auto start_token1 = utf8_type->to_string(ranges[3].start);
        auto start_token2 = utf8_type->to_string(ranges[5].start);
        auto end_token1 = utf8_type->to_string(ranges[3].end);
        auto end_token2 = utf8_type->to_string(ranges[55].end);
        auto &qp = e.local_qp();
        return e.execute_cql("create table cf1(pk text PRIMARY KEY, v int);").discard_result().then([&e] {
            return e.execute_cql("create table cf2(pk text PRIMARY KEY, v int);").discard_result();
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 512);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' limit 100;").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 100);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name = 'cf1';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 256);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name > 'cf1';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 256);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name >= 'cf1';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 512);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name < 'cf2';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 256);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name <= 'cf2';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 512);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name in ('cf1', 'cf2');").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 512);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name >= 'cf1' and table_name <= 'cf1';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 256);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name >= 'cf1' and table_name <= 'cf2';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 512);
            });
        }).then([&qp] {
            return qp.execute_internal("select * from system.size_estimates where keyspace_name = 'ks' "
                                       "and table_name > 'cf1' and table_name < 'cf2';").then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 0);
            });
        }).then([&qp, start_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s';", start_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start >= '%s';", start_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 253);
            });
        }).then([&qp, start_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start > '%s';", start_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 252);
            });
        }).then([&qp, start_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start <= '%s';", start_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 4);
            });
         }).then([&qp, start_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start < '%s';", start_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 3);
            });
          }).then([&qp, start_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start >= '%s' and range_start <= '%s';", start_token1, start_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1, start_token2] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start >= '%s' and range_start <= '%s';", start_token1, start_token2)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 3);
            });
        }).then([&qp, start_token1, start_token2] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start > '%s' and range_start < '%s';", start_token1, start_token2)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1, start_token2] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start in ('%s', '%s');", start_token1, start_token2)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 2);
            });
        }).then([&qp, start_token1, start_token2] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start > '%s' and range_start <= '%s';", start_token1, start_token2)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 2);
            });
        }).then([&qp, start_token1, start_token2] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start >= '%s' and range_start < '%s';", start_token1, start_token2)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 2);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s' and range_end = '%s';", start_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s' and range_end >= '%s';", start_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s' and range_end > '%s';", start_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 0);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s' and range_end <= '%s';", start_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s' and range_end < '%s';", start_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 0);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s' and range_end >= '%s' and range_end <= '%s';", start_token1, end_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and table_name = 'cf1' and range_start = '%s' and range_end > '%s' and range_end < '%s';", start_token1, end_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 0);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and (table_name, range_start, range_end) = ('cf1', '%s', '%s');", start_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 1);
            });
        }).then([&qp, start_token1, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and (table_name, range_start, range_end) >= ('cf1', '%s', '%s') and (table_name) <= ('cf2');", start_token1, end_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 509);
            });
        }).then([&qp, start_token1, start_token2, end_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and (table_name, range_start, range_end) >= ('cf1', '%s', '%s') "
                                              "and (table_name, range_start) <= ('cf2', '%s');", start_token1, end_token1, start_token2)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 259);
            });
        }).then([&qp, start_token1] {
            return qp.execute_internal(sprint("select * from system.size_estimates where keyspace_name = 'ks' "
                                              "and (table_name, range_start) < ('cf2', '%s');", start_token1)).then([](auto rs) {
                BOOST_REQUIRE_EQUAL(rs->size(), 259);
            });
        }).discard_result();
    });
}
