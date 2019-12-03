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
#include <iterator>
#include <stdint.h>

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/metrics_api.hh>
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/cql_config.hh"

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

namespace {

using clevel = db::consistency_level;
using seastar::metrics::impl::value_vector;

constexpr auto level_count = size_t(clevel::MAX_VALUE) - size_t(clevel::MIN_VALUE) + 1;

/// Retrieves query processor's query metrics as a map from each label to its value.
std::unordered_map<sstring, uint64_t> get_query_metrics() {
    auto all_metrics = seastar::metrics::impl::get_values();
    const auto& all_metadata = *all_metrics->metadata;
    const auto qp_group = find_if(cbegin(all_metadata), cend(all_metadata),
        [](const auto& x) { return x.mf.name == "query_processor_queries"; });
    BOOST_REQUIRE(qp_group != cend(all_metadata));
    const auto values = all_metrics->values[distance(cbegin(all_metadata), qp_group)];
    std::vector<sstring> labels;
    for (const auto& metric : qp_group->metrics) {
        const auto found = metric.id.labels().find("consistency_level");
        BOOST_REQUIRE(found != metric.id.labels().cend());
        labels.push_back(found->second);
    }
    BOOST_REQUIRE(values.size() == level_count);
    BOOST_REQUIRE(labels.size() == level_count);
    std::unordered_map<sstring, uint64_t> label_to_value;
    for (size_t i = 0; i < labels.size(); ++i) {
        label_to_value[labels[i]] = values[i].ui();
    }
    return label_to_value;
}

/// Creates query_options with cl, infinite timeout, and no named values.
auto make_options(clevel cl) {
    return std::make_unique<cql3::query_options>(
        cl, infinite_timeout_config, std::vector<cql3::raw_value>());
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_query_counters) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        // Executes a query and waits for it to complete.
        auto process_query = [&e](const sstring& query, clevel cl) mutable {
            e.execute_cql(query, make_options(cl)).get();
        };

        // Executes a prepared statement and waits for it to complete.
        auto process_prepared = [&e](const sstring& query, clevel cl) mutable {
            e.prepare(query).then([&e, cl](const auto& id) {
                return e.execute_prepared(id, {}, cl);})
            .get();
        };

        // Executes a batch of (modifying) statements and waits for it to complete.
        auto process_batch = [&e](const std::vector<sstring_view>& queries, clevel cl) mutable {
            e.execute_batch(queries, make_options(cl)).get();
        };

        auto expected = get_query_metrics();

        process_query("create table ks.cf (k text, v int, primary key (k))", clevel::ANY);
        ++expected["ANY"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_query("select * from ks.cf", clevel::QUORUM);
        ++expected["QUORUM"];
        process_query("select * from ks.cf", clevel::QUORUM);
        ++expected["QUORUM"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_query("select * from ks.cf", clevel::ONE);
        ++expected["ONE"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_query("select * from ks.cf", clevel::ALL);
        ++expected["ALL"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());
        process_prepared("select * from ks.cf", clevel::ALL);
        ++expected["ALL"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_query("select * from ks.cf", clevel::LOCAL_QUORUM);
        ++expected["LOCAL_QUORUM"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_prepared("insert into ks.cf (k, v) values ('0', 0)", clevel::EACH_QUORUM);
        ++expected["EACH_QUORUM"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_query("select * from ks.cf where k='x'", clevel::SERIAL);
        ++expected["SERIAL"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());
        process_prepared("select * from ks.cf where k='x'", clevel::SERIAL);
        ++expected["SERIAL"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());
        process_query("select * from ks.cf where k='x'", clevel::SERIAL);
        ++expected["SERIAL"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_query("select * from ks.cf where k='x'", clevel::LOCAL_SERIAL);
        ++expected["LOCAL_SERIAL"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_prepared("select * from ks.cf", clevel::LOCAL_ONE);
        ++expected["LOCAL_ONE"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_batch({"insert into ks.cf (k, v) values ('1', 1)"}, clevel::EACH_QUORUM);
        ++expected["EACH_QUORUM"];
        BOOST_CHECK_EQUAL(expected, get_query_metrics());

        process_batch(
            {"insert into ks.cf (k, v) values ('2', 2)", "insert into ks.cf (k, v) values ('3', 3)"},
            clevel::ANY);
        expected["ANY"] += 2;
        BOOST_CHECK_EQUAL(expected, get_query_metrics());
    });
}
