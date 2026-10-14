/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include <iterator>
#include <stdint.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/test_utils.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "utils/error_injection.hh"

using namespace std::chrono_literals;

BOOST_AUTO_TEST_SUITE(query_processor_test)

SEASTAR_TEST_CASE(test_execute_internal_insert) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        return e.execute_cql("create table ks.cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").then([](auto rs) {
            BOOST_REQUIRE(dynamic_pointer_cast<cql_transport::messages::result_message::schema_change>(rs));
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key1"), 1, 100 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = 1;", { sstring("key1") }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                auto i = rs->one().template get_as<int32_t>("r1");
                BOOST_CHECK_EQUAL(i, int32_t(100));
            });
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key2"), 2, 200 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf;", cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                BOOST_CHECK_EQUAL(rs->size(), 2);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_execute_internal_delete) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        return e.execute_cql("create table ks.cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").then([](auto rs) {
            BOOST_REQUIRE(dynamic_pointer_cast<cql_transport::messages::result_message::schema_change>(rs));
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key1"), 1, 100 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("delete from ks.cf where p1 = ? and c1 = ?;", { sstring("key1"), 1 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf;", cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        });
    });
}

SEASTAR_TEST_CASE(test_execute_internal_update) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        return e.execute_cql("create table ks.cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").then([](auto rs) {
            BOOST_REQUIRE(dynamic_pointer_cast<cql_transport::messages::result_message::schema_change>(rs));
        }).then([&qp] {
            return qp.execute_internal("insert into ks.cf (p1, c1, r1) values (?, ?, ?);", { sstring("key1"), 1, 100 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = 1;", { sstring("key1") }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                auto i = rs->one().template get_as<int32_t>("r1");
                BOOST_CHECK_EQUAL(i, int32_t(100));
            });
        }).then([&qp] {
            return qp.execute_internal("update ks.cf set r1 = ? where p1 = ? and c1 = ?;", { 200, sstring("key1"), 1 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(rs->empty());
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = 1;", { sstring("key1") }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
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

        e.local_qp().query_internal("SELECT * from ks.cf", [&counter] (const cql3::untyped_result_set::row& row) mutable {
            counter++;
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).get();

        BOOST_CHECK_EQUAL(counter, 0);

        for (auto i = 0; i < 900; i++) {
            total += i;
            e.local_qp().execute_internal("insert into ks.cf (k , v) values (?, ? );", { to_sstring(i), i}, cql3::query_processor::cache_internal::yes).get();
        }
        e.local_qp().query_internal("SELECT * from ks.cf", [&counter, &sum] (const cql3::untyped_result_set::row& row) mutable {
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
            e.local_qp().execute_internal("insert into ks.cf (k , v) values (?, ? );", { to_sstring(i), i}, cql3::query_processor::cache_internal::yes).get();
        }
        e.local_qp().query_internal("SELECT * from ks.cf", [&counter, &sum] (const cql3::untyped_result_set::row& row) mutable {
            counter++;
            sum += row.get_as<int>("v");
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }).get();
        BOOST_CHECK_EQUAL(counter, 2200);
        BOOST_CHECK_EQUAL(total, sum);
        counter = 1000;
        e.local_qp().query_internal("SELECT * from ks.cf", [&counter] (const cql3::untyped_result_set::row& row) mutable {
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
        const auto found = metric.labels().find("consistency_level");
        BOOST_REQUIRE(found != metric.labels().cend());
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
    return std::make_unique<cql3::query_options>(cl, std::vector<cql3::raw_value>());
}

} // anonymous namespace

// query_internal() with a deadline reads all pages before the deadline and
// fails with a timeout, reading nothing, once the deadline has passed.
SEASTAR_TEST_CASE(test_query_internal_with_deadline) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
        for (auto i = 0; i < 10; i++) {
            e.local_qp().execute_internal("insert into ks.cf (k , v) values (?, ? );", { to_sstring(i), i}, cql3::query_processor::cache_internal::yes).get();
        }
        int counter = 0;
        auto count_rows = [&counter] (const cql3::untyped_result_set::row&) {
            counter++;
            return make_ready_future<stop_iteration>(stop_iteration::no);
        };
        // Four pages, each fetched with the time left until the deadline.
        e.local_qp().query_internal("SELECT * from ks.cf", db::consistency_level::ONE, {}, 3,
                db::timeout_clock::now() + std::chrono::minutes(1), count_rows).get();
        BOOST_CHECK_EQUAL(counter, 10);

        counter = 0;
        try {
            e.local_qp().query_internal("SELECT * from ks.cf", db::consistency_level::ONE, {}, 3,
                    db::timeout_clock::now(), count_rows).get();
            BOOST_FAIL("query_internal() did not fail past its deadline");
        } catch (const exceptions::read_timeout_exception&) {
        } catch (const semaphore_timed_out&) {
        }
        BOOST_CHECK_EQUAL(counter, 0);
    });
}

SEASTAR_TEST_CASE(test_query_counters) {
    cql_test_config cfg;
    cfg.need_remote_proxy = true;
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
        auto process_batch = [&e](const std::vector<std::string_view>& queries, clevel cl) mutable {
            e.execute_batch(queries, cql3::statements::batch_statement::type::UNLOGGED, make_options(cl)).get();
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
    }, std::move(cfg));
}

SEASTAR_TEST_CASE(test_select_full_scan_metrics) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& qp = e.local_qp();
        cquery_nofail(e, "create table ks.fsm (pk int, ck int, c1 int, c2 int, PRIMARY KEY(pk, ck));");
        cquery_nofail(e, "create index on ks.fsm (c1);");
        qp.execute_internal("insert into ks.fsm (pk, ck, c1, c2) values (?,?,?,?);", { 1, 1, 1, 1 }, cql3::query_processor::cache_internal::yes).get();
        auto stat_bc1 = qp.get_cql_stats().select_bypass_caches;
        qp.execute_internal("select * from ks.fsm bypass cache;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_bc1 + 1, qp.get_cql_stats().select_bypass_caches);

        auto process_prepared = [&e](const sstring& query, clevel cl) mutable {
            e.prepare(query).then([&e, cl](const auto& id) {
                return e.execute_prepared(id, {}, cl);})
            .get();
        };
        auto stat_bc2 = qp.get_cql_stats().select_bypass_caches;
        process_prepared("select * from ks.fsm bypass cache", clevel::ALL);
        BOOST_CHECK_EQUAL(stat_bc2 + 1, qp.get_cql_stats().select_bypass_caches);

        auto stat_ac1 = qp.get_cql_stats().select_allow_filtering;
        qp.execute_internal("select * from ks.fsm where c2 = 1 allow filtering;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ac1 + 1, qp.get_cql_stats().select_allow_filtering);

        // Unrestricted PK, full scan without BYPASS CACHE
        auto stat_ps1 = qp.get_cql_stats().select_partition_range_scan;
        auto stat_psnb1 = qp.get_cql_stats().select_partition_range_scan_no_bypass_cache;
        qp.execute_internal("select * from ks.fsm;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps1 + 1, qp.get_cql_stats().select_partition_range_scan);
        BOOST_CHECK_EQUAL(stat_psnb1 + 1, qp.get_cql_stats().select_partition_range_scan_no_bypass_cache);

        // Unrestricted PK, full scan with   BYPASS CACHE
        auto stat_psnb2 = qp.get_cql_stats().select_partition_range_scan_no_bypass_cache;
        qp.execute_internal("select * from ks.fsm BYPASS CACHE;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_psnb2, qp.get_cql_stats().select_partition_range_scan_no_bypass_cache);

        // Restricted PK, no full scan
        auto stat_ps2 = qp.get_cql_stats().select_partition_range_scan;
        qp.execute_internal("select * from ks.fsm where pk = 1;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps2, qp.get_cql_stats().select_partition_range_scan);

        // Indexed on c1, no full scan
        auto stat_ps3 = qp.get_cql_stats().select_partition_range_scan;
        qp.execute_internal("select * from ks.fsm where c1 = 1;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps3, qp.get_cql_stats().select_partition_range_scan);

        // Filtered by ck but not filtered by pk
        auto stat_ps4 = qp.get_cql_stats().select_partition_range_scan;
        qp.execute_internal("select * from ks.fsm where ck = 1 allow filtering;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps4 + 1, qp.get_cql_stats().select_partition_range_scan);

        // Filtered by unindexed non-cluster column
        auto stat_ps5 = qp.get_cql_stats().select_partition_range_scan;
        qp.execute_internal("select * from ks.fsm where c2 = 1 allow filtering;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps5 + 1, qp.get_cql_stats().select_partition_range_scan);

        // System table full scan, not measured
        auto stat_ps6 = qp.get_cql_stats().select_partition_range_scan;
        qp.execute_internal("select * from system.views_builds_in_progress;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps6, qp.get_cql_stats().select_partition_range_scan);

        // Range token on PK, full scan
        auto stat_ps7 = qp.get_cql_stats().select_partition_range_scan;
        qp.execute_internal("select * from ks.fsm where token(pk) > 100;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps7 + 1, qp.get_cql_stats().select_partition_range_scan);

        // Token on PK equals, no full scan
        auto stat_ps8 = qp.get_cql_stats().select_partition_range_scan;
        qp.execute_internal("select * from ks.fsm where token(pk) = 1;", cql3::query_processor::cache_internal::yes).get();
        BOOST_CHECK_EQUAL(stat_ps8, qp.get_cql_stats().select_partition_range_scan);
    });
}

// Preparing a batch yields between its sub-statements, so a batch of slow
// sub-statements must not stall the reactor (SCYLLADB-219). The injection
// makes every sub-statement prepare burn 50 ms of CPU. In one task the 20 of
// them would be a 1 s stall, against a 100 ms threshold.
SEASTAR_TEST_CASE(test_batch_prepare_does_not_stall) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        auto& qp = e.local_qp();
        auto& injector = utils::get_local_injector();

        constexpr size_t n = 20;
        std::string text = "BEGIN UNLOGGED BATCH ";
        for (size_t i = 0; i < n; ++i) {
            text += fmt::format("INSERT INTO ks.t (pk, v) VALUES ({}, {}); ", i, i);
        }
        text += "APPLY BATCH";
        auto parsed = cql3::query_processor::parse_statement(text, cql3::internal_dialect());

        std::atomic<unsigned> stalls = 0;
        auto old_threshold = engine().get_blocked_reactor_notify_ms();
        auto old_report = reactor::test::get_stall_detector_report_function();
        auto restore = defer([&] () noexcept {
            injector.disable("batch_statement_prepare_spin");
            reactor::test::set_stall_detector_report_function(std::move(old_report));
            engine().update_blocked_reactor_notify_ms(old_threshold);
        });
        injector.enable("batch_statement_prepare_spin");
        reactor::test::set_stall_detector_report_function([&] { ++stalls; });
        engine().update_blocked_reactor_notify_ms(100ms);
        // The detector arms the new threshold when the next batch of tasks
        // starts. Sleeping, unlike yielding, ends the current batch for sure.
        seastar::sleep(10ms).get();

        auto prepared = parsed->prepare(qp.db(), qp.get_cql_stats(), qp.get_cql_config()).get();

        BOOST_REQUIRE_EQUAL(stalls, 0);
        auto& batch = dynamic_cast<cql3::statements::batch_statement&>(*prepared->statement);
        BOOST_REQUIRE_EQUAL(batch.get_statements().size(), n);
    });
}

static void wait_for_injection_enter(std::string_view name) {
    while (utils::get_local_injector().enter_count(name) == 0) {
        seastar::sleep(1ms).get();
    }
}

// The prepared statements cache drops every load in progress when a schema
// change invalidates any of its entries. So a batch PREPARE suspended across a
// change to another table is answered but not cached, and the first EXECUTE
// finds the id unprepared and the driver prepares again.
SEASTAR_TEST_CASE(test_schema_change_during_batch_prepare_is_not_cached) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        e.execute_cql("CREATE TABLE ks.other (pk int PRIMARY KEY, v int)").get();
        auto& qp = e.local_qp();
        auto& injector = utils::get_local_injector();

        auto id = e.prepare("BEGIN UNLOGGED BATCH INSERT INTO ks.t (pk, v) VALUES (1, 1); INSERT INTO ks.t (pk, v) VALUES (2, 2); APPLY BATCH").get();
        BOOST_REQUIRE(qp.get_prepared(id));

        injector.enable("batch_statement_prepare_yield", true);
        auto prepare = e.prepare("BEGIN UNLOGGED BATCH INSERT INTO ks.t (pk, v) VALUES (3, 3); INSERT INTO ks.t (pk, v) VALUES (4, 4); APPLY BATCH");
        wait_for_injection_enter("batch_statement_prepare_yield");
        e.execute_cql("ALTER TABLE ks.other ADD c int").get();
        injector.receive_message("batch_statement_prepare_yield");
        auto id2 = prepare.get();

        BOOST_REQUIRE(qp.get_prepared(id));
        BOOST_REQUIRE(!qp.get_prepared(id2));
    });
}

// A change to a batch's table while its prepare is suspended would leave the
// sub-statements prepared against different versions of the table, so the
// batch must fail instead of running, plain or conditional.
SEASTAR_TEST_CASE(test_schema_change_during_batch_prepare_fails) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();
        auto& injector = utils::get_local_injector();

        int column = 0;
        for (auto batch : {
                "BEGIN UNLOGGED BATCH INSERT INTO ks.t (pk, ck, v) VALUES (0, 1, 1); INSERT INTO ks.t (pk, ck, v) VALUES (0, 2, 2); APPLY BATCH",
                "BEGIN BATCH INSERT INTO ks.t (pk, ck, v) VALUES (0, 1, 1) IF NOT EXISTS; INSERT INTO ks.t (pk, ck, v) VALUES (0, 2, 2) IF NOT EXISTS; APPLY BATCH"}) {
            injector.enable("batch_statement_prepare_yield", true);
            auto query = e.execute_cql(batch);
            wait_for_injection_enter("batch_statement_prepare_yield");
            e.execute_cql(fmt::format("ALTER TABLE ks.t ADD c{} int", column++)).get();
            injector.receive_message("batch_statement_prepare_yield");
            BOOST_REQUIRE_THROW(query.get(), exceptions::invalid_request_exception);
        }
        assert_that(e.execute_cql("SELECT * FROM ks.t").get()).is_rows().is_empty();
    });
}

// Two callers can prepare the same internal statement at once when its
// prepare() suspends. The second must not replace the first one's entry,
// whose weak pointer was already handed out.
SEASTAR_TEST_CASE(test_concurrent_prepare_internal_share_one_statement) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        auto& qp = e.local_qp();
        auto& injector = utils::get_local_injector();
        const sstring text = "BEGIN UNLOGGED BATCH INSERT INTO ks.t (pk, v) VALUES (?, ?); INSERT INTO ks.t (pk, v) VALUES (?, ?); APPLY BATCH";

        injector.enable("batch_statement_prepare_yield", true);
        auto first = qp.prepare_internal(text);
        wait_for_injection_enter("batch_statement_prepare_yield");
        auto second = qp.prepare_internal(text).get();
        injector.receive_message("batch_statement_prepare_yield");
        auto first_ptr = first.get();

        BOOST_REQUIRE_EQUAL(&*first_ptr, &*second);
        qp.execute_internal(text, {1, 1, 2, 2}, cql3::query_processor::cache_internal::yes).get();
        assert_that(e.execute_cql("SELECT pk FROM ks.t").get()).is_rows().with_size(2);
    });
}

// A statement of 16 KiB or more is parsed in a thread that yields between
// tokens (SCYLLADB-219). The injection makes every token burn 10 ms of CPU,
// and the statement has a few dozen of them, so in one task the parse would
// stall for hundreds of ms, against a 100 ms threshold.
SEASTAR_TEST_CASE(test_large_statement_parse_does_not_stall) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#if !defined(SCYLLA_ENABLE_ERROR_INJECTION) || defined(DEBUG)
        fmt::print("Skipping test as it depends on error injection and on a parser that yields, which debug builds lack. Please run in dev mode.\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v text)").get();
        auto& injector = utils::get_local_injector();
        const sstring value(20 * 1024, 'x');

        std::atomic<unsigned> stalls = 0;
        auto old_threshold = engine().get_blocked_reactor_notify_ms();
        auto old_report = reactor::test::get_stall_detector_report_function();
        auto restore = defer([&] () noexcept {
            injector.disable("parse_statement_in_thread_spin");
            reactor::test::set_stall_detector_report_function(std::move(old_report));
            engine().update_blocked_reactor_notify_ms(old_threshold);
        });
        injector.enable("parse_statement_in_thread_spin");
        reactor::test::set_stall_detector_report_function([&] { ++stalls; });
        engine().update_blocked_reactor_notify_ms(100ms);
        // See test_batch_prepare_does_not_stall.
        seastar::sleep(10ms).get();

        e.execute_cql(fmt::format("INSERT INTO ks.t (pk, v) VALUES (1, '{}')", value)).get();

        BOOST_REQUIRE_EQUAL(stalls, 0);
        BOOST_REQUIRE_GT(injector.enter_count("parse_statement_in_thread_spin"), 0);
        assert_that(e.execute_cql("SELECT v FROM ks.t WHERE pk = 1").get()).is_rows().with_rows({{utf8_type->decompose(value)}});
    });
}

BOOST_AUTO_TEST_SUITE_END()
