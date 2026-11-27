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
#include "test/lib/error_injection.hh"
#include "test/lib/test_utils.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/defer.hh>
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "replica/database.hh"
#include "utils/error_injection.hh"

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

template <typename T>
static size_t count_yields(const future<T>& f, std::string_view injection_name) {
    auto& injector = utils::get_local_injector();
    size_t yields = 0;
    auto seen = injector.enter_count(injection_name);
    while (!f.available()) {
        seastar::thread::yield();
        auto count = injector.enter_count(injection_name);
        yields += count != seen;
        seen = count;
    }
    return yields;
}

// Preparing a batch yields between its sub-statements, so a batch of slow
// sub-statements does not stall the reactor (SCYLLADB-219). The injection
// makes every sub-statement burn 1 ms of CPU, more than the task quota, so
// the prepare yields after each of the 8.
SEASTAR_TEST_CASE(test_batch_prepare_does_not_stall) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        auto& qp = e.local_qp();
        auto& injector = utils::get_local_injector();

        // A batch of n INSERTs.
        constexpr size_t n = 8;
        std::string text = "BEGIN UNLOGGED BATCH ";
        for (size_t i = 0; i < n; ++i) {
            text += fmt::format("INSERT INTO ks.t (pk, v) VALUES ({}, {}); ", i, i);
        }
        text += "APPLY BATCH";
        auto parsed = cql3::query_processor::parse_statement(text, cql3::internal_dialect());

        // Prepare it with the 1 ms spin, and count the yields.
        auto disable = defer([&] () noexcept { injector.disable("delay_after_batch_sub_statement"); });
        injector.enable("delay_after_batch_sub_statement", false, {{"spin_us", "1000"}});
        auto prepare = parsed->prepare(qp.db(), qp.get_cql_stats(), qp.get_cql_config());
        auto yields = count_yields(prepare, "delay_after_batch_sub_statement");
        auto prepared = prepare.get().prepared;

        // It yielded after most sub-statements, and prepared all of them.
        BOOST_REQUIRE_GE(yields, n / 2);
        auto& batch = dynamic_cast<cql3::statements::batch_statement&>(*prepared->statement);
        BOOST_REQUIRE_EQUAL(batch.get_statements().size(), n);
    });
}

// A schema change waits while a batch is prepared, so the sub-statements of
// the batch all use one version of their table. The batch then runs, plain or
// conditional, and the schema change is applied after its prepare. A table
// dropped and created again is another table under the same name, so a DROP
// TABLE waits too.
SEASTAR_TEST_CASE(test_schema_change_waits_for_batch_prepare) {
    // A conditional batch runs Paxos, as in paxos_state_test.
    cql_test_config cfg;
    cfg.initial_tablets = 1;
    cfg.need_remote_proxy = true;
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();
        auto& qp = e.local_qp();
        auto& injector = utils::get_local_injector();
        auto disable = defer([&] () noexcept { injector.disable("delay_after_batch_sub_statement"); });
        const char* plain = "BEGIN UNLOGGED BATCH INSERT INTO ks.t (pk, ck, v) VALUES (0, 1, 1); INSERT INTO ks.t (pk, ck, v) VALUES (0, 2, 2); APPLY BATCH";
        const char* conditional = "BEGIN BATCH INSERT INTO ks.t (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS; INSERT INTO ks.t (pk, ck, v) VALUES (1, 2, 2) IF NOT EXISTS; APPLY BATCH";

        int column = 0;
        for (auto batch : {plain, conditional}) {
            // Pause the prepare after the first sub-statement.
            injector.enable("delay_after_batch_sub_statement", true, {{"pause", "1"}});
            auto query = e.execute_cql(batch);
            wait_for_injection_enter("delay_after_batch_sub_statement").get();

            // An ALTER waits for the paused prepare.
            const auto version = e.data_dictionary().find_schema("ks", "t")->version();
            auto alter = e.execute_cql(fmt::format("ALTER TABLE ks.t ADD c{} int", column++));
            e.wait_until_schema_change_waits();
            BOOST_REQUIRE(!alter.available());
            BOOST_REQUIRE(e.data_dictionary().find_schema("ks", "t")->version() == version);

            // The batch runs, then the ALTER is applied.
            injector.receive_message("delay_after_batch_sub_statement");
            query.get();
            alter.get();
            BOOST_REQUIRE(e.data_dictionary().find_schema("ks", "t")->version() != version);
        }

        // Both batches wrote their rows.
        assert_that(e.execute_cql("SELECT pk, ck FROM ks.t").get()).is_rows().with_size(4);

        // This batch is only prepared, so the test can check the table of each
        // sub-statement.
        const auto old_id = e.data_dictionary().find_schema("ks", "t")->id();
        auto parsed = cql3::query_processor::parse_statement(conditional, cql3::internal_dialect());
        injector.enable("delay_after_batch_sub_statement", true, {{"pause", "1"}});
        auto prepare = parsed->prepare(qp.db(), qp.get_cql_stats(), qp.get_cql_config());
        wait_for_injection_enter("delay_after_batch_sub_statement").get();

        // A DROP TABLE waits for the paused prepare.
        auto drop = e.execute_cql("DROP TABLE ks.t");
        e.wait_until_schema_change_waits();
        BOOST_REQUIRE(!drop.available());

        // The prepare ends, then the DROP. The table created again has a new id.
        injector.receive_message("delay_after_batch_sub_statement");
        auto prepared = prepare.get().prepared;
        drop.get();
        e.execute_cql("CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();
        BOOST_REQUIRE(e.data_dictionary().find_schema("ks", "t")->id() != old_id);

        // Both sub-statements use the table from before the DROP.
        const auto& statements = dynamic_cast<cql3::statements::batch_statement&>(*prepared->statement).get_statements();
        BOOST_REQUIRE_EQUAL(statements.size(), 2);
        BOOST_REQUIRE(statements[0].statement->s->id() == old_id);
        BOOST_REQUIRE(statements[1].statement->s->id() == old_id);
    }, std::move(cfg));
}

// A USE on the connection can run while the prepare of a batch is suspended.
// The tables of the batch must resolve against the keyspace from before the
// USE.
SEASTAR_TEST_CASE(test_use_during_batch_prepare) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        e.execute_cql("CREATE KEYSPACE ks2 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks2.t (pk int PRIMARY KEY, v int)").get();
        auto& injector = utils::get_local_injector();

        // Pause the prepare of a batch sent while the connection uses ks.
        auto disable = defer([&] () noexcept { injector.disable("delay_after_batch_sub_statement"); });
        injector.enable("delay_after_batch_sub_statement", true, {{"pause", "1"}});
        auto query = e.execute_cql("BEGIN UNLOGGED BATCH INSERT INTO t (pk, v) VALUES (1, 1); INSERT INTO t (pk, v) VALUES (2, 2); APPLY BATCH");
        wait_for_injection_enter("delay_after_batch_sub_statement").get();

        // The USE runs during the pause.
        e.execute_cql("USE ks2").get();
        injector.receive_message("delay_after_batch_sub_statement");
        query.get();

        // The batch wrote to ks.
        assert_that(e.execute_cql("SELECT pk FROM ks.t").get()).is_rows().with_size(2);
        assert_that(e.execute_cql("SELECT pk FROM ks2.t").get()).is_rows().is_empty();
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

        // The first prepare pauses, and a second one of the same text runs
        // meanwhile.
        auto disable = defer([&] () noexcept { injector.disable("delay_after_batch_sub_statement"); });
        injector.enable("delay_after_batch_sub_statement", true, {{"pause", "1"}});
        auto first = qp.prepare_internal(text);
        wait_for_injection_enter("delay_after_batch_sub_statement").get();
        auto second = qp.prepare_internal(text).get();
        injector.receive_message("delay_after_batch_sub_statement");
        auto first_ptr = first.get();

        // Both callers got the same statement, and it runs.
        BOOST_REQUIRE_EQUAL(&*first_ptr, &*second);
        qp.execute_internal(text, {1, 1, 2, 2}, cql3::query_processor::cache_internal::yes).get();
        assert_that(e.execute_cql("SELECT pk FROM ks.t").get()).is_rows().with_size(2);
    });
}

// A BATCH message that finds a prepared statement older than its table removes
// it from the cache, and the driver prepares it again (SCYLLADB-219).
SEASTAR_TEST_CASE(test_remove_prepared) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        auto& qp = e.local_qp();
        const sstring text = "INSERT INTO ks.t (pk, v) VALUES (?, ?)";
        auto id = e.prepare(text).get();
        auto prepared = qp.get_prepared(id);
        BOOST_REQUIRE(prepared);

        qp.remove_prepared(std::nullopt, id, *prepared);
        BOOST_REQUIRE(!qp.get_prepared(id));

        BOOST_REQUIRE(e.prepare(text).get() == id);
        BOOST_REQUIRE(qp.get_prepared(id));
    });
}

// A PREPARE response that is still being sent keeps a stale statement alive,
// and the user's authorized cache returns it even after the statement is
// prepared again. Removing the stale statement drops that entry and keeps the
// newer statement (SCYLLADB-219).
SEASTAR_TEST_CASE(test_remove_prepared_keeps_newer_statement) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)").get();
        auto& qp = e.local_qp();
        const auto& user = e.local_client_state().user();
        const sstring text = "INSERT INTO ks.t (pk, v) VALUES (1, 1)";

        // An EXECUTE adds the statement to the user's authorized cache, and a
        // PREPARE response that is not sent yet keeps it alive.
        auto id = e.prepare(text).get();
        e.execute_prepared(id, {}).get();
        auto unsent_response = qp.prepare(text, e.local_client_state(), e.test_dialect()).get();
        auto stale = qp.get_prepared(id);
        BOOST_REQUIRE(stale);

        // A schema change drops the statement from the cache, and the driver
        // prepares it again.
        e.execute_cql("ALTER TABLE ks.t ADD w int").get();
        BOOST_REQUIRE(e.prepare(text).get() == id);
        auto newer = qp.get_prepared(id);
        BOOST_REQUIRE(newer && newer.get() != stale.get());

        // The user's authorized entry still finds the stale statement, so a
        // BATCH message finds it and removes it.
        BOOST_REQUIRE(qp.get_prepared(user, id).get() == stale.get());

        // Removing the stale statement drops the user's entry, and the newer
        // statement stays.
        qp.remove_prepared(user, id, *stale);
        BOOST_REQUIRE(!qp.get_prepared(user, id));
        BOOST_REQUIRE(qp.get_prepared(id).get() == newer.get());
    });
}

// A statement of 16 KiB or more is parsed in a thread that yields between
// tokens (SCYLLADB-219). The injection makes every token burn 1 ms of CPU,
// more than the task quota, so the parse yields after each.
SEASTAR_TEST_CASE(test_large_statement_parse_does_not_stall) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v text)").get();
        auto& injector = utils::get_local_injector();
        const sstring value(20 * 1024, 'x');

        // Run a large INSERT with the 1 ms spin, and count the yields.
        auto disable = defer([&] () noexcept { injector.disable("delay_thread_parse_token"); });
        injector.enable("delay_thread_parse_token", false, {{"spin_us", "1000"}});
        auto insert = e.execute_cql(fmt::format("INSERT INTO ks.t (pk, v) VALUES (1, '{}')", value));
        auto yields = count_yields(insert, "delay_thread_parse_token");
        insert.get();

        // The parse yielded after most tokens, and the row is there.
        BOOST_REQUIRE_GT(yields, 1);
        BOOST_REQUIRE_GE(yields, injector.enter_count("delay_thread_parse_token") / 2);
        assert_that(e.execute_cql("SELECT v FROM ks.t WHERE pk = 1").get()).is_rows().with_rows({{utf8_type->decompose(value)}});
    });
}

// A USE on the connection can run while a large statement is parsed in a
// thread. The statement must resolve against the keyspace from before, as a
// QUERY and as a PREPARE.
SEASTAR_TEST_CASE(test_use_during_large_statement_parse) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v text)").get();
        e.execute_cql("CREATE KEYSPACE ks2 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}").get();
        e.execute_cql("CREATE TABLE ks2.t (pk int PRIMARY KEY, v text)").get();
        auto& injector = utils::get_local_injector();
        const sstring value(20 * 1024, 'x');
        auto disable = defer([&] () noexcept { injector.disable("delay_thread_parse_token"); });

        // Starts a statement, and runs USE ks2 while its parse is paused.
        auto use_during = [&] (auto start) {
            injector.enable("delay_thread_parse_token", true, {{"pause", "1"}});
            auto f = start();
            wait_for_injection_enter("delay_thread_parse_token").get();
            e.execute_cql("USE ks2").get();
            injector.receive_message("delay_thread_parse_token");
            return f.get();
        };

        // As a QUERY.
        use_during([&] { return e.execute_cql(fmt::format("INSERT INTO t (pk, v) VALUES (1, '{}')", value)); });

        // As a PREPARE. The USE ks2 above stays in effect, so go back to ks first.
        e.execute_cql("USE ks").get();
        auto id = use_during([&] { return e.prepare(fmt::format("INSERT INTO t (pk, v) VALUES (2, '{}')", value)); });
        e.execute_prepared(id, {}).get();

        // Both rows went to ks.
        assert_that(e.execute_cql("SELECT pk FROM ks.t").get()).is_rows().with_rows_ignore_order({{int32_type->decompose(1)}, {int32_type->decompose(2)}});
        assert_that(e.execute_cql("SELECT pk FROM ks2.t").get()).is_rows().is_empty();
    });
}

// The CPU time the scheduling group has used on this shard, in ms.
static uint64_t scheduler_runtime_ms(const sstring& group) {
    auto all_metrics = seastar::metrics::impl::get_values();
    const auto& all_metadata = *all_metrics->metadata;
    for (size_t i = 0; i < all_metadata.size(); ++i) {
        if (all_metadata[i].mf.name != "scheduler_runtime_ms") {
            continue;
        }
        for (size_t j = 0; j < all_metadata[i].metrics.size(); ++j) {
            const auto& labels = all_metadata[i].metrics[j].labels();
            auto found = labels.find("group");
            if (found != labels.end() && found->second.value() == group) {
                return all_metrics->values[i][j].ui();
            }
        }
    }
    BOOST_FAIL(fmt::format("no scheduler_runtime_ms for group {}", group));
    return 0;
}

// The thread that parses a large statement must run in the scheduling group
// of the request that sent the statement.
SEASTAR_TEST_CASE(test_large_statement_parse_runs_in_callers_scheduling_group) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v text)").get();
        const sstring value(20 * 1024, 'x');

        // A scheduling group of its own, and a 1 ms spin on every token.
        auto sg = create_scheduling_group("large_parse", 100).get();
        auto destroy = defer([&] () noexcept { destroy_scheduling_group(sg).get(); });
        auto& injector = utils::get_local_injector();
        auto disable = defer([&] () noexcept { injector.disable("delay_thread_parse_token"); });
        injector.enable("delay_thread_parse_token", false, {{"spin_us", "1000"}});

        // Run a large INSERT in that group.
        auto runtime_before = scheduler_runtime_ms("large_parse");
        with_scheduling_group(sg, [&] {
            return e.execute_cql(fmt::format("INSERT INTO ks.t (pk, v) VALUES (1, '{}')", value)).discard_result();
        }).get();

        // The group used at least half of the CPU the injection spun for, 1 ms
        // on each entry.
        auto spin_ms = injector.enter_count("delay_thread_parse_token");
        BOOST_REQUIRE_GT(spin_ms, 0);
        BOOST_REQUIRE_GE(scheduler_runtime_ms("large_parse") - runtime_before, spin_ms / 2);
    });
}

// Large statements are parsed one at a time in a scheduling group, so the
// second one of a group starts when the first is parsed. A large statement of
// another group does not wait for them.
SEASTAR_TEST_CASE(test_large_statements_are_parsed_one_at_a_time_in_a_scheduling_group) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v text)").get();
        const sstring padding(16 * 1024, 'x');
        // Each PREPARE has a text of its own, so that it is not found cached.
        auto slow = [&] (int i) {
            return fmt::format("SELECT * FROM ks.t WHERE pk IN ({}) /* {} {} */", fmt::join(std::vector<int>(20, 1), ", "), i, padding);
        };
        auto quick = [&] (int i) {
            return fmt::format("SELECT * FROM ks.t /* {} {} */", i, padding);
        };

        // Two scheduling groups, and a way to prepare a statement in one.
        auto sg1 = create_scheduling_group("large_parse_1", 100).get();
        auto destroy1 = defer([&] () noexcept { destroy_scheduling_group(sg1).get(); });
        auto sg2 = create_scheduling_group("large_parse_2", 100).get();
        auto destroy2 = defer([&] () noexcept { destroy_scheduling_group(sg2).get(); });
        auto& injector = utils::get_local_injector();
        auto disable = defer([&] () noexcept { injector.disable("delay_thread_parse_token"); });
        auto prepare_in = [&] (scheduling_group sg, sstring text) {
            return with_scheduling_group(sg, [&e, text = std::move(text)] {
                return e.prepare(text).discard_result();
            });
        };

        // In one group, the quick statement waits until the slow one is parsed,
        // so no token is parsed after it. The injection makes every token burn
        // 0.5 ms of CPU, and the slow statement has several times the tokens of
        // the quick one.
        injector.enable("delay_thread_parse_token", false, {{"spin_us", "500"}});
        auto slow_statement = prepare_in(sg1, slow(1));
        wait_for_injection_enter("delay_thread_parse_token").get();
        prepare_in(sg1, quick(1)).get();

        // No token of the slow statement was parsed after the quick one.
        auto entries_after_quick = injector.enter_count("delay_thread_parse_token");
        slow_statement.get();
        BOOST_REQUIRE_EQUAL(injector.enter_count("delay_thread_parse_token"), entries_after_quick);
        injector.disable("delay_thread_parse_token");

        // In another group, the quick statement is parsed while the slow one's
        // parse is paused.
        injector.enable("delay_thread_parse_token", true, {{"pause", "1"}});
        slow_statement = prepare_in(sg1, slow(2));
        wait_for_injection_enter("delay_thread_parse_token").get();
        with_timeout(lowres_clock::now() + std::chrono::seconds(30), prepare_in(sg2, quick(2))).get();
        injector.receive_message("delay_thread_parse_token");
        slow_statement.get();
    });
}

// A change to the schema of any table makes the prepared statements cache
// drop every load in progress. So a PREPARE whose statement is parsed in a
// thread across such a change is answered but not cached. The first EXECUTE
// finds the id unprepared, and the driver prepares again.
SEASTAR_TEST_CASE(test_schema_change_during_large_statement_parse_is_not_cached) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        e.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY, v text)").get();
        e.execute_cql("CREATE TABLE ks.other (pk int PRIMARY KEY, v int)").get();
        auto& qp = e.local_qp();
        auto& injector = utils::get_local_injector();
        const sstring value(20 * 1024, 'x');

        // The parse of a PREPARE pauses, and another table changes meanwhile.
        auto disable = defer([&] () noexcept { injector.disable("delay_thread_parse_token"); });
        injector.enable("delay_thread_parse_token", true, {{"pause", "1"}});
        auto prepare = e.prepare(fmt::format("INSERT INTO ks.t (pk, v) VALUES (1, '{}')", value));
        wait_for_injection_enter("delay_thread_parse_token").get();
        e.execute_cql("ALTER TABLE ks.other ADD c int").get();
        injector.receive_message("delay_thread_parse_token");
        auto id = prepare.get();

        // The PREPARE got its answer, but the statement is not cached.
        BOOST_REQUIRE(!qp.get_prepared(id));
    });
}

BOOST_AUTO_TEST_SUITE_END()
