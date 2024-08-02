/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_assertions.hh"
#include <seastar/core/coroutine.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include "utils/UUID_gen.hh"
#include "utils/error_injection.hh"
#include "transport/messages/result_message.hh"
#include "service/migration_manager.hh"
#include <fmt/ranges.h>
#include <seastar/core/metrics_api.hh>

static future<utils::chunked_vector<std::vector<managed_bytes_opt>>> fetch_rows(cql_test_env& e, std::string_view cql) {
    auto msg = co_await e.execute_cql(cql);
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);
    co_return rows->rs().result_set().rows();
}

static future<size_t> get_history_size(cql_test_env& e) {
    co_return (co_await fetch_rows(e, "select * from system.group0_history")).size();
}

SEASTAR_TEST_CASE(test_abort_server_on_background_error) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
    return make_ready_future<>();
#else
    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        utils::get_local_injector().enable("store_log_entries/test-failure", true);

        auto get_metric_ui64 = [&](sstring name) {
            const auto& value_map = seastar::metrics::impl::get_value_map();
            const auto& metric_family = value_map.at("raft_group0_" + name);
            const auto& registered_metric = metric_family.at({{"shard", "0"}});
            return (*registered_metric)().ui();
        };

        auto get_status = [&] {
            return get_metric_ui64("status");
        };

        auto perform_schema_change = [&, has_ks = false] () mutable -> future<> {
            if (has_ks) {
                co_await e.execute_cql("drop keyspace new_ks");
            } else {
                co_await e.execute_cql("create keyspace new_ks with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}");
            }
            has_ks = !has_ks;
        };

        auto check_error = [](const raft::stopped_error& e) {
            return e.what() == sstring("Raft instance is stopped, reason: \"background error, std::runtime_error (store_log_entries/test-failure)\"");
        };
        BOOST_REQUIRE_EQUAL(get_status(), 1);
        BOOST_CHECK_EXCEPTION(co_await perform_schema_change(), raft::stopped_error, check_error);
        BOOST_REQUIRE_EQUAL(get_status(), 2);
        BOOST_CHECK_EXCEPTION(co_await perform_schema_change(), raft::stopped_error, check_error);
        BOOST_REQUIRE_EQUAL(get_status(), 2);
        BOOST_CHECK_EXCEPTION(co_await perform_schema_change(), raft::stopped_error, check_error);
        BOOST_REQUIRE_EQUAL(get_status(), 2);
    });
#endif
}

SEASTAR_TEST_CASE(test_group0_history_clearing_old_entries) {
    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        using namespace std::chrono;

        auto get_history_size = std::bind_front(::get_history_size, std::ref(e));

        auto perform_schema_change = [&, has_ks = false] () mutable -> future<> {
            if (has_ks) {
                co_await e.execute_cql("drop keyspace new_ks");
            } else {
                co_await e.execute_cql("create keyspace new_ks with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}");
            }
            has_ks = !has_ks;
        };

        auto size = co_await get_history_size();
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), size + 1);

        auto& rclient = e.get_raft_group0_client();
        rclient.set_history_gc_duration(gc_clock::duration{0});

        // When group0_history_gc_duration is 0, any change should clear all previous history entries.
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 1);
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 1);

        rclient.set_history_gc_duration(duration_cast<gc_clock::duration>(weeks{1}));
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 2);

        for (int i = 0; i < 10; ++i) {
            co_await perform_schema_change();
        }

        // Would use a shorter sleep, but gc_clock's resolution is one second.
        auto sleep_dur = seconds{1};
        co_await sleep(sleep_dur);

        for (int i = 0; i < 10; ++i) {
            co_await perform_schema_change();
        }

        auto get_history_timestamps = [&] () -> future<std::vector<microseconds>> {
            auto rows = co_await fetch_rows(e, "select state_id from system.group0_history");
            std::vector<microseconds> result;
            for (auto& row: rows) {
                auto state_id = value_cast<utils::UUID>(timeuuid_type->deserialize(*row[0]));
                result.push_back(utils::UUID_gen::unix_timestamp_micros(state_id));
            }
            co_return result;
        };

        auto timestamps1 = co_await get_history_timestamps();
        rclient.set_history_gc_duration(duration_cast<gc_clock::duration>(sleep_dur));
        co_await perform_schema_change();
        auto timestamps2 = co_await get_history_timestamps();
        // State IDs are sorted in descending order in the history table.
        // The first entry corresponds to the last schema change.
        auto last_ts = timestamps2.front();

        testlog.info("timestamps1: {}", timestamps1);
        testlog.info("timestamps2: {}", timestamps2);

        // All entries in `timestamps2` except `last_ts` should be present in `timestamps1`.
        BOOST_REQUIRE(std::includes(timestamps1.begin(), timestamps1.end(), timestamps2.begin()+1, timestamps2.end(), std::greater{}));

        // Count the number of timestamps in `timestamps1` that are older than the last entry by `sleep_dur` or more.
        // There should be about 12 because we slept for `sleep_dur` between the two loops above
        // and performing these schema changes should be much faster than `sleep_dur`.
        auto older_by_sleep_dur = std::count_if(timestamps1.begin(), timestamps1.end(), [last_ts, sleep_dur] (microseconds ts) {
            return last_ts - ts > sleep_dur;
        });

        testlog.info("older by sleep_dur: {}", older_by_sleep_dur);

        // That last change should have cleared exactly those older than `sleep_dur` entries.
        // Therefore `timestamps2` should contain all in `timestamps1` minus those changes plus one (`last_ts`).
        BOOST_REQUIRE_EQUAL(timestamps2.size(), timestamps1.size() - older_by_sleep_dur + 1);

    });
}

SEASTAR_TEST_CASE(test_concurrent_group0_modifications) {
    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        auto& rclient = e.get_raft_group0_client();
        auto& mm = e.migration_manager().local();

        // raft_group0_client::_group0_operation_mutex prevents concurrent group 0 changes to be executed on a single node,
        // so in production `group0_concurrent_modification` never occurs if all changes go through a single node.
        // For this test, give it more units so it doesn't block these concurrent executions
        // in order to simulate a scenario where multiple nodes concurrently send schema changes.
        rclient.operation_mutex().signal(1337);

        // Make DDL statement execution fail on the first attempt if it gets a concurrent modification exception.
        mm.set_concurrent_ddl_retries(0);

        auto get_history_size = std::bind_front(::get_history_size, std::ref(e));

        auto perform_schema_changes = [] (cql_test_env& e, size_t n, size_t task_id) -> future<size_t> {
            size_t successes = 0;
            bool has_ks = false;
            auto drop_ks_cql = format("drop keyspace new_ks{}", task_id);
            auto create_ks_cql = format("create keyspace new_ks{} with replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}", task_id);

            auto perform = [&] () -> future<> {
                try {
                    if (has_ks) {
                        co_await e.execute_cql(drop_ks_cql);
                    } else {
                        co_await e.execute_cql(create_ks_cql);
                    }
                    has_ks = !has_ks;
                    ++successes;
                } catch (const service::group0_concurrent_modification&) {}
            };

            while (n--) {
                co_await perform();
            }

            co_return successes;
        };

        auto size = co_await get_history_size();

        size_t N = 4;
        size_t M = 4;

        // Run N concurrent tasks, each performing M schema changes in sequence.
        auto successes = co_await map_reduce(boost::irange(size_t{0}, N), std::bind_front(perform_schema_changes, std::ref(e), M), 0, std::plus{});

        // The number of new entries that appeared in group 0 history table should be exactly equal
        // to the number of successful schema changes.
        BOOST_REQUIRE_EQUAL(successes, (co_await get_history_size()) - size);

        // Make it so that execution of a DDL statement will perform up to (N-1) * M + 1 attempts (first try + up to (N-1) * M retries).
        mm.set_concurrent_ddl_retries((N-1)*M);

        // Run N concurrent tasks, each performing M schema changes in sequence.
        // (use different range of task_ids so the new tasks' statements don't conflict with existing keyspaces from previous tasks)
        successes = co_await map_reduce(boost::irange(N, 2*N), std::bind_front(perform_schema_changes, std::ref(e), M), 0, std::plus{});

        // Each task performs M schema changes. There are N tasks.
        // Thus, for each task, all other tasks combined perform (N-1) * M schema changes.
        // Each `group0_concurrent_modification` exception means that some statement executed successfully in another task.
        // Thus, each statement can get at most (N-1) * M `group0_concurrent_modification` exceptions.
        // Since we configured the system to perform (N-1) * M + 1 attempts, the last attempt should always succeed even if all previous
        // ones failed - because that means every other task has finished its work.
        // Thus, `group0_concurrent_modification` should never propagate outside `execute_cql`.
        // Therefore the number of successes should be the number of calls to `execute_cql`, which is N*M in total.
        BOOST_REQUIRE_EQUAL(successes, N*M);

        // Let's verify that the mutex indeed does its job.
        rclient.operation_mutex().consume(1337);
        mm.set_concurrent_ddl_retries(0);

        successes = co_await map_reduce(boost::irange(2*N, 3*N), std::bind_front(perform_schema_changes, std::ref(e), M), 0, std::plus{});

        // Each execution should have succeeded on first attempt because the mutex serialized them all.
        BOOST_REQUIRE_EQUAL(successes, N*M);

    });
}

SEASTAR_TEST_CASE(test_group0_batch) {
    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        auto& rclient = e.get_raft_group0_client();
        abort_source as;

        co_await e.execute_cql("CREATE TABLE test_group0_batch (key int, part int, PRIMARY KEY (key, part))");

        auto insert_mut = [&] (int key, int part) -> future<mutation> {
            auto muts = co_await e.get_modification_mutations(format("INSERT INTO test_group0_batch (key, part) VALUES ({}, {})", key, part));
            co_return muts[0];
        };

        auto do_transaction = [&] (std::function<future<>(service::group0_batch&)> f) -> future<> {
            auto guard = co_await rclient.start_operation(as);
            service::group0_batch mc(std::move(guard));
            co_await f(mc);
            co_await std::move(mc).commit(rclient, as, ::service::raft_timeout{});
        };

        // test simple add_mutation
        co_await do_transaction([&] (service::group0_batch& mc) -> future<> {
            mc.add_mutation(co_await insert_mut(1, 1));
            mc.add_mutation(co_await insert_mut(2, 1));
        });
        BOOST_TEST_PASSPOINT();
        assert_that(co_await e.execute_cql("SELECT * FROM test_group0_batch WHERE part = 1 ALLOW FILTERING"))
            .is_rows()
            .with_size(2)
            .with_rows_ignore_order({
                {int32_type->decompose(1), int32_type->decompose(1)},
                {int32_type->decompose(2), int32_type->decompose(1)}});

        // test extract
        {
            auto guard = co_await rclient.start_operation(as);
            service::group0_batch mc(std::move(guard));
            mc.add_mutation(co_await insert_mut(1, 2));
            mc.add_generator([&] (api::timestamp_type t) -> ::service::mutations_generator {
                co_yield co_await insert_mut(2, 2);
                co_yield co_await insert_mut(3, 2);
            });
            auto v = co_await std::move(mc).extract();
            BOOST_REQUIRE_EQUAL(v.first.size(), 3);
            BOOST_REQUIRE(v.second); // we got the guard too
        }
        BOOST_TEST_PASSPOINT();
        assert_that(co_await e.execute_cql("SELECT * FROM test_group0_batch WHERE part = 2 ALLOW FILTERING")).is_rows().is_empty();

        // test all add methods combined
        co_await do_transaction([&] (service::group0_batch& mc) -> future<> {
            mc.add_mutations({
                co_await insert_mut(1, 3),
                co_await insert_mut(2, 3)
            });
            mc.add_mutation(co_await insert_mut(3, 3));
            mc.add_generator([&] (api::timestamp_type t) -> ::service::mutations_generator {
                co_yield co_await insert_mut(4, 3);
                co_yield co_await insert_mut(5, 3);
                co_yield co_await insert_mut(6, 3);
            });
        });
        BOOST_TEST_PASSPOINT();
        assert_that(co_await e.execute_cql("SELECT * FROM test_group0_batch WHERE part = 3 ALLOW FILTERING"))
            .is_rows()
            .with_size(6)
            .with_rows_ignore_order({
                {int32_type->decompose(1), int32_type->decompose(3)},
                {int32_type->decompose(2), int32_type->decompose(3)},
                {int32_type->decompose(3), int32_type->decompose(3)},
                {int32_type->decompose(4), int32_type->decompose(3)},
                {int32_type->decompose(5), int32_type->decompose(3)},
                {int32_type->decompose(6), int32_type->decompose(3)}});

        // test generator only
        co_await do_transaction([&] (service::group0_batch& mc) -> future<> {
            mc.add_generator([&] (api::timestamp_type t) -> ::service::mutations_generator {
                co_yield co_await insert_mut(1, 4);
            });
            co_return;
        });
        BOOST_TEST_PASSPOINT();
        assert_that(co_await e.execute_cql("SELECT * FROM test_group0_batch WHERE part = 4 ALLOW FILTERING"))
            .is_rows()
            .with_size(1)
            .with_rows_ignore_order({
                {int32_type->decompose(1), int32_type->decompose(4)}});


        // nop without mutations nor generator
        auto mc1 = service::group0_batch::unused();
        co_await std::move(mc1).commit(rclient, as, ::service::raft_timeout{});
    });
}
