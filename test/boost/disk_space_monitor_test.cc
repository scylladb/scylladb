/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include "utils/disk_space_monitor.hh"
#include "db/config.hh"

BOOST_AUTO_TEST_SUITE(disk_space_monitor_test)

SEASTAR_TEST_CASE(test_capacity_override) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        utils::disk_space_monitor& monitor = e.disk_space_monitor();
        std::filesystem::space_info orig_space = {
            .capacity = 100,
            .free = 12,
            .available = 11,
        };
        auto reg = monitor.set_space_source([&] { return make_ready_future<std::filesystem::space_info>(orig_space); });

        utils::phased_barrier poll_barrier("poll_barrier"); // new operation started whenever monitor calls listeners.
        auto op = poll_barrier.start();
        auto listener_registration = monitor.listen([&] (auto& mon) mutable {
            op = poll_barrier.start();
            return make_ready_future<>();
        });

        monitor.trigger_poll();
        poll_barrier.advance_and_await().get();
        BOOST_REQUIRE(monitor.space() == orig_space);

        e.db_config().data_file_capacity(90);
        monitor.trigger_poll();
        poll_barrier.advance_and_await().get();

        BOOST_REQUIRE_EQUAL(monitor.space().capacity, 90);
        BOOST_REQUIRE_EQUAL(monitor.space().available, 1);
        BOOST_REQUIRE_EQUAL(monitor.space().free, 2);

        e.db_config().data_file_capacity(10);
        monitor.trigger_poll();
        poll_barrier.advance_and_await().get();
        BOOST_REQUIRE_EQUAL(monitor.space().capacity, 10);
        BOOST_REQUIRE_EQUAL(monitor.space().available, 0);
        BOOST_REQUIRE_EQUAL(monitor.space().free, 0);

        e.db_config().data_file_capacity(1000);
        monitor.trigger_poll();
        poll_barrier.advance_and_await().get();
        BOOST_REQUIRE_EQUAL(monitor.space().capacity, 1000);
        BOOST_REQUIRE_EQUAL(monitor.space().available, 911);
        BOOST_REQUIRE_EQUAL(monitor.space().free, 912);

        e.db_config().data_file_capacity(0);
        monitor.trigger_poll();
        poll_barrier.advance_and_await().get();
        BOOST_REQUIRE(monitor.space() == orig_space);
    });
}


SEASTAR_THREAD_TEST_CASE(test_subscription_options) {
    cql_test_config test_cfg;
    auto& db_cfg = *test_cfg.db_config;

    db_cfg.critical_disk_utilization_level(0.8);
    // The test controls polling by triggering it manually in the test.
    db_cfg.disk_space_monitor_normal_polling_interval_in_seconds(100);
    db_cfg.disk_space_monitor_high_polling_interval_in_seconds(100);

    do_with_cql_env_thread([](cql_test_env& e) {
        auto& dsm = e.disk_space_monitor();
        float disk_utilization = 0.1;

        auto registration = dsm.set_space_source([&] {
            std::filesystem::space_info space = {
                .capacity = 100,
                .free = 100*(1.0-disk_utilization),
                .available = 100*(1-disk_utilization),
            };
            return make_ready_future<std::filesystem::space_info>(space);
        });

        struct stats {
            size_t constant_updates = 0;
            size_t constant_updates_only_above = 0;
            size_t constant_updates_only_below = 0;

            size_t crossing_threshold_updates = 0;
            size_t crossing_threshold_updates_only_above = 0;
            size_t crossing_threshold_updates_only_below = 0;
        } stats;

        utils::phased_barrier poll_barrier("poll_barrier");
        auto op = poll_barrier.start();
        auto listener_registration = dsm.listen([&] (auto&) mutable {
            op = poll_barrier.start();
            return make_ready_future<>();
        });

        auto s1 = dsm.subscribe(0.8, [&stats] (auto above_threshold) -> future<> {
            ++stats.constant_updates;
            return make_ready_future<>();
        }, {.only_crossing_threshold = false, .when_above_threshold = true, .when_below_threshold = true});

        auto s2 = dsm.subscribe(0.8, [&stats] (auto above_threshold) -> future<> {
            ++stats.constant_updates_only_above;
            BOOST_CHECK(above_threshold == utils::disk_space_monitor::above_threshold::yes);
            return make_ready_future<>();
        }, {.only_crossing_threshold = false, .when_above_threshold = true, .when_below_threshold = false});

        auto s3 = dsm.subscribe(0.8, [&stats] (auto above_threshold) -> future<> {
            ++stats.constant_updates_only_below;
            BOOST_CHECK(above_threshold == utils::disk_space_monitor::above_threshold::no);
            return make_ready_future<>();
        }, {.only_crossing_threshold = false, .when_above_threshold = false, .when_below_threshold = true});

        auto s4 = dsm.subscribe(0.8, [&stats] (auto above_threshold) -> future<> {
            ++stats.crossing_threshold_updates;
            return make_ready_future<>();
        }, {.only_crossing_threshold = true, .when_above_threshold = true, .when_below_threshold = true});

        auto s5 = dsm.subscribe(0.8, [&stats] (auto above_threshold) -> future<> {
            ++stats.crossing_threshold_updates_only_above;
            BOOST_CHECK(above_threshold == utils::disk_space_monitor::above_threshold::yes);
            return make_ready_future<>();
        }, {.only_crossing_threshold = true, .when_above_threshold = true, .when_below_threshold = false});

        auto s6 = dsm.subscribe(0.8, [&stats] (auto above_threshold) -> future<> {
            ++stats.crossing_threshold_updates_only_below;
            BOOST_CHECK(above_threshold == utils::disk_space_monitor::above_threshold::no);
            return make_ready_future<>();
        }, {.only_crossing_threshold = true, .when_above_threshold = false, .when_below_threshold = true});

        for (float du : {0.1, 0.5, 0.85, 0.9, 0.7, 0.6, 0.93, 0.9, 0.82, 0.81}) {
            disk_utilization = du;
            dsm.trigger_poll();
            poll_barrier.advance_and_await().get();
        }

        dsm.stop().get();

        BOOST_REQUIRE_EQUAL(stats.constant_updates, 10);
        BOOST_REQUIRE_EQUAL(stats.constant_updates_only_above, 6);
        BOOST_REQUIRE_EQUAL(stats.constant_updates_only_below, 4);

        BOOST_REQUIRE_EQUAL(stats.crossing_threshold_updates, 3);
        BOOST_REQUIRE_EQUAL(stats.crossing_threshold_updates_only_above, 2);
        BOOST_REQUIRE_EQUAL(stats.crossing_threshold_updates_only_below, 1);

    }, test_cfg).get();
}

BOOST_AUTO_TEST_SUITE_END()
