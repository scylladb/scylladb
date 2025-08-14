/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"

#include "utils/disk_space_monitor.hh"
#include "utils/updateable_value.hh"

BOOST_AUTO_TEST_SUITE(disk_space_monitor_test)

SEASTAR_THREAD_TEST_CASE(test_capacity_override) {
    utils::updateable_value_source<uint64_t> data_file_capacity(0);

    utils::disk_space_monitor::config dsm_cfg = {
        .sched_group = create_scheduling_group("streaming", 200).get(),
        // The test controls polling by triggering it manually so make it big
        .normal_polling_interval = utils::updateable_value<int>(std::numeric_limits<int>::max()),
        .high_polling_interval = utils::updateable_value<int>(std::numeric_limits<int>::max()),
        .polling_interval_threshold = utils::updateable_value<float>(1.0),
        .capacity_override = utils::updateable_value<uint64_t>(data_file_capacity),
    };

    seastar::abort_source as;
    tmpdir data_dir;
    auto data_dir_path = data_dir.path().string();

    utils::disk_space_monitor dsm(as, data_dir_path, dsm_cfg);
    auto stop_dsm = defer([&dsm] { dsm.stop().get(); });

    std::filesystem::space_info orig_space = {
        .capacity = 100,
        .free = 12,
        .available = 11,
    };
    auto reg = dsm.set_space_source([orig_space] { return make_ready_future<std::filesystem::space_info>(orig_space); });

    dsm.start().get();

    utils::phased_barrier poll_barrier("poll_barrier"); // new operation started whenever monitor calls listeners.
    auto op = poll_barrier.start();
    auto listener_registration = dsm.listen([&] (auto& mon) mutable {
        op = poll_barrier.start();
        return make_ready_future<>();
    });

    dsm.trigger_poll();
    poll_barrier.advance_and_await().get();
    BOOST_REQUIRE(dsm.space() == orig_space);

    data_file_capacity.set(90);
    dsm.trigger_poll();
    poll_barrier.advance_and_await().get();

    BOOST_REQUIRE_EQUAL(dsm.space().capacity, 90);
    BOOST_REQUIRE_EQUAL(dsm.space().available, 1);
    BOOST_REQUIRE_EQUAL(dsm.space().free, 2);

    data_file_capacity.set(10);
    dsm.trigger_poll();
    poll_barrier.advance_and_await().get();
    BOOST_REQUIRE_EQUAL(dsm.space().capacity, 10);
    BOOST_REQUIRE_EQUAL(dsm.space().available, 0);
    BOOST_REQUIRE_EQUAL(dsm.space().free, 0);

    data_file_capacity.set(1000);
    dsm.trigger_poll();
    poll_barrier.advance_and_await().get();
    BOOST_REQUIRE_EQUAL(dsm.space().capacity, 1000);
    BOOST_REQUIRE_EQUAL(dsm.space().available, 911);
    BOOST_REQUIRE_EQUAL(dsm.space().free, 912);

    data_file_capacity.set(0);
    dsm.trigger_poll();
    poll_barrier.advance_and_await().get();
    BOOST_REQUIRE(dsm.space() == orig_space);
}

SEASTAR_THREAD_TEST_CASE(test_subscription_options) {
    utils::disk_space_monitor::config dsm_cfg = {
        .sched_group = create_scheduling_group("streaming", 200).get(),
        // The test controls polling by triggering it manually so make it big
        .normal_polling_interval = utils::updateable_value<int>(std::numeric_limits<int>::max()),
        .high_polling_interval = utils::updateable_value<int>(std::numeric_limits<int>::max()),
        .polling_interval_threshold = utils::updateable_value<float>(1.0),
        .capacity_override = utils::updateable_value<uint64_t>(0),
    };

    seastar::abort_source as;
    tmpdir data_dir;
    auto data_dir_path = data_dir.path().string();

    utils::disk_space_monitor dsm(as, data_dir_path, dsm_cfg);
    auto stop_dsm = defer([&dsm] { dsm.stop().get(); });
    dsm.start().get();

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
    stop_dsm.cancel();

    BOOST_REQUIRE_EQUAL(stats.constant_updates, 10);
    BOOST_REQUIRE_EQUAL(stats.constant_updates_only_above, 6);
    BOOST_REQUIRE_EQUAL(stats.constant_updates_only_below, 4);

    BOOST_REQUIRE_EQUAL(stats.crossing_threshold_updates, 3);
    BOOST_REQUIRE_EQUAL(stats.crossing_threshold_updates_only_above, 2);
    BOOST_REQUIRE_EQUAL(stats.crossing_threshold_updates_only_below, 1);
}

BOOST_AUTO_TEST_SUITE_END()
