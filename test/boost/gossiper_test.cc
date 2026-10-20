/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <sstream>

#include "gms/gossiper.hh"
#include "test/lib/cql_test_env.hh"
#include "utils/error_injection.hh"

BOOST_AUTO_TEST_SUITE(gossiper_test)

using namespace std::chrono_literals;

// Reproduces SCYLLADB-5502: lock_endpoint() gave up with timed_out_error when its
// wait round expired, instead of logging the lock holders and waiting again.
// Captures the log, so it must run on one shard (see custom_args in test_config.yaml).
SEASTAR_TEST_CASE(test_lock_endpoint_keeps_waiting_after_wait_round_expires) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
    return make_ready_future<>();
#else
    // seastar::logger writes to one process-wide ostream without locking.
    if (this_smp_shard_count() != 1) {
        fmt::print("Skipping test as it captures the log and needs -c1.\n");
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto& g = e.gossiper().local();
        const auto ep = locator::host_id::create_random_id();

        std::ostringstream log;
        seastar::logger::set_ostream(log);
        // Replay the captured log so a failure keeps the gossiper output.
        auto restore_log = defer([&log] noexcept {
            seastar::logger::set_ostream(std::cerr);
            std::cerr << log.view();
        });

        constexpr auto wait_injection = "gossiper_lock_endpoint_wait_ms";
        utils::get_local_injector().enable(wait_injection, false /* one_shot */, {{"value", "50"}});
        auto disable_injection = defer([&] noexcept { utils::get_local_injector().disable(wait_injection); });

        const auto expired_line = fmt::format("waiting for endpoint lock (ep={}) took more than", ep);
        auto expired_rounds = [&] {
            const auto s = log.str();
            size_t n = 0;
            for (auto pos = s.find(expired_line); pos != std::string::npos; pos = s.find(expired_line, pos + 1)) {
                ++n;
            }
            return n;
        };

        // lock_endpoint() warns with a backtrace when called outside the gossip scheduling group.
        auto lock = [&] {
            return with_scheduling_group(g.get_scheduling_group(), [&] { return g.lock_endpoint(ep, gms::null_permit_id); });
        };
        auto holder = lock().get();
        auto waiter = lock();

        const auto deadline = lowres_clock::now() + 30s;
        while (expired_rounds() < 2 && !waiter.available() && lowres_clock::now() < deadline) {
            seastar::sleep(10ms).get();
        }
        if (waiter.failed()) {
            BOOST_FAIL(fmt::format("lock_endpoint() failed while the lock was held: {}", waiter.get_exception()));
        }
        BOOST_REQUIRE(!waiter.available());
        BOOST_REQUIRE_GE(expired_rounds(), 2);

        holder.release();
        auto permit = waiter.get();
        BOOST_REQUIRE(permit.id());
    });
#endif
}

BOOST_AUTO_TEST_SUITE_END()
