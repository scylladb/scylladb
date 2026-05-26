/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/random_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include "service/session.hh"

BOOST_AUTO_TEST_SUITE(sessions_test)

using namespace service;

SEASTAR_TEST_CASE(test_default_session_always_exists) {
    return seastar::async([] {
        session_manager mgr;
        auto guard = mgr.enter_session(default_session_id);
        guard.check();

        mgr.initiate_close_of_sessions_except({});
        mgr.drain_closing_sessions().get();

        guard = mgr.enter_session(default_session_id);
        guard.check();
    });
}

SEASTAR_TEST_CASE(test_default_constructible_session_does_not_exist) {
    return seastar::async([] {
        session_manager mgr;
        session_id id;
        // For safety, we don't want to treat unset id same as default_session_id.
        BOOST_REQUIRE_THROW(mgr.enter_session(id), std::runtime_error);
    });
}

SEASTAR_TEST_CASE(test_session_closing) {
    return seastar::async([] {
        session_manager mgr;

        auto id = session_id(utils::make_random_uuid());
        auto id2 = session_id(utils::make_random_uuid());
        auto id3 = session_id(utils::make_random_uuid());
        auto id4 = session_id(utils::make_random_uuid());

        BOOST_REQUIRE_THROW(mgr.enter_session(id), std::runtime_error);

        mgr.create_session(id);
        mgr.create_session(id2);

        auto guard = mgr.enter_session(id);
        auto guard2 = mgr.enter_session(id2);

        guard.check();
        guard2.check();

        mgr.initiate_close_of_sessions_except({id});

        BOOST_REQUIRE(guard.valid());
        BOOST_REQUIRE(!guard2.valid());

        auto f = mgr.drain_closing_sessions();
        auto f2 = mgr.drain_closing_sessions(); // test concurrent drain
        BOOST_REQUIRE(!f.available()); // blocked by guard2

        // Concurrent wait drain
        mgr.create_session(id3);
        mgr.initiate_close_of_sessions_except({id});
        mgr.create_session(id3); // no-op
        mgr.create_session(id4);

        {
            auto _ = std::move(guard2);
        }

        f.get();
        f2.get();

        mgr.drain_closing_sessions().get();

        BOOST_REQUIRE(guard.valid());

        mgr.enter_session(id);
        mgr.enter_session(id4);
        BOOST_REQUIRE_THROW(mgr.enter_session(id2), std::runtime_error);
        BOOST_REQUIRE_THROW(mgr.enter_session(id3), std::runtime_error);
    });
}

// Verifies that when a session is closed, subscribers to its abort_source
// are notified, enabling them to release their session guards and unblock
// drain_closing_sessions(). This is the mechanism used to clean up orphaned
// repair_meta instances on follower nodes.
SEASTAR_TEST_CASE(test_session_abort_source_unblocks_drain) {
    return seastar::async([] {
        session_manager mgr;

        auto id = session_id(utils::make_random_uuid());
        mgr.create_session(id);

        // Simulate a repair_meta holding a session guard
        auto guard = std::make_optional<session::guard>(mgr.enter_session(id));
        BOOST_REQUIRE(guard->valid());

        // Subscribe to the session's abort_source (as insert_repair_meta does)
        auto& session_as = mgr.get_session_abort_source(id);
        BOOST_REQUIRE(!session_as.abort_requested());

        bool aborted = false;
        auto sub = session_as.subscribe([&] () noexcept {
            aborted = true;
            // Release the guard (simulates repair_meta cleanup)
            guard.reset();
        });
        BOOST_REQUIRE(sub);

        // Close the session — this should fire the abort_source
        mgr.initiate_close_of_sessions_except({});
        BOOST_REQUIRE(aborted);
        BOOST_REQUIRE(!guard.has_value());
        BOOST_REQUIRE(session_as.abort_requested());

        // drain should complete immediately since the guard was released
        mgr.drain_closing_sessions().get();
    });
}

// Verifies that subscribing to an already-closed session's abort_source
// returns an empty subscription (abort already requested).
SEASTAR_TEST_CASE(test_session_abort_source_already_closed) {
    return seastar::async([] {
        session_manager mgr;

        auto id = session_id(utils::make_random_uuid());
        mgr.create_session(id);

        mgr.initiate_close_of_sessions_except({});

        auto& session_as = mgr.get_session_abort_source(id);
        BOOST_REQUIRE(session_as.abort_requested());

        // Subscribing after abort returns empty subscription
        bool called = false;
        auto sub = session_as.subscribe([&] () noexcept {
            called = true;
        });
        BOOST_REQUIRE(!sub);
        BOOST_REQUIRE(!called);

        mgr.drain_closing_sessions().get();
    });
}

// Verifies the late-subscription path used by insert_repair_meta(): if the
// session is already closing, subscribe() returns an empty subscription and the
// caller must release its guard explicitly.
SEASTAR_TEST_CASE(test_session_abort_source_already_closed_requires_immediate_cleanup) {
    return seastar::async([] {
        session_manager mgr;

        auto id = session_id(utils::make_random_uuid());
        mgr.create_session(id);

        auto guard = std::make_optional<session::guard>(mgr.enter_session(id));

        mgr.initiate_close_of_sessions_except({});

        auto& session_as = mgr.get_session_abort_source(id);
        BOOST_REQUIRE(session_as.abort_requested());

        bool called = false;
        auto sub = session_as.subscribe([&] () noexcept {
            called = true;
        });
        BOOST_REQUIRE(!sub);
        BOOST_REQUIRE(!called);

        auto f = mgr.drain_closing_sessions();
        BOOST_REQUIRE(!f.available());

        // This mirrors the insert_repair_meta() fix: when subscribe() returns
        // empty, the late entrant must clean itself up immediately.
        guard.reset();
        f.get();
    });
}

// Verifies that drain blocks until the abort subscriber releases the guard.
SEASTAR_TEST_CASE(test_session_abort_source_drain_blocks_without_release) {
    return seastar::async([] {
        session_manager mgr;

        auto id = session_id(utils::make_random_uuid());
        mgr.create_session(id);

        auto guard = std::make_optional<session::guard>(mgr.enter_session(id));

        // Subscribe but do NOT release the guard in the callback
        auto& session_as = mgr.get_session_abort_source(id);
        bool aborted = false;
        auto sub = session_as.subscribe([&] () noexcept {
            aborted = true;
            // Intentionally not releasing guard here
        });

        mgr.initiate_close_of_sessions_except({});
        BOOST_REQUIRE(aborted);

        // drain should block because the guard is still held
        auto f = mgr.drain_closing_sessions();
        BOOST_REQUIRE(!f.available());

        // Now release the guard — drain should complete
        guard.reset();
        f.get();
    });
}

BOOST_AUTO_TEST_SUITE_END()
