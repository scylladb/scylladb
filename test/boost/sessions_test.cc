/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/random_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include "service/session.hh"

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
