/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Reproducer for: cql_server::event_notifier fans out one schema/topology/status
// event to every registered listener by independently reconstructing and
// re-serializing an identical response body per listener (transport/event_notifier.cc),
// instead of serializing the body once and sharing it across listeners.
//
// Exercises the actual production caching template (get_or_make_shared_event(),
// shared by every event_notifier.cc fan-out loop) and the actual production
// event builder (make_schema_change_event_response(), the same function
// connection::make_schema_change_event() delegates to), asserting the body is
// constructed at most once per distinct cache key regardless of listener count.

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "transport/event.hh"
#include "transport/response.hh"
#include "tracing/trace_state.hh"

using namespace cql_transport;

BOOST_AUTO_TEST_CASE(test_fanout_shared_cache_constructs_body_once_per_key) {
    event::schema_change ev(
        event::schema_change::change_type::CREATED,
        event::schema_change::target_type::TABLE,
        "ks", "cf");

    size_t construction_count = 0;
    cql_transport::shared_event_cache<uint8_t> cache;
    constexpr size_t n_listeners = 20000;

    for (size_t i = 0; i < n_listeners; ++i) {
        cql_transport::get_or_make_shared_event(cache, uint8_t{4}, [&] {
            ++construction_count;
            return cql_transport::make_schema_change_event_response(ev, 4);
        });
    }
    BOOST_REQUIRE_EQUAL(construction_count, 1u);

    // A second, distinct key adds exactly one more construction, not one per
    // listener: "once per key", not "once total".
    for (size_t i = 0; i < n_listeners; ++i) {
        cql_transport::get_or_make_shared_event(cache, uint8_t{5}, [&] {
            ++construction_count;
            return cql_transport::make_schema_change_event_response(ev, 5);
        });
    }
    BOOST_REQUIRE_EQUAL(construction_count, 2u);
}
