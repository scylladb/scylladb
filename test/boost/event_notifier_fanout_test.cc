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
// This file has one part:
//  1. test_fanout_bodies_are_identical_across_listeners: calls the exact
//     production serialization code the fan-out loop calls per listener and
//     shows the produced bytes are byte-for-byte identical every time for the
//     same event, i.e. recomputing them per listener is pure waste.

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <vector>

#include "transport/event.hh"
#include "transport/response.hh"
#include "tracing/trace_state.hh"

using namespace cql_transport;

namespace {

// Exactly what cql_server::connection::make_schema_change_event() does
// (transport/server.cc), which is what event_notifier's fan-out loop calls
// once per registered listener (transport/event_notifier.cc).
bytes_ostream make_schema_change_body(const event::schema_change& ev, uint8_t version) {
    response r(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    r.write_string("SCHEMA_CHANGE");
    r.serialize(ev, version);
    return std::move(r).extract_body();
}

}

BOOST_AUTO_TEST_CASE(test_fanout_bodies_are_identical_across_listeners) {
    // The whole premise of "serialize once, fan out" being possible is that
    // the bytes are the same for every listener. Confirm that's true for the
    // actual production serialization, not an assumption.
    event::schema_change ev(
        event::schema_change::change_type::CREATED,
        event::schema_change::target_type::TABLE,
        "ks", "cf");

    auto first = make_schema_change_body(ev, 4);
    BOOST_REQUIRE_GT(first.size(), 0u);
    for (int i = 0; i < 50; ++i) {
        auto body = make_schema_change_body(ev, 4);
        BOOST_REQUIRE_EQUAL(body.size(), first.size());
        BOOST_REQUIRE(body == first);
    }
}
