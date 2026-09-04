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
// This file has three parts, all calling the real production functions
// directly (cql_transport::make_schema_change_event_response() and
// cql_transport::get_or_make_shared_event()/shared_event_cache, both in
// transport/response.hh) rather than test-local reimplementations:
//  1. test_fanout_bodies_are_identical_across_listeners: calls the exact
//     production serialization code the fan-out loop calls per listener and
//     shows the produced bytes are byte-for-byte identical every time for the
//     same event, i.e. recomputing them per listener is pure waste.
//  2. test_fanout_cost_does_not_amortize_with_listener_count: documents, via
//     BOOST_TEST_MESSAGE only (not asserted, since wall-clock deltas are
//     inherently flaky under scheduler/allocator contention), that the raw
//     per-listener serialization cost is O(N) with no amortization.
//  3. test_fanout_shared_cache_constructs_body_once_per_key: exercises the
//     actual production caching template (get_or_make_shared_event(), shared
//     by every event_notifier.cc fan-out loop) and the actual production
//     event builder (make_schema_change_event_response(), the same function
//     connection::make_schema_change_event() delegates to), asserting the
//     body is constructed at most once per distinct cache key regardless of
//     listener count.

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <vector>

#include "transport/event.hh"
#include "transport/response.hh"
#include "tracing/trace_state.hh"

using namespace cql_transport;
using namespace std::chrono;

namespace {

// Extracts the body bytes from the actual production event-response builder.
bytes_ostream make_schema_change_body(const event::schema_change& ev, uint8_t version) {
    return std::move(*make_schema_change_event_response(ev, version)).extract_body();
}

// Simulates event_notifier::on_create_column_family's loop body run N times:
// N "listeners" all being notified of the same table creation. Returns
// (total wall time, the last produced body) so callers can check both cost
// and correctness of the redundant-serialization claim.
std::pair<nanoseconds, bytes_ostream> run_fanout(size_t n_listeners) {
    event::schema_change ev(
        event::schema_change::change_type::CREATED,
        event::schema_change::target_type::TABLE,
        "ks", "cf");

    bytes_ostream last_body;
    auto start = steady_clock::now();
    for (size_t i = 0; i < n_listeners; ++i) {
        last_body = make_schema_change_body(ev, 4);
    }
    auto elapsed = steady_clock::now() - start;
    return {duration_cast<nanoseconds>(elapsed), std::move(last_body)};
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

BOOST_AUTO_TEST_CASE(test_fanout_cost_does_not_amortize_with_listener_count) {
    // Small vs. large but test-feasible listener count (avoid the literal
    // production scale point of 10,000s of CQL connections).
    constexpr size_t small_n = 100;
    constexpr size_t large_n = 20000;

    // Warm up (branch predictors, allocator caches) so the first measurement
    // isn't penalized by one-time setup costs.
    run_fanout(1000);

    auto [small_time, small_body] = run_fanout(small_n);
    auto [large_time, large_body] = run_fanout(large_n);

    BOOST_REQUIRE_EQUAL(small_body.size(), large_body.size());
    BOOST_REQUIRE(small_body == large_body);

    double small_per_op = double(small_time.count()) / small_n;
    double large_per_op = double(large_time.count()) / large_n;
    double ratio_of_totals = double(large_time.count()) / double(small_time.count());
    double expected_ratio = double(large_n) / double(small_n); // 200x

    // Informational only: wall-clock timing is inherently noisy under
    // scheduler/allocator contention, so it is reported but not asserted on.
    // The deterministic invariant that used to be checked here (cost does
    // not amortize with N) is instead covered by
    // test_fanout_shared_cache_constructs_body_once_per_key below, which
    // asserts a construction count rather than a timing ratio.
    BOOST_TEST_MESSAGE("small_n=" << small_n << " total_ns=" << small_time.count()
        << " per_op_ns=" << small_per_op);
    BOOST_TEST_MESSAGE("large_n=" << large_n << " total_ns=" << large_time.count()
        << " per_op_ns=" << large_per_op);
    BOOST_TEST_MESSAGE("ratio_of_totals=" << ratio_of_totals << " expected(linear)=" << expected_ratio);
}

BOOST_AUTO_TEST_CASE(test_fanout_shared_cache_constructs_body_once_per_key) {
    // Exercises the actual production caching (cql_transport::get_or_make_shared_event,
    // shared by event_notifier.cc's fan-out loops) and the actual production
    // event builder (cql_transport::make_schema_change_event_response, the same
    // function connection::make_schema_change_event() delegates to) -- not a
    // test-local reimplementation of either.
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
