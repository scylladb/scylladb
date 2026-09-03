/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/test_case.hh>

#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include "gms/versioned_value.hh"

// Unit tests for gms::merge_endpoint_state(), which the gossiper uses to
// coalesce pending endpoint states of the same generation: for every
// application state and for the heartbeat, the highest version must win, and
// no value present in either input may be lost. The last case is the one
// that matters: a heartbeat-only state always carries the highest version,
// so any scheme that picks one whole state over the other by version would
// discard the other state's application states, and gossip never re-delivers
// a value below the advertised max version.

using namespace gms;

namespace {

endpoint_state make_state(int32_t hb_version, std::initializer_list<std::pair<application_state, versioned_value>> states) {
    endpoint_state es{heart_beat_state{generation_type(1), version_type(hb_version)}, inet_address("127.0.0.1")};
    for (auto& [key, value] : states) {
        es.add_application_state(key, value);
    }
    return es;
}

versioned_value val(sstring value, int32_t version) {
    return versioned_value(utils::chunked_string(value), version_type(version));
}

const versioned_value* get(const endpoint_state& es, application_state key) {
    return es.get_application_state_ptr(key);
}

void check(const endpoint_state& es, application_state key, sstring value, int32_t version) {
    const auto* v = get(es, key);
    BOOST_REQUIRE(v != nullptr);
    BOOST_REQUIRE_EQUAL(v->value().linearize(), value);
    BOOST_REQUIRE_EQUAL(v->version().value(), version);
}

} // anonymous namespace

// Disjoint application states: the result must carry both.
SEASTAR_TEST_CASE(test_merge_unions_disjoint_states) {
    auto into = make_state(10, {{application_state::STATUS, val("NORMAL", 6)}});
    auto from = make_state(12, {{application_state::LOAD, val("0.5", 11)}});

    merge_endpoint_state(into, from);

    check(into, application_state::STATUS, "NORMAL", 6);
    check(into, application_state::LOAD, "0.5", 11);
    BOOST_REQUIRE_EQUAL(into.get_heart_beat_state().get_heart_beat_version().value(), 12);
    return seastar::make_ready_future<>();
}

// Overlapping application state: the higher version must win, in both
// directions, and an equal version must not overwrite.
SEASTAR_TEST_CASE(test_merge_keeps_highest_version_per_state) {
    auto into = make_state(10, {{application_state::STATUS, val("BOOT", 4)}});
    auto from = make_state(12, {{application_state::STATUS, val("NORMAL", 6)}});
    merge_endpoint_state(into, from);
    check(into, application_state::STATUS, "NORMAL", 6);

    // Older incoming value must not regress the newer one.
    auto older = make_state(13, {{application_state::STATUS, val("BOOT", 4)}});
    merge_endpoint_state(into, older);
    check(into, application_state::STATUS, "NORMAL", 6);

    // Equal version: keep what we have.
    auto equal = make_state(14, {{application_state::STATUS, val("SHOULD_NOT_APPEAR", 6)}});
    merge_endpoint_state(into, equal);
    check(into, application_state::STATUS, "NORMAL", 6);
    return seastar::make_ready_future<>();
}

// The heartbeat: higher version wins, lower does not regress it.
SEASTAR_TEST_CASE(test_merge_keeps_highest_heartbeat) {
    auto into = make_state(20, {});
    auto newer = make_state(30, {});
    merge_endpoint_state(into, newer);
    BOOST_REQUIRE_EQUAL(into.get_heart_beat_state().get_heart_beat_version().value(), 30);

    auto older = make_state(25, {});
    merge_endpoint_state(into, older);
    BOOST_REQUIRE_EQUAL(into.get_heart_beat_state().get_heart_beat_version().value(), 30);
    return seastar::make_ready_future<>();
}

// The case the merge exists for: a heartbeat-only state has the highest
// version of all, but must not displace the application states of a richer
// state it is coalesced with — in either arrival order.
SEASTAR_TEST_CASE(test_merge_heartbeat_only_state_loses_no_facts) {
    const auto rich = [] {
        return make_state(43, {
            {application_state::STATUS, val("NORMAL", 41)},
            {application_state::TOKENS, val("t1,t2", 3)},
            {application_state::SCHEMA, val("s1", 21)},
        });
    };
    const auto hb_only = [] {
        return make_state(80, {});
    };

    // Rich state pending, heartbeat-only arrives.
    auto into = rich();
    merge_endpoint_state(into, hb_only());
    check(into, application_state::STATUS, "NORMAL", 41);
    check(into, application_state::TOKENS, "t1,t2", 3);
    check(into, application_state::SCHEMA, "s1", 21);
    BOOST_REQUIRE_EQUAL(into.get_heart_beat_state().get_heart_beat_version().value(), 80);

    // Heartbeat-only pending, rich state arrives.
    auto into2 = hb_only();
    merge_endpoint_state(into2, rich());
    check(into2, application_state::STATUS, "NORMAL", 41);
    check(into2, application_state::TOKENS, "t1,t2", 3);
    check(into2, application_state::SCHEMA, "s1", 21);
    BOOST_REQUIRE_EQUAL(into2.get_heart_beat_state().get_heart_beat_version().value(), 80);
    return seastar::make_ready_future<>();
}
