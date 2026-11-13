/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"

#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include "gms/gossiper.hh"
#include "gms/versioned_value.hh"
#include "message/messaging_service.hh"
#include "idl/gossip.dist.hh"

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

// Regression for #15899: importing only DRAINING raises the advertised max
// above application values the coordinator has not received, hiding them forever.
SEASTAR_TEST_CASE(test_prepare_shutdown_preserves_earlier_application_states) {
    cql_test_config cfg;
    cfg.ms_listen = true;
    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto& gossip = env.gossiper().local();
        const auto id = gossip.my_host_id();
        auto snapshot = *gossip.get_this_endpoint_state_ptr();
        snapshot.add_application_state(application_state::X_11_PADDING, versioned_value("unseen-before-drain"));
        snapshot.add_application_state(application_state::DRAINING, versioned_value("true"));
        const auto padding = *snapshot.get_application_state_ptr(application_state::X_11_PADDING);
        const auto marker = *snapshot.get_application_state_ptr(application_state::DRAINING);

        // A later fact can arrive before the full snapshot. Its aggregate max
        // must not suppress older, previously unseen application values.
        gossip.add_local_application_state(application_state::LOAD, versioned_value::load(0.75)).get();
        const auto load = *gossip.get_application_state_ptr(id, application_state::LOAD);
        BOOST_REQUIRE_GT(load.version().value(), marker.version().value());

        abort_source as;
        auto& messaging = env.get_messaging_service().local();
        auto send = [&] (endpoint_state state) {
            return ser::gossip_rpc_verbs::send_gossip_prepare_shutdown(&messaging, id,
                    rpc::rpc_clock_type::now() + std::chrono::seconds(10), as, std::move(state));
        };

        auto old_generation = snapshot;
        const auto hb = snapshot.get_heart_beat_state();
        old_generation.set_heart_beat_state_and_update_timestamp(
                heart_beat_state(generation_type(hb.get_generation().value() - 1), hb.get_heart_beat_version()));
        BOOST_REQUIRE_THROW(send(std::move(old_generation)).get(), std::exception);
        BOOST_REQUIRE(!gossip.is_draining(id));

        send(std::move(snapshot)).get();
        const auto applied = gossip.get_this_endpoint_state_ptr();
        check(*applied, application_state::X_11_PADDING, padding.value().linearize(), padding.version().value());
        check(*applied, application_state::DRAINING, marker.value().linearize(), marker.version().value());
        BOOST_REQUIRE_GE(applied->get_application_state_ptr(application_state::LOAD)->version().value(), load.version().value());
        BOOST_REQUIRE(applied->get_heart_beat_state().get_generation() == hb.get_generation());
    }, std::move(cfg));
}

// Regression for #15899: a live state from the previous process must not
// acknowledge startup before this process's generation has reached the peer.
SEASTAR_TEST_CASE(test_up_notification_requires_current_process_generation) {
    cql_test_config cfg;
    cfg.ms_listen = true;
    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto& gossip = env.gossiper().local();
        const auto id = gossip.my_host_id();
        const auto generation = gossip.get_this_endpoint_state_ptr()->get_heart_beat_state().get_generation().value();
        auto& messaging = env.get_messaging_service().local();
        abort_source as;
        auto notify = [&] (int64_t process_generation, std::chrono::milliseconds timeout) {
            return ser::gossip_rpc_verbs::send_gossip_echo(&messaging, id,
                    rpc::rpc_clock_type::now() + timeout, as, process_generation, true);
        };

        // The same host is normal and alive in all cases. Only the exact
        // generation proves it can accept this process's drain request.
        notify(generation, std::chrono::seconds(10)).get();
        BOOST_REQUIRE_THROW(notify(generation + 1, std::chrono::milliseconds(100)).get(), std::exception);
        BOOST_REQUIRE_THROW(notify(generation - 1, std::chrono::milliseconds(100)).get(), std::exception);

        auto pending = notify(generation + 1, std::chrono::seconds(10));
        // Round-trip on the same connection before advancing the state, so
        // the mismatched notification has reached its wait loop.
        notify(generation, std::chrono::seconds(10)).get();
        BOOST_REQUIRE(!pending.available());
        gossip.force_newer_generation();
        pending.get();
    }, std::move(cfg));
}
