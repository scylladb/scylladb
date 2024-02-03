/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_test_case.hh"

#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"

#include "test/lib/cql_test_env.hh"

future<> do_with_tracing_env(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in = {}) {
    return do_with_cql_env_thread([func](auto &env) {
        sharded<tracing::tracing>& tracing = tracing::tracing::tracing_instance();
        tracing.start(sstring("trace_keyspace_helper")).get();
        auto stop = defer([&tracing] { tracing.stop().get(); });
        tracing.invoke_on_all(&tracing::tracing::start, std::ref(env.qp()), std::ref(env.migration_manager())).get();
        func(env).get();
    }, std::move(cfg_in));
}

SEASTAR_TEST_CASE(tracing_respect_events) {
    return do_with_tracing_env([](auto &e) {
        tracing::tracing &t = tracing::tracing::get_local_tracing_instance();

        t.set_ignore_trace_events(false);
        BOOST_CHECK(!t.ignore_trace_events_enabled());

        // create session
        tracing::trace_state_props_set trace_props;
        trace_props.set(tracing::trace_state_props::log_slow_query);
        trace_props.set(tracing::trace_state_props::full_tracing);

        tracing::trace_state_ptr trace_state1 = t.create_session(tracing::trace_type::QUERY, trace_props);
        tracing::begin(trace_state1, "begin", gms::inet_address());

        tracing::trace(trace_state1, "trace 1");
        BOOST_CHECK_EQUAL(trace_state1->events_size(), 1);
        BOOST_CHECK(tracing::make_trace_info(trace_state1) != std::nullopt);

        // disable tracing events, it must be ignored for full tracing
        t.set_ignore_trace_events(true);
        BOOST_CHECK(t.ignore_trace_events_enabled());

        tracing::trace_state_ptr trace_state2 = t.create_session(tracing::trace_type::QUERY, trace_props);
        tracing::begin(trace_state2, "begin", gms::inet_address());

        tracing::trace(trace_state2, "trace 1");
        BOOST_CHECK_EQUAL(trace_state2->events_size(), 1);
        BOOST_CHECK(tracing::make_trace_info(trace_state2) != std::nullopt);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(tracing_slow_query_fast_mode) {
    return do_with_tracing_env([](auto &e) {
        tracing::tracing &t = tracing::tracing::get_local_tracing_instance();

        // disable tracing events
        t.set_ignore_trace_events(true);
        BOOST_CHECK(t.ignore_trace_events_enabled());

        // create session
        tracing::trace_state_props_set trace_props;
        trace_props.set(tracing::trace_state_props::log_slow_query);

        tracing::trace_state_ptr trace_state = t.create_session(tracing::trace_type::QUERY, trace_props);
        tracing::begin(trace_state, "begin", gms::inet_address());

        // check no event created
        tracing::trace(trace_state, "trace 1");
        BOOST_CHECK_EQUAL(trace_state->events_size(), 0);
        BOOST_CHECK(tracing::make_trace_info(trace_state) == std::nullopt);

        return make_ready_future<>();
    });
}
