/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/testing/test_case.hh>

#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"
#include "tracing/tracing_backend_registry.hh"
#include "utils/class_registrator.hh"

#include "test/lib/cql_test_env.hh"

future<> do_with_tracing_env(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in = {}) {
    return do_with_cql_env([func](auto &env) {
        // supervisor::notify("creating tracing");
        tracing::backend_registry tracing_backend_registry;
        tracing::register_tracing_keyspace_backend(tracing_backend_registry);
        tracing::tracing::create_tracing(tracing_backend_registry, "trace_keyspace_helper").get();

        // supervisor::notify("starting tracing");
        tracing::tracing::start_tracing(env.qp()).get();

        return do_with(std::move(tracing_backend_registry), [func, &env](auto &reg) {
            return func(env).finally([]() {
                return tracing::tracing::tracing_instance().invoke_on_all([](tracing::tracing &local_tracing) {
                    return local_tracing.shutdown();
                }).finally([]() {
                    return tracing::tracing::tracing_instance().stop();
                });
            });
        });
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
