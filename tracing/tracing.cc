/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
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
#include <seastar/core/metrics.hh>
#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"
#include "tracing/tracing_backend_registry.hh"

namespace tracing {

logging::logger tracing_logger("tracing");
const gc_clock::duration tracing::tracing::write_period = std::chrono::seconds(2);
const std::chrono::seconds tracing::tracing::default_slow_query_record_ttl = std::chrono::seconds(86400);
const std::chrono::microseconds tracing::tracing::default_slow_query_duraion_threshold = std::chrono::milliseconds(500);

std::vector<sstring> trace_type_names = {
    "NONE",
    "QUERY",
    "REPAIR"
};

tracing::tracing(const backend_registry& br, sstring tracing_backend_helper_class_name)
        : _write_timer([this] { write_timer_callback(); })
        , _thread_name(seastar::format("shard {:d}", this_shard_id()))
        , _backend_registry(br)
        , _tracing_backend_helper_class_name(std::move(tracing_backend_helper_class_name))
        , _gen(std::random_device()())
        , _slow_query_duration_threshold(default_slow_query_duraion_threshold)
        , _slow_query_record_ttl(default_slow_query_record_ttl) {
    namespace sm = seastar::metrics;

    _metrics.add_group("tracing", {
        sm::make_derive("dropped_sessions", stats.dropped_sessions,
                        sm::description("Counts a number of dropped sessions due to too many pending sessions/records. "
                                        "High value indicates that backend is saturated with the rate with which new tracing records are created.")),

        sm::make_derive("dropped_records", stats.dropped_records,
                        sm::description("Counts a number of dropped records due to too many pending records. "
                                        "High value indicates that backend is saturated with the rate with which new tracing records are created.")),

        sm::make_derive("trace_records_count", stats.trace_records_count,
                        sm::description("This metric is a rate of tracing records generation.")),

        sm::make_derive("trace_errors", stats.trace_errors,
                        sm::description("Counts a number of trace records dropped due to an error (e.g. OOM).")),

        sm::make_gauge("active_sessions", _active_sessions,
                        sm::description("Holds a number of a currently active tracing sessions.")),

        sm::make_gauge("cached_records", _cached_records,
                        sm::description(seastar::format("Holds a number of tracing records cached in the tracing sessions that are not going to be written in the next write event. "
                                                        "If sum of this metric, pending_for_write_records and flushing_records is close to {} we are likely to start dropping tracing records.", max_pending_trace_records + write_event_records_threshold))),

        sm::make_gauge("pending_for_write_records", _pending_for_write_records_count,
                        sm::description(seastar::format("Holds a number of tracing records that are going to be written in the next write event. "
                                                        "If sum of this metric, cached_records and flushing_records is close to {} we are likely to start dropping tracing records.", max_pending_trace_records + write_event_records_threshold))),

        sm::make_gauge("flushing_records", _flushing_records,
                        sm::description(seastar::format("Holds a number of tracing records that currently being written to the I/O backend. "
                                                        "If sum of this metric, cached_records and pending_for_write_records is close to {} we are likely to start dropping tracing records.", max_pending_trace_records + write_event_records_threshold))),
    });
}

future<> tracing::create_tracing(const backend_registry& br, sstring tracing_backend_class_name) {
    return tracing_instance().start(std::ref(br), std::move(tracing_backend_class_name));
}

future<> tracing::start_tracing(sharded<cql3::query_processor>& qp) {
    return tracing_instance().invoke_on_all([&qp] (tracing& local_tracing) {
        return local_tracing.start(qp.local());
    });
}

future<> tracing::stop_tracing() {
    return tracing_instance().invoke_on_all([] (tracing& local_tracing) {
        // It might have been shut down while draining
        return local_tracing._down ? make_ready_future<>() : local_tracing.shutdown();
    });
}

trace_state_ptr tracing::create_session(trace_type type, trace_state_props_set props) noexcept {
    if (!started()) {
        return nullptr;
    }

    try {
        // Don't create a session if its records are likely to be dropped
        if (!may_create_new_session()) {
            return nullptr;
        }

        // Ignore events if they are disabled and this is not a full tracing request (probability tracing or user request)
        props.set_if<trace_state_props::ignore_events>(!props.contains<trace_state_props::full_tracing>() && ignore_trace_events_enabled());

        ++_active_sessions;
        return make_lw_shared<trace_state>(type, props);
    } catch (...) {
        // return an uninitialized state in case of any error (OOM?)
        return nullptr;
    }
}

trace_state_ptr tracing::create_session(const trace_info& secondary_session_info) noexcept {
    if (!started()) {
        return nullptr;
    }

    try {
        // Don't create a session if its records are likely to be dropped
        if (!may_create_new_session(secondary_session_info.session_id)) {
            return nullptr;
        }

        ++_active_sessions;
        return make_lw_shared<trace_state>(secondary_session_info);
    } catch (...) {
        // return an uninitialized state in case of any error (OOM?)
        return nullptr;
    }
}

future<> tracing::start(cql3::query_processor& qp) {
    try {
        _tracing_backend_helper_ptr = _backend_registry.create_backend(_tracing_backend_helper_class_name, *this);
    } catch (no_such_tracing_backend& e) {
        tracing_logger.error("Can't create tracing backend helper {}: not supported", _tracing_backend_helper_class_name);
        throw;
    } catch (...) {
        throw;
    }

    return _tracing_backend_helper_ptr->start(qp).then([this] {
        _down = false;
        _write_timer.arm(write_period);
    });
}

void tracing::write_timer_callback() {
    if (_down) {
        return;
    }

    tracing_logger.trace("Timer kicks in: {}", _pending_for_write_records_bulk.size() ? "writing" : "not writing");
    write_pending_records();
    _write_timer.arm(write_period);
}

future<> tracing::shutdown() {
    tracing_logger.info("Asked to shut down");
    if (_down) {
        throw std::logic_error("tracing: shutdown() called for the service that is already down");
    }

    write_pending_records();
    _down = true;
    _write_timer.cancel();
    return _tracing_backend_helper_ptr->stop().then([] {
        tracing_logger.info("Tracing is down");
    });
}

future<> tracing::stop() {
    if (!_down) {
        throw std::logic_error("tracing: stop() called before shutdown()");
    }

    return make_ready_future<>();
}

void tracing::set_trace_probability(double p) {
    if (p < 0 || p > 1) {
        throw std::out_of_range("trace probability must be in a [0,1] range");
    }

    _trace_probability = p;
    _normalized_trace_probability = std::llround(_trace_probability * (_gen.max() + 1));

    tracing_logger.info("Setting tracing probability to {} (normalized {})", _trace_probability, _normalized_trace_probability);
}

one_session_records::one_session_records()
    : _local_tracing_ptr(tracing::get_local_tracing_instance().shared_from_this())
    , backend_state_ptr(_local_tracing_ptr->allocate_backend_session_state())
    , budget_ptr(_local_tracing_ptr->get_cached_records_ptr()) {}

std::ostream& operator<<(std::ostream& os, const span_id& id) {
    return os << id.get_id();
}
}

