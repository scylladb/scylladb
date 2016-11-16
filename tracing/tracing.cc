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
 * Copyright (C) 2016 ScyllaDB
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
#include "tracing/tracing.hh"
#include "utils/class_registrator.hh"
#include "tracing/trace_state.hh"

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

tracing::tracing(const sstring& tracing_backend_helper_class_name)
        : _write_timer([this] { write_timer_callback(); })
        , _thread_name(seastar::format("shard {:d}", engine().cpu_id()))
        , _registrations{
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "dropped_sessions")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, stats.dropped_sessions)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "dropped_records")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, stats.dropped_records)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "trace_records_count")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, stats.trace_records_count)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "trace_errors")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, stats.trace_errors)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "active_sessions")
                    , scollectd::make_typed(scollectd::data_type::GAUGE, _active_sessions)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "cached_records")
                    , scollectd::make_typed(scollectd::data_type::GAUGE, _cached_records)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "pending_for_write_records")
                    , scollectd::make_typed(scollectd::data_type::GAUGE, _pending_for_write_records_count)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "flushing_records")
                    , scollectd::make_typed(scollectd::data_type::GAUGE, _flushing_records))}
        , _gen(std::random_device()())
        , _slow_query_duration_threshold(default_slow_query_duraion_threshold)
        , _slow_query_record_ttl(default_slow_query_record_ttl) {
    try {
        _tracing_backend_helper_ptr = create_object<i_tracing_backend_helper>(tracing_backend_helper_class_name, *this);
    } catch (no_such_class& e) {
        tracing_logger.error("Can't create tracing backend helper {}: not supported", tracing_backend_helper_class_name);
        throw;
    } catch (...) {
        throw;
    }
}

future<> tracing::create_tracing(const sstring& tracing_backend_class_name) {
    return tracing_instance().start(tracing_backend_class_name).then([] {
        return tracing_instance().invoke_on_all([] (tracing& local_tracing) {
            return local_tracing.start();
        });
    });
}

trace_state_ptr tracing::create_session(trace_type type, trace_state_props_set props) {
    trace_state_ptr tstate;
    try {
        // Don't create a session if its records are likely to be dropped
        if (!may_create_new_session()) {
            return trace_state_ptr();
        }

        ++_active_sessions;
        return make_lw_shared<trace_state>(type, props);
    } catch (...) {
        // return an uninitialized state in case of any error (OOM?)
        return trace_state_ptr();
    }
}

trace_state_ptr tracing::create_session(const trace_info& secondary_session_info) {
    try {
        // Don't create a session if its records are likely to be dropped
        if (!may_create_new_session(secondary_session_info.session_id)) {
            return trace_state_ptr();
        }

        ++_active_sessions;
        return make_lw_shared<trace_state>(secondary_session_info);
    } catch (...) {
        // return an uninitialized state in case of any error (OOM?)
        return trace_state_ptr();
    }
}

future<> tracing::start() {
    return _tracing_backend_helper_ptr->start().then([this] {
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
        throw std::invalid_argument("trace probability must be in a [0,1] range");
    }

    _trace_probability = p;
    _normalized_trace_probability = std::llround(_trace_probability * (_gen.max() + 1));

    tracing_logger.info("Setting tracing probability to {} (normalized {})", _trace_probability, _normalized_trace_probability);
}

one_session_records::one_session_records()
    : backend_state_ptr(tracing::get_local_tracing_instance().allocate_backend_session_state())
    , budget_ptr(tracing::get_local_tracing_instance().get_cached_records_ptr()) {}
}

