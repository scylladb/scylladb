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

static logging::logger logger("tracing");

std::vector<sstring> trace_type_names = {
    "NONE",
    "QUERY",
    "REPAIR"
};

tracing::tracing(const sstring& tracing_backend_helper_class_name)
        : _thread_name(to_sstring(engine().cpu_id()))
        , _registrations{
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "max_sessions_threshold_hits")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, stats.max_sessions_threshold_hits)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "max_traces_threshold_hits")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, stats.max_traces_threshold_hits)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "trace_events_count")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, stats.trace_events_count)),
            scollectd::add_polled_metric(scollectd::type_instance_id("tracing"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "pending_sessions")
                    , scollectd::make_typed(scollectd::data_type::GAUGE, _pending_sessions))} {
    try {
        _tracing_backend_helper_ptr = create_object<i_tracing_backend_helper>(tracing_backend_helper_class_name);
    } catch (no_such_class& e) {
        logger.error("Can't create tracing backend helper {}: not supported", tracing_backend_helper_class_name);
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

trace_state_ptr tracing::create_session(trace_type type, bool flush_on_close, const std::experimental::optional<utils::UUID>& session_id) {
    trace_state_ptr tstate;
    try {
        if (_pending_sessions > max_pending_sessions) {
            if (session_id) {
                logger.trace("{}: Maximum sessions count is reached. Dropping a secondary session", session_id);
            } else {
                logger.trace("Maximum sessions count is reached. Dropping a primary session");
            }

            if (++stats.max_sessions_threshold_hits % tracing::max_threshold_hits_warning_period == 1) {
                logger.warn("Maximum sessions limit is hit {} times", stats.max_sessions_threshold_hits);
            }

            return trace_state_ptr();
        }
        ++_pending_sessions;
        return make_lw_shared<trace_state>(type, flush_on_close, session_id);
    } catch (...) {
        // return an uninitialized state in case of any error (OOM?)
        return trace_state_ptr();
    }
}

future<> tracing::stop() {
    logger.info("Asked to stop");
    return _tracing_backend_helper_ptr->stop().then([] {
        logger.info("Tracing is down");
    });
}
}

