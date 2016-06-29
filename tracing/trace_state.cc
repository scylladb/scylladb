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
#include <chrono>
#include "tracing/trace_state.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "service/storage_proxy.hh"
#include "to_string.hh"
#include "timestamp.hh"

namespace tracing {

static logging::logger logger("trace_state");

std::unordered_map<sstring, sstring> trace_state::get_params() {
    if (!_params_ptr) {
        return {};
    }

    std::unordered_map<sstring, sstring> params_map;
    params_values& vals = *_params_ptr;

    if (vals.batchlog_endpoints) {
        params_map.emplace("batch_endpoints", join(sstring(","),  *vals.batchlog_endpoints | boost::adaptors::transformed([](gms::inet_address ep) {return seastar::format("/{}", ep);})));
    }

    if (vals.cl) {
        params_map.emplace("consistency_level", seastar::format("{}", *vals.cl));
    }

    if (vals.serial_cl) {
        params_map.emplace("serial_consistency_level", seastar::format("{}", *vals.serial_cl));
    }

    if (vals.page_size) {
        params_map.emplace("page_size", seastar::format("{:d}", *vals.page_size));
    }

    if (vals.query) {
        params_map.emplace("query", *vals.query);
    }

    if (vals.user_timestamp) {
        params_map.emplace("user_timestamp", seastar::format("{:d}", *vals.user_timestamp));
    }

    return params_map;
}

trace_state::~trace_state() {
    if (_tracing_began) {
        if (_primary) {
            // We don't account the session_record event when checking a limit
            // of maximum events per session because there may be only one such
            // event and we don't want to cripple the primary session by
            // "stealing" one trace() event from it.
            //
            // We do want to report it in statistics however. If for instance
            // there are a lot of tracing sessions that only open itself and
            // then do nothing - they will create a lot of session_record events
            // and we do want to know about it.
            ++_pending_trace_events;
            _local_backend.write_session_record(_session_id, _client, get_params(), std::move(_request), _started_at, _type, elapsed(), _ttl);
        }

        _local_tracing_ptr->end_session();

        if (_write_on_close) {
            _local_tracing_ptr->write_pending_records();
        }

        // update some stats and get out...
        auto& tracing_stats = _local_tracing_ptr->stats;

        tracing_stats.trace_events_count += _pending_trace_events;

        if (_pending_trace_events >= tracing::max_trace_events_per_session) {
            logger.trace("{}: Maximum number of traces is reached. Some traces are going to be dropped", _session_id);

            if (++tracing_stats.max_traces_threshold_hits % tracing::max_threshold_hits_warning_period == 1) {
                logger.warn("Maximum traces per session limit is hit {} times", tracing_stats.max_traces_threshold_hits);
            }
        }
    }
}
}
