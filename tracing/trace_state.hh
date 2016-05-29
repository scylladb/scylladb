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
#pragma once

#include <deque>
#include "mutation.hh"
#include "utils/UUID_gen.hh"
#include "tracing/tracing.hh"

namespace tracing {

class trace_state final {
    using clock_type = std::chrono::steady_clock;

private:
    utils::UUID _session_id;
    trace_type _type;
    bool _flush_on_close;
    // Used for calculation of time passed since the beginning of a tracing
    // session till each tracing event.
    clock_type::time_point _start;
    gc_clock::duration _ttl;
    // TRUE for a primary trace_state object
    bool _primary;
    bool _tracing_began = false;
    std::chrono::system_clock::rep _started_at;
    gms::inet_address _client;
    sstring _request;
    std::unordered_map<sstring, sstring> _params;
    int _pending_trace_events = 0;

public:
    trace_state(trace_type type, bool flush_on_close, const std::experimental::optional<utils::UUID>& session_id = std::experimental::nullopt)
        : _session_id(session_id ? *session_id : utils::UUID_gen::get_time_UUID())
        , _type(type)
        , _flush_on_close(flush_on_close)
        , _ttl(ttl_by_type(_type))
        , _primary(!session_id)
    { }

    ~trace_state();

    const utils::UUID& get_session_id() const {
        return _session_id;
    }

    trace_type get_type() const {
        return _type;
    }

    bool get_flush_on_close() const {
        return _flush_on_close;
    }

private:
    /**
     * Returns the number of microseconds passed since the beginning of this
     * tracing session.
     *
     * @return number of microseconds passed since the beginning of this session
     */
    inline int elapsed();

    void begin() {
        std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
        _start = clock_type::now();
        std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
        _tracing_began = true;
    }

    void begin(sstring request, gms::inet_address client, std::unordered_map<sstring, sstring> params) {
        begin();
        _started_at = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        _request = std::move(request);
        _client = std::move(client);
        _params = std::move(params);
    }

    inline void trace(sstring message);

    template <typename... A>
    friend void begin(trace_state_ptr& p, A&&... a);

    template <typename... A>
    friend void trace(trace_state_ptr& p, A&&... a);
};

void trace_state::trace(sstring message) {
    if (!_tracing_began) {
        throw std::logic_error("trying to use a trace() before begin() for \"" + message + "\" tracepoint");
    }

    if (_pending_trace_events >= tracing::max_trace_events_per_session) {
        return;
    }

    ++_pending_trace_events;
    tracing::get_local_tracing_instance().backend_helper().store_event_record(_session_id, std::move(message), elapsed(), _ttl);
}

int trace_state::elapsed() {
    using namespace std::chrono;
    std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
    auto elapsed = duration_cast<microseconds>(clock_type::now() - _start).count();
    std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);

    if (elapsed > std::numeric_limits<int>::max()) {
        return std::numeric_limits<int>::max();
    }

    return elapsed;
}

template <typename... A>
void begin(trace_state_ptr& p, A&&... a) {
    if (p) {
        p->begin(std::forward<A>(a)...);
    }
}

template <typename... A>
void trace(trace_state_ptr& p, A&&... a) {
    if (p) {
        p->trace(std::forward<A>(a)...);
    }
}
}
