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

#include <vector>
#include <atomic>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include "gc_clock.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"

namespace tracing {

class trace_state;

enum class trace_type : uint8_t {
    NONE,
    QUERY,
    REPAIR,
};

extern std::vector<sstring> trace_type_names;

inline const sstring& type_to_string(trace_type t) {
    return trace_type_names.at(static_cast<int>(t));
}

/**
 * Returns a TTL for a given trace type
 * @param t trace type
 *
 * @return TTL
 */
inline gc_clock::duration ttl_by_type(const trace_type t) {
    switch (t) {
    case trace_type::NONE:
    case trace_type::QUERY:
        return gc_clock::duration(86400);  // 1 day
    case trace_type::REPAIR:
        return gc_clock::duration(604800); // 7 days
    default:
        // unknown type value - must be a SW bug
        throw std::invalid_argument("unknown trace type: " + std::to_string(int(t)));
    }
}

class trace_info {
public:
    utils::UUID session_id;
    trace_type type;
    bool flush_on_close;

public:
    trace_info(utils::UUID sid, trace_type t, bool f_o_c)
        : session_id(std::move(sid))
        , type(t)
        , flush_on_close(f_o_c)
    { }
};

struct i_tracing_backend_helper {
    virtual ~i_tracing_backend_helper() {}
    virtual future<> start() = 0;
    virtual future<> stop() = 0;

    /**
     * Store a new tracing session record
     *
     * @param session_id tracing session ID
     * @param client client IP
     * @param parameters optional parameters
     * @param request request we are tracing
     * @param started_at amount of milliseconds passed since Epoch before this
     *                   session is started (on a Coordinator Node)
     * @param command a type of this trace
     * @param elapsed number of microseconds this tracing session took
     * @param ttl TTL of a session record
     */
    virtual void store_session_record(const utils::UUID& session_id,
                                      gms::inet_address client,
                                      std::unordered_map<sstring, sstring> parameters,
                                      sstring request,
                                      long started_at,
                                      trace_type command,
                                      int elapsed,
                                      gc_clock::duration ttl) = 0;

    /**
     * Store a new tracing event record
     * @param session_id tracing session ID
     * @param message tracing message
     * @param elapsed number of microseconds passed since a beginning of a
     *                corresponding tracing session till this event
     * @param ttl TTL of the event record
     */
    virtual void store_event_record(const utils::UUID& session_id,
                                    sstring message,
                                    int elapsed,
                                    gc_clock::duration ttl) = 0;

    /**
     * Commit all pending tracing records to the underlying storage.
     */
    virtual void flush() = 0;
};

using trace_state_ptr = lw_shared_ptr<trace_state>;

class tracing {
public:
    static constexpr int max_pending_sessions = 100;
    static constexpr int max_trace_events_per_session = 30;
    // Number of max threshold XXX hits when an info message is printed
    static constexpr int max_threshold_hits_warning_period = 10000;

    struct stats {
        uint64_t max_sessions_threshold_hits = 0;
        uint64_t max_traces_threshold_hits = 0;
        uint64_t trace_events_count = 0;
    } stats;

private:
    uint64_t _pending_sessions = 0;
    std::unique_ptr<i_tracing_backend_helper> _tracing_backend_helper_ptr;
    sstring _thread_name;
    scollectd::registrations _registrations;

public:
    i_tracing_backend_helper& backend_helper() {
        return *_tracing_backend_helper_ptr;
    }

    const sstring& get_thread_name() const {
        return _thread_name;
    }

    static seastar::sharded<tracing>& tracing_instance() {
        // FIXME: leaked intentionally to avoid shutdown problems, see #293
        static seastar::sharded<tracing>* tracing_inst = new seastar::sharded<tracing>();

        return *tracing_inst;
    }

    static tracing& get_local_tracing_instance() {
        return tracing_instance().local();
    }

    static future<> create_tracing(const sstring& tracing_backend_helper_class_name);
    tracing(const sstring& tracing_backend_helper_class_name);

    // Initialize a tracing backend (e.g. tracing_keyspace or logstash)
    future<> start() { return _tracing_backend_helper_ptr->start(); }

    // waits until all active tracing sessions are over.
    future<> stop();

    /**
     * Create a new tracing session.
     *
     * @param type a tracing session type
     * @param flush_on_close flush a backend before closing the session
     * @param session_id a session ID to create a (secondary) session with
     *
     * @return tracing state handle
     */
    trace_state_ptr create_session(trace_type type, bool flush_on_close, const std::experimental::optional<utils::UUID>& session_id = std::experimental::nullopt);

    void end_session() {
        --_pending_sessions;
    }
};
}
