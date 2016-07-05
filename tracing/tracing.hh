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
#include <random>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include "gc_clock.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"

namespace tracing {

class trace_state;
class tracing;

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
    bool write_on_close;

public:
    trace_info(utils::UUID sid, trace_type t, bool w_o_c)
        : session_id(std::move(sid))
        , type(t)
        , write_on_close(w_o_c)
    { }
};

struct i_tracing_backend_helper {
    using wall_clock = std::chrono::system_clock;

protected:
    tracing& _local_tracing;

public:
    i_tracing_backend_helper(tracing& tr) : _local_tracing(tr) {}
    virtual ~i_tracing_backend_helper() {}
    virtual future<> start() = 0;
    virtual future<> stop() = 0;

    /**
     * Write a new tracing session record
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
    virtual void write_session_record(const utils::UUID& session_id,
                                      gms::inet_address client,
                                      std::unordered_map<sstring, sstring> parameters,
                                      sstring request,
                                      long started_at,
                                      trace_type command,
                                      int elapsed,
                                      gc_clock::duration ttl) = 0;

    /**
     * Write a new tracing event record
     * @param session_id tracing session ID
     * @param message tracing message
     * @param elapsed number of microseconds passed since a beginning of a
     *                corresponding tracing session till this event
     * @param ttl TTL of the event record
     * @param event_time_point time point when a record was taken
     */
    virtual void write_event_record(const utils::UUID& session_id,
                                    sstring message,
                                    int elapsed,
                                    gc_clock::duration ttl,
                                    wall_clock::time_point event_time_point) = 0;

private:
    /**
     * Commit all pending tracing records to the underlying storage.
     * The implementation has to call tracing::tracing::write_complete(nr) for
     * each "nr" completed records once they are written to the backend.
     */
    virtual void kick() = 0;

    friend class tracing;
};

using trace_state_ptr = lw_shared_ptr<trace_state>;

class tracing : public seastar::async_sharded_service<tracing> {
public:
    static const gc_clock::duration write_period;
    static constexpr int max_pending_for_write_sessions = 1000;
    static constexpr int max_trace_events_per_session = 30;
    // Number of max threshold XXX hits when an info message is printed
    static constexpr int max_threshold_hits_warning_period = 10000;

    struct stats {
        uint64_t max_sessions_threshold_hits = 0;
        uint64_t max_traces_threshold_hits = 0;
        uint64_t trace_events_count = 0;
        uint64_t trace_errors = 0;
    } stats;

private:
    uint64_t _active_sessions = 0;
    uint64_t _pending_for_write_sessions = 0;
    uint64_t _flushing_sessions = 0;
    timer<lowres_clock> _write_timer;
    bool _down = false;
    std::unique_ptr<i_tracing_backend_helper> _tracing_backend_helper_ptr;
    sstring _thread_name;
    scollectd::registrations _registrations;
    double _trace_probability = 0.0; // keep this one for querying purposes
    uint64_t _normalized_trace_probability = 0;
    std::ranlux48_base _gen;

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
    future<> start();

    future<> stop();

    /**
     * Waits until all pending tracing records are flushed to the backend an
     * shuts down the backend. The following calls to
     * write_session_record()/write_event_record() methods of a backend instance
     * should be a NOOP.
     *
     * @return a ready future when the shutdown is complete
     */
    future<> shutdown();

    void write_pending_records() {
        // if service is down - do nothing
        if (_down) {
            return;
        }

        _flushing_sessions += _pending_for_write_sessions;
        _pending_for_write_sessions = 0;
        _tracing_backend_helper_ptr->kick();
    }

    void write_complete(uint64_t nr = 1) {
        if (nr > _flushing_sessions) {
            throw std::logic_error("completing more sessions than there are pending");
        }
        _flushing_sessions -= nr;
    }

    /**
     * Create a new tracing session.
     *
     * @param type a tracing session type
     * @param write_on_close flush a backend before closing the session
     * @param session_id a session ID to create a (secondary) session with
     *
     * @return tracing state handle
     */
    trace_state_ptr create_session(trace_type type, bool write_on_close, const std::experimental::optional<utils::UUID>& session_id = std::experimental::nullopt);

    void end_session() {
        --_active_sessions;
        ++_pending_for_write_sessions;
        if (_pending_for_write_sessions >= max_pending_for_write_sessions) {
            write_pending_records();
        }
    }

    /**
     * Sets a probability for tracing a CQL request.
     *
     * @param p a new tracing probability - a floating point value in a [0,1]
     *          range. It would effectively define a portion of CQL requests
     *          initiated on the current Node that will be traced.
     * @throw std::invalid_argument if @ref p is out of range
     */
    void set_trace_probability(double p);
    double get_trace_probability() const {
        return _trace_probability;
    }

    bool trace_next_query() {
        return _normalized_trace_probability != 0 && _gen() < _normalized_trace_probability;
    }

private:
    void write_timer_callback();
};
}
