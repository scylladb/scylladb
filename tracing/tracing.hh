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

extern logging::logger tracing_logger;

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

struct one_session_records;
using records_bulk = std::deque<lw_shared_ptr<one_session_records>>;

struct backend_session_state_base {
    virtual ~backend_session_state_base() {};
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
     * Write a bulk of tracing records.
     *
     * This function has to clear a scheduled state of each one_session_records object
     * in the @param bulk after it has been actually passed to the backend for writing.
     *
     * @param bulk a bulk of records
     */
    virtual void write_records_bulk(records_bulk& bulk) = 0;

    virtual std::unique_ptr<backend_session_state_base> allocate_session_state() const = 0;

private:
    friend class tracing;
};

struct event_record {
    sstring message;
    int elapsed;
    i_tracing_backend_helper::wall_clock::time_point event_time_point;

    event_record(sstring message_, int elapsed_, i_tracing_backend_helper::wall_clock::time_point event_time_point_)
        : message(std::move(message_))
        , elapsed(elapsed_)
        , event_time_point(event_time_point_) {}
};

struct session_record {
    gms::inet_address client;
    std::unordered_map<sstring, sstring> parameters;
    sstring request;
    long started_at = 0;
    trace_type command = trace_type::NONE;
    int elapsed = -1;

    bool ready() const {
        return elapsed >= 0;
    }
};

class one_session_records {
public:
    utils::UUID session_id;
    session_record session_rec;
    gc_clock::duration ttl;
    std::deque<event_record> events_recs;
    std::unique_ptr<backend_session_state_base> backend_state_ptr;

    // A pointer to the records counter of the corresponding state new records
    // of this tracing session should consume from (e.g. "cached" or "pending
    // for write").
    uint64_t* budget_ptr;

    one_session_records();

    /**
     * Consume a single record from the per-shard budget.
     */
    void consume_from_budget() {
        ++(*budget_ptr);
    }

    /**
     * Should be called when a record is scheduled for write.
     * From that point till data_consumed() call all new records will be written
     * in the next write event.
     */
    inline void set_pending_for_write();

    /**
     * Should be called after all data pending to be written in this record has
     * been processed.
     * From that point on new records are cached internally and have to be
     * explicitly committed for write in order to be written during the write event.
     */
    inline void data_consumed();

    bool is_pending_for_write() const {
        return _is_pending_for_write;
    }

    uint64_t size() const {
        return events_recs.size() + session_rec.ready();
    }

private:
    bool _is_pending_for_write = false;
};

using trace_state_ptr = lw_shared_ptr<trace_state>;

class tracing : public seastar::async_sharded_service<tracing> {
public:
    static const gc_clock::duration write_period;
    // maximum number of sessions pending for write per shard
    static constexpr int max_pending_sessions = 1000;
    // expectation of an average number of trace records per session
    static constexpr int exp_trace_events_per_session = 10;
    // maximum allowed pending records per-shard
    static constexpr int max_pending_trace_records = max_pending_sessions * exp_trace_events_per_session;
    // number of pending sessions that would trigger a write event
    static constexpr int write_event_sessions_threshold = 100;
    // number of pending records that would trigger a write event
    static constexpr int write_event_records_threshold = write_event_sessions_threshold * exp_trace_events_per_session;
    // Number of events when an info message is printed
    static constexpr int log_warning_period = 10000;

    struct stats {
        uint64_t dropped_sessions = 0;
        uint64_t dropped_records = 0;
        uint64_t trace_records_count = 0;
        uint64_t trace_errors = 0;
    } stats;

private:
    // A number of currently active tracing sessions
    uint64_t _active_sessions = 0;

    // Below are 3 counters that describe the total amount of tracing records on
    // this shard. Each counter describes a state in which a record may be.
    //
    // Each record may only be in a specific state at every point of time and
    // thereby it must be accounted only in one and only one of the three
    // counters below at any given time.
    //
    // The sum of all three counters should not be greater than
    // (max_pending_trace_records + write_event_records_threshold) at any time
    // (actually it can get as high as a value above plus (max_pending_sessions)
    // if all sessions are primary but we won't take this into an account for
    // simplicity).
    //
    // The same is about the number of outstanding sessions: it may not be
    // greater than (max_pending_sessions + write_event_sessions_threshold) at
    // any time.
    //
    // If total number of tracing records is greater or equal to the limit
    // above, the new trace point is going to be dropped.
    //
    // If current number or records plus the expected number of trace records
    // per session (exp_trace_events_per_session) is greater than the limit
    // above new sessions will be dropped. A new session will also be dropped if
    // there are too many active sessions.
    //
    // When the record or a session is dropped the appropriate statistics
    // counters are updated and there is a rate-limited warning message printed
    // to the log.
    //
    // Every time a number of records pending for write is greater or equal to
    // (write_event_records_threshold) or a number of sessions pending for
    // write is greater or equal to (write_event_sessions_threshold) a write
    // event is issued.
    //
    // Every 2 seconds a timer would write all pending for write records
    // available so far.

    // Total number of records cached in the active sessions that are not going
    // to be written in the next write event
    uint64_t _cached_records = 0;
    // Total number of records that are currently being written to I/O
    uint64_t _flushing_records = 0;
    // Total number of records in the _pending_for_write_records_bulk. All of
    // them are going to be written to the I/O during the next write event.
    uint64_t _pending_for_write_records_count = 0;

    records_bulk _pending_for_write_records_bulk;
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
        if (_pending_for_write_records_bulk.size()) {
            _flushing_records += _pending_for_write_records_count;
            stats.trace_records_count += _pending_for_write_records_count;
            _pending_for_write_records_count = 0;
            _tracing_backend_helper_ptr->write_records_bulk(_pending_for_write_records_bulk);
            _pending_for_write_records_bulk.clear();
        }
    }

    void write_complete(uint64_t nr = 1) {
        if (nr > _flushing_records) {
            throw std::logic_error(seastar::format("completing more records ({:d}) than there are pending ({:d})", nr, _flushing_records));
        }
        _flushing_records -= nr;
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

    void write_maybe() {
        if (_pending_for_write_records_count >= write_event_records_threshold || _pending_for_write_records_bulk.size() >= write_event_sessions_threshold) {
            write_pending_records();
        }
    }

    void end_session(lw_shared_ptr<one_session_records> records, bool write_now) {
        --_active_sessions;

        // if service is down - drop the records and return
        if (_down) {
            return;
        }

        try {
            schedule_for_write(std::move(records));
        } catch (...) {
            // OOM: bump up the error counter and ignore
            ++stats.trace_errors;
            return;
        }

        if (write_now) {
            write_pending_records();
        } else {
            write_maybe();
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

    std::unique_ptr<backend_session_state_base> allocate_backend_session_state() const {
        return _tracing_backend_helper_ptr->allocate_session_state();
    }

    /**
     * Checks if there is enough budget for the @param nr new records
     * @param nr number of new records
     *
     * @return TRUE if there is enough budget, FLASE otherwise
     */
    bool have_records_budget(uint64_t nr = 1) {
        // We don't want the total amount of pending, active and flushing records to
        // bypass the maximum number of pending records plus the number of
        // records that are possibly being written write now.
        //
        // If either records are being created too fast or a backend doesn't
        // keep up we want to start dropping records.
        // In any case, this should be rare.
        if (_pending_for_write_records_count + _cached_records + _flushing_records + nr > max_pending_trace_records + write_event_records_threshold) {
            return false;
        }

        return true;
    }

    uint64_t* get_pending_records_ptr() {
        return &_pending_for_write_records_count;
    }

    uint64_t* get_cached_records_ptr() {
        return &_cached_records;
    }

    void schedule_for_write(lw_shared_ptr<one_session_records> records) {
        if (records->is_pending_for_write()) {
            return;
        }

        _pending_for_write_records_bulk.emplace_back(records);
        records->set_pending_for_write();

        // move the current records from a "cached" to "pending for write" state
        auto current_records_num = records->size();
        _cached_records -= current_records_num;
        _pending_for_write_records_count += current_records_num;
    }

private:
    void write_timer_callback();
};

void one_session_records::set_pending_for_write() {
    _is_pending_for_write = true;
    budget_ptr = tracing::get_local_tracing_instance().get_pending_records_ptr();
}

void one_session_records::data_consumed() {
    _is_pending_for_write = false;
    budget_ptr = tracing::get_local_tracing_instance().get_cached_records_ptr();
}
}
