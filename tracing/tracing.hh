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
#pragma once

#include <vector>
#include <atomic>
#include <random>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/metrics_registration.hh>
#include "gc_clock.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "enum_set.hh"
#include "log.hh"
#include "seastarx.hh"

namespace cql3 { class query_processor; }

namespace tracing {

using elapsed_clock = std::chrono::steady_clock;

extern logging::logger tracing_logger;

class trace_state_ptr;
class tracing;
class backend_registry;

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
inline std::chrono::seconds ttl_by_type(const trace_type t) {
    switch (t) {
    case trace_type::NONE:
    case trace_type::QUERY:
        return std::chrono::seconds(86400);  // 1 day
    case trace_type::REPAIR:
        return std::chrono::seconds(604800); // 7 days
    default:
        // unknown type value - must be a SW bug
        throw std::invalid_argument("unknown trace type: " + std::to_string(int(t)));
    }
}

/**
 * @brief represents an ID of a single tracing span.
 *
 * Currently span ID is a random 64-bit integer.
 */
class span_id {
private:
    uint64_t _id = illegal_id;

public:
    static constexpr uint64_t illegal_id = 0;

public:
    span_id() = default;
    uint64_t get_id() const { return _id; }
    span_id(uint64_t id) : _id(id) {}

    /**
     * @return New span_id with a random legal value
     */
    static span_id make_span_id();
};

std::ostream& operator<<(std::ostream& os, const span_id& id);

// !!!!IMPORTANT!!!!
//
// The enum_set based on this enum is serialized using IDL, therefore new items
// should always be added to the end of this enum - never before the existing
// ones.
//
// Otherwise this may break IDL's backward compatibility.
enum class trace_state_props {
    write_on_close, primary, log_slow_query, full_tracing, ignore_events
};

using trace_state_props_set = enum_set<super_enum<trace_state_props,
    trace_state_props::write_on_close,
    trace_state_props::primary,
    trace_state_props::log_slow_query,
    trace_state_props::full_tracing,
    trace_state_props::ignore_events>>;

class trace_info {
public:
    utils::UUID session_id;
    trace_type type;
    bool write_on_close;
    trace_state_props_set state_props;
    uint32_t slow_query_threshold_us; // in microseconds
    uint32_t slow_query_ttl_sec; // in seconds
    span_id parent_id;

public:
    trace_info(utils::UUID sid, trace_type t, bool w_o_c, trace_state_props_set s_p, uint32_t slow_query_threshold, uint32_t slow_query_ttl, span_id p_id)
        : session_id(std::move(sid))
        , type(t)
        , write_on_close(w_o_c)
        , state_props(s_p)
        , slow_query_threshold_us(slow_query_threshold)
        , slow_query_ttl_sec(slow_query_ttl)
        , parent_id(std::move(p_id))
    {
        state_props.set_if<trace_state_props::write_on_close>(write_on_close);
    }
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
    virtual future<> start(cql3::query_processor& qp) = 0;
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
    elapsed_clock::duration elapsed;
    i_tracing_backend_helper::wall_clock::time_point event_time_point;

    event_record(sstring message_, elapsed_clock::duration elapsed_, i_tracing_backend_helper::wall_clock::time_point event_time_point_)
        : message(std::move(message_))
        , elapsed(elapsed_)
        , event_time_point(event_time_point_) {}
};

struct session_record {
    gms::inet_address client;
    // Keep the containers below sorted since some backends require that and
    // it's very cheap to always do that because the amount of elements in a
    // container is very small.
    std::map<sstring, sstring> parameters;
    std::set<sstring> tables;
    sstring username;
    sstring request;
    size_t request_size = 0;
    size_t response_size = 0;
    std::chrono::system_clock::time_point started_at;
    trace_type command = trace_type::NONE;
    elapsed_clock::duration elapsed;
    std::chrono::seconds slow_query_record_ttl;

private:
    bool _consumed = false;

public:
    session_record()
        : username("<unauthenticated request>")
        , elapsed(-1) {}

    bool ready() const {
        return elapsed.count() >= 0 && !_consumed;
    }

    void set_consumed() {
        _consumed = true;
    }
};

class one_session_records {
private:
    shared_ptr<tracing> _local_tracing_ptr;
public:
    utils::UUID session_id;
    session_record session_rec;
    std::chrono::seconds ttl;
    std::deque<event_record> events_recs;
    std::unique_ptr<backend_session_state_base> backend_state_ptr;
    bool do_log_slow_query = false;

    // A pointer to the records counter of the corresponding state new records
    // of this tracing session should consume from (e.g. "cached" or "pending
    // for write").
    uint64_t* budget_ptr;

    // Each tracing session object represents a single tracing span.
    //
    // Each span has a span ID. In order to be able to build a full tree of all
    // spans of the same query we need a parent span ID as well.
    span_id parent_id;
    span_id my_span_id;

    one_session_records();

    /**
     * Consume a single record from the per-shard budget.
     */
    void consume_from_budget() {
        ++(*budget_ptr);
    }

    /**
     * Drop all pending records and return the budget.
     */
    void drop_records() {
        (*budget_ptr) -= size();
        events_recs.clear();
        session_rec.set_consumed();
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

    static const std::chrono::microseconds default_slow_query_duraion_threshold;
    static const std::chrono::seconds default_slow_query_record_ttl;

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
    // _down becomes FALSE after the local service is fully initialized and
    // tracing records are allowed to be created and collected. It becomes TRUE
    // after the shutdown() call and prevents further write attempts to I/O
    // backend.
    bool _down = true;
    // If _slow_query_logging_enabled is enabled, a query processor keeps all
    // trace events related to the query until in the end it can decide
    // if the query was slow to be saved.
    bool _slow_query_logging_enabled = false;
    // If _ignore_trace_events is enabled, tracing::trace ignores all tracing
    // events as well as creating trace_state descendants with trace_info to
    // track tracing sessions only. This is used to implement lightweight
    // slow query tracing.
    bool _ignore_trace_events = false;
    std::unique_ptr<i_tracing_backend_helper> _tracing_backend_helper_ptr;
    sstring _thread_name;
    const backend_registry& _backend_registry;
    sstring _tracing_backend_helper_class_name;
    seastar::metrics::metric_groups _metrics;
    double _trace_probability = 0.0; // keep this one for querying purposes
    uint64_t _normalized_trace_probability = 0;
    std::ranlux48_base _gen;
    std::chrono::microseconds _slow_query_duration_threshold;
    std::chrono::seconds _slow_query_record_ttl;

public:
    uint64_t get_next_rand_uint64() {
        return _gen();
    }

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

    bool started() const {
        return !_down;
    }

    static future<> create_tracing(const backend_registry& br, sstring tracing_backend_helper_class_name);
    static future<> start_tracing(sharded<cql3::query_processor>& qp);
    static future<> stop_tracing();
    tracing(const backend_registry& br, sstring tracing_backend_helper_class_name);

    // Initialize a tracing backend (e.g. tracing_keyspace or logstash)
    future<> start(cql3::query_processor& qp);

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
     * Create a new primary tracing session.
     *
     * @param type a tracing session type
     * @param props trace session properties set
     *
     * @return tracing state handle
     */
    trace_state_ptr create_session(trace_type type, trace_state_props_set props) noexcept;

    /**
     * Create a new secondary tracing session.
     *
     * @param secondary_session_info tracing session info
     *
     * @return tracing state handle
     */
    trace_state_ptr create_session(const trace_info& secondary_session_info) noexcept;

    void write_maybe() {
        if (_pending_for_write_records_count >= write_event_records_threshold || _pending_for_write_records_bulk.size() >= write_event_sessions_threshold) {
            write_pending_records();
        }
    }

    void end_session() {
        --_active_sessions;
    }

    void write_session_records(lw_shared_ptr<one_session_records> records, bool write_now) {
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

    void set_slow_query_enabled(bool enable = true) {
        _slow_query_logging_enabled = enable;
    }

    bool slow_query_tracing_enabled() const {
        return _slow_query_logging_enabled;
    }

    void set_ignore_trace_events(bool enable = true) {
        _ignore_trace_events = enable;
    }

    bool ignore_trace_events_enabled() const {
        return _ignore_trace_events;
    }

    /**
     * Set the slow query threshold
     *
     * We limit the number of microseconds in the threshold by a maximal unsigned 32-bit
     * integer.
     *
     * If a new threshold value exceeds the above limitation we will override it
     * with the value based on a limit above.
     *
     * @param new_threshold new threshold value
     */
    void set_slow_query_threshold(std::chrono::microseconds new_threshold) {
        if (new_threshold.count() > std::numeric_limits<uint32_t>::max()) {
            _slow_query_duration_threshold = std::chrono::microseconds(std::numeric_limits<uint32_t>::max());
            return;
        }

        _slow_query_duration_threshold = new_threshold;
    }

    std::chrono::microseconds slow_query_threshold() const {
        return _slow_query_duration_threshold;
    }

    /**
     * Set the slow query record TTL
     *
     * We limit the number of seconds in the TTL by a maximal signed 32-bit
     * integer.
     *
     * If a new TTL value exceeds the above limitation we will override it
     * with the value based on a limit above.
     *
     * @param new_ttl new TTL
     */
    void set_slow_query_record_ttl(std::chrono::seconds new_ttl) {
        if (new_ttl.count() > std::numeric_limits<int32_t>::max()) {
            _slow_query_record_ttl = std::chrono::seconds(std::numeric_limits<int32_t>::max());
            return;
        }

        _slow_query_record_ttl = new_ttl;
    }

    std::chrono::seconds slow_query_record_ttl() const {
        return _slow_query_record_ttl;
    }

private:
    void write_timer_callback();

    /**
     * Check if we may create a new tracing session.
     *
     * @return TRUE if conditions are allowing creating a new tracing session
     */
    bool may_create_new_session(const std::optional<utils::UUID>& session_id = std::nullopt) {
        // Don't create a session if its records are likely to be dropped
        if (!have_records_budget(exp_trace_events_per_session) || _active_sessions >= max_pending_sessions + write_event_sessions_threshold) {
            if (session_id) {
                tracing_logger.trace("{}: Too many outstanding tracing records or sessions. Dropping a secondary session", *session_id);
            } else {
                tracing_logger.trace("Too many outstanding tracing records or sessions. Dropping a primary session");
            }

            if (++stats.dropped_sessions % tracing::log_warning_period == 1) {
                tracing_logger.warn("Dropped {} sessions: open_sessions {}, cached_records {} pending_for_write_records {}, flushing_records {}",
                            stats.dropped_sessions, _active_sessions, _cached_records, _pending_for_write_records_count, _flushing_records);
            }

            return false;
        }

        return true;
    }
};

void one_session_records::set_pending_for_write() {
    _is_pending_for_write = true;
    budget_ptr = _local_tracing_ptr->get_pending_records_ptr();
}

void one_session_records::data_consumed() {
    if (session_rec.ready()) {
        session_rec.set_consumed();
    }

    _is_pending_for_write = false;
    budget_ptr = _local_tracing_ptr->get_cached_records_ptr();
}

inline span_id span_id::make_span_id() {
    // make sure the value is always greater than 0
    return 1 + (tracing::get_local_tracing_instance().get_next_rand_uint64() << 1);
}
}
