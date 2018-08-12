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
#include <unordered_set>
#include <seastar/util/lazy.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/checked_ptr.hh>
#include "mutation.hh"
#include "utils/UUID_gen.hh"
#include "tracing/tracing.hh"
#include "gms/inet_address.hh"
#include "auth/authenticated_user.hh"

namespace cql3{
class query_options;
struct raw_value_view;

namespace statements {
class prepared_statement;
}
}

namespace tracing {

extern logging::logger trace_state_logger;

using prepared_checked_weak_ptr = seastar::checked_ptr<seastar::weak_ptr<cql3::statements::prepared_statement>>;

class trace_state final {
public:
    // A primary session may be in 3 states:
    //   - "inactive": between the creation and a begin() call.
    //   - "foreground": after a begin() call and before a
    //     stop_foreground_and_write() call.
    //   - "background": after a stop_foreground_and_write() call and till the
    //     state object is destroyed.
    //
    // - Traces are not allowed while state is in an "inactive" state.
    // - The time the primary session was in a "foreground" state is the time
    //   reported as a session's "duration".
    // - Traces that have arrived during the "background" state will be recorded
    //   as usual but their "elapsed" time will be greater or equal to the
    //   session's "duration".
    //
    // Secondary sessions may only be in an "inactive" or in a "foreground"
    // states.
    enum class state {
        inactive,
        foreground,
        background
    };

private:
    lw_shared_ptr<one_session_records> _records;
    // Used for calculation of time passed since the beginning of a tracing
    // session till each tracing event.
    elapsed_clock::time_point _start;
    std::chrono::microseconds _slow_query_threshold;
    trace_state_props_set _state_props;
    state _state = state::inactive;
    std::chrono::system_clock::rep _started_at;
    gms::inet_address _client;
    sstring _request;
    int _pending_trace_events = 0;
    shared_ptr<tracing> _local_tracing_ptr;

    struct params_values;
    struct params_values_deleter {
        void operator()(params_values* pv);
    };

    class params_ptr {
    private:
        std::unique_ptr<params_values, params_values_deleter> _vals;
        params_values* get_ptr_safe();

    public:
        explicit operator bool() const {
            return (bool)_vals;
        }

        params_values* operator->() {
            return get_ptr_safe();
        }

        params_values& operator*() {
            return *get_ptr_safe();
        }
    } _params_ptr;

public:
    trace_state(trace_type type, trace_state_props_set props)
        : _state_props(props)
        , _local_tracing_ptr(tracing::get_local_tracing_instance().shared_from_this())
    {
        if (!full_tracing() && !log_slow_query()) {
            throw std::logic_error("A primary session has to be created for either full tracing or a slow query logging");
        }

        // This is a primary session
        _state_props.set(trace_state_props::primary);

        init_session_records(type, _local_tracing_ptr->slow_query_record_ttl());
        _slow_query_threshold = _local_tracing_ptr->slow_query_threshold();
    }

    trace_state(const trace_info& info)
        : _state_props(info.state_props)
        , _local_tracing_ptr(tracing::get_local_tracing_instance().shared_from_this())
    {
        // This is a secondary session
        _state_props.remove(trace_state_props::primary);

        // Default a secondary session to a full tracing.
        // We may get both zeroes for a full_tracing and a log_slow_query if a
        // primary session is created with an older server version.
        _state_props.set_if<trace_state_props::full_tracing>(!full_tracing() && !log_slow_query());

        // inherit the slow query threshold and ttl from the coordinator
        init_session_records(info.type, std::chrono::seconds(info.slow_query_ttl_sec), info.session_id, info.parent_id);
        _slow_query_threshold = std::chrono::microseconds(info.slow_query_threshold_us);

        trace_state_logger.trace("{}: props {}, slow query threshold {}us, slow query ttl {}s", session_id(), _state_props.mask(), info.slow_query_threshold_us, info.slow_query_ttl_sec);
    }

    ~trace_state();

    const utils::UUID& session_id() const {
        return _records->session_id;
    }

    bool is_in_state(state s) const {
        return _state == s;
    }

    void set_state(state s) {
        _state = s;
    }

    trace_type type() const {
        return _records->session_rec.command;
    }

    bool is_primary() const {
        return _state_props.contains(trace_state_props::primary);
    }

    bool write_on_close() const {
        return _state_props.contains(trace_state_props::write_on_close);
    }

    bool full_tracing() const {
        return _state_props.contains(trace_state_props::full_tracing);
    }

    bool log_slow_query() const {
        return _state_props.contains(trace_state_props::log_slow_query);
    }

    trace_state_props_set raw_props() const {
        return _state_props;
    }

    /**
     * @return a slow query threshold value in microseconds.
     */
    uint32_t slow_query_threshold_us() const {
        return _slow_query_threshold.count();
    }

    /**
     * @return a slow query entry TTL value in seconds
     */
    uint32_t slow_query_ttl_sec() const {
        return _records->session_rec.slow_query_record_ttl.count();
    }

    /**
     * @return a span ID
     */
    span_id my_span_id() const {
        return _records->my_span_id;
    }

private:
    /**
     * Stop a foreground state and write pending records to I/O.
     *
     * @param prepared_options_ptr query options of the prepared statement
     *
     * @note The tracing session's "duration" is the time it was in the "foreground" state.
     */
    void stop_foreground_and_write(const cql3::query_options* prepared_options_ptr = nullptr) noexcept;

    bool should_log_slow_query(elapsed_clock::duration e) const {
        return log_slow_query() && e > _slow_query_threshold;
    }

    void init_session_records(trace_type type, std::chrono::seconds slow_query_ttl, std::experimental::optional<utils::UUID> session_id = std::experimental::nullopt, span_id parent_id = span_id::illegal_id) {
        _records = make_lw_shared<one_session_records>();
        _records->session_id = session_id ? *session_id : utils::UUID_gen::get_time_UUID();

        if (full_tracing()) {
            if (!log_slow_query()) {
                _records->ttl = ttl_by_type(type);
            } else {
                _records->ttl = std::max(ttl_by_type(type), slow_query_ttl);
            }
        } else {
            _records->ttl = slow_query_ttl;
        }

        _records->session_rec.command = type;
        _records->session_rec.slow_query_record_ttl = slow_query_ttl;
        _records->my_span_id = span_id::make_span_id();
        _records->parent_id = parent_id;
    }

    bool should_write_records() const {
        return full_tracing() || _records->do_log_slow_query;
    }

    /**
     * Returns the amount of time passed since the beginning of this tracing session.
     *
     * @return the amount of time passed since the beginning of this session
     */
    elapsed_clock::duration elapsed();

    /**
     * Initiates a tracing session.
     *
     * Starts the tracing session time measurments.
     * This overload is meant for secondary sessions.
     */
    void begin() {
        std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
        _start = elapsed_clock::now();
        std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
        set_state(state::foreground);
    }

    /**
     * Initiates a tracing session.
     *
     * Starts the tracing session time measurments.
     * This overload is meant for primary sessions.
     *
     * @param request description of a request being traces
     * @param client address of a client the traced request came from
     */
    void begin(sstring request, gms::inet_address client) {
        begin();
        _records->session_rec.client = client;
        _records->session_rec.request = std::move(request);
        _records->session_rec.started_at = std::chrono::system_clock::now();
    }

    template <typename Func>
    void begin(const seastar::lazy_eval<Func>& lf, gms::inet_address client) {
        begin(lf(), client);
    }

    /**
     * Stores a batchlog endpoints.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'batchlog_endpoints' key.
     *
     * @param val the set of batchlog endpoints
     */
    void set_batchlog_endpoints(const std::unordered_set<gms::inet_address>& val);

    /**
     * Stores a consistency level of a query being traced.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'consistency_level' key.
     *
     * @param val the consistency level
     */
    void set_consistency_level(db::consistency_level val);

    /**
     * Stores an optional serial consistency level of a query being traced.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'serial_consistency_level' key.
     *
     * @param val the optional value with a serial consistency level
     */
    void set_optional_serial_consistency_level(const std::experimental::optional<db::consistency_level>& val);

    /**
     * Returns the string with the representation of the given raw value.
     * If the value is NULL or unset the 'null' or 'unset value' strings are returned correspondingly.
     *
     * @param v view of the given raw value
     * @param t type object corresponding to the given raw value.
     * @return the string with the representation of the given raw value.
     */
    sstring raw_value_to_sstring(const cql3::raw_value_view& v, const data_type& t);

    /**
     * Stores a page size of a query being traced.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'page_size' key.
     *
     * @param val the PAGE size
     */
    void set_page_size(int32_t val);

    /**
     * Set a size of the request being traces.
     *
     * @param s a request size
     */
    void set_request_size(size_t s) noexcept;

    /**
     * Set a size of the response of the query being traces.
     *
     * @param s a response size
     */
    void set_response_size(size_t s) noexcept;

    /**
     * Store a query string.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'query' key.
     *
     * @param val the query string
     */
    void add_query(sstring_view val);

    /**
     * Store a user provided timestamp.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'user_timestamp' key.
     *
     * @param val the timestamp
     */
    void set_user_timestamp(api::timestamp_type val);

    /**
     * Store a pointer to a prepared statement that is being traced.
     *
     * There may be more than one prepared statement that is traced in case of a BATCH command.
     *
     * @param prepared a checked weak pointer to a prepared statement
     */
    void add_prepared_statement(prepared_checked_weak_ptr& prepared);

    void set_username(const stdx::optional<auth::authenticated_user>& user) {
        if (user) {
            _records->session_rec.username = sprint("%s", *user);
        }
    }

    void add_table_name(sstring full_table_name) {
        _records->session_rec.tables.emplace(std::move(full_table_name));
    }

    /**
     * Fill the map in a session's record with the values set so far.
     *
     * @param prepared_options_ptr parameters of the prepared statement
     */
    void build_parameters_map(const cql3::query_options* prepared_options_ptr);

    /**
     * Fill the map in a session's record with the parameters' values of a single prepared statement.
     *
     * Parameters values will be stored with a key '@ref param_name_prefix[X]' where X is an index of the corresponding
     * parameter.
     *
     * @param prepared prepared statement handle
     * @param options CQL options used in the current invocation of the prepared statement
     * @param param_name_prefix prefix of the parameter key in the map, e.g. "param" or "param[1]"
     */
    void build_parameters_map_for_one_prepared(const prepared_checked_weak_ptr& prepared, const cql3::query_options& options, const sstring& param_name_prefix);

    /**
     * The actual trace message storing method.
     *
     * @note This method is allowed to throw.
     * @param msg the trace message to store
     */
    void trace_internal(sstring msg);

    /**
     * Add a single trace entry - a special case for a simple string.
     *
     * @param msg trace message
     */
    void trace(sstring msg) noexcept {
        try {
            trace_internal(std::move(msg));
        } catch (...) {
            // Bump up an error counter and ignore
            ++_local_tracing_ptr->stats.trace_errors;
        }
    }
    void trace(const char* msg) noexcept {
        try {
            trace_internal(sstring(msg));
        } catch (...) {
            // Bump up an error counter and ignore
            ++_local_tracing_ptr->stats.trace_errors;
        }
    }

    /**
     * Add a single trace entry - printf-like version
     *
     * Add a single trace entry with a message given in a printf-like way:
     * format string with positional parameters.
     *
     * @note Both format string and positional parameters are going to be copied
     * and the final string is going to built later. A caller has to take this
     * into an account and make sure that positional parameters are both
     * copiable and that their copying is not expensive.
     *
     * @tparam A
     * @param fmt format string
     * @param a positional parameters
     */
    template <typename... A>
    void trace(const char* fmt, A&&... a) noexcept;

    template <typename... A>
    friend void begin(const trace_state_ptr& p, A&&... a);

    template <typename... A>
    friend void trace(const trace_state_ptr& p, A&&... a) noexcept;

    friend void set_page_size(const trace_state_ptr& p, int32_t val);
    friend void set_request_size(const trace_state_ptr& p, size_t s) noexcept;
    friend void set_response_size(const trace_state_ptr& p, size_t s) noexcept;
    friend void set_batchlog_endpoints(const trace_state_ptr& p, const std::unordered_set<gms::inet_address>& val);
    friend void set_consistency_level(const trace_state_ptr& p, db::consistency_level val);
    friend void set_optional_serial_consistency_level(const trace_state_ptr& p, const std::experimental::optional<db::consistency_level>&val);
    friend void add_query(const trace_state_ptr& p, sstring_view val);
    friend void set_user_timestamp(const trace_state_ptr& p, api::timestamp_type val);
    friend void add_prepared_statement(const trace_state_ptr& p, prepared_checked_weak_ptr& prepared);
    friend void set_username(const trace_state_ptr& p, const stdx::optional<auth::authenticated_user>& user);
    friend void add_table_name(const trace_state_ptr& p, const sstring& ks_name, const sstring& cf_name);
    friend void stop_foreground(const trace_state_ptr& state) noexcept;
    friend void stop_foreground_prepared(const trace_state_ptr& state, const cql3::query_options* prepared_options_ptr) noexcept;
};

class trace_state_ptr final {
private:
    lw_shared_ptr<trace_state> _state_ptr;

public:
    trace_state_ptr() = default;
    trace_state_ptr(lw_shared_ptr<trace_state> state_ptr)
        : _state_ptr(std::move(state_ptr))
    {}
    trace_state_ptr(nullptr_t)
        : _state_ptr(nullptr)
    {}

    explicit operator bool() const noexcept {
        return __builtin_expect(bool(_state_ptr), false);
    }

    trace_state* operator->() const noexcept {
        return _state_ptr.get();
    }

    trace_state& operator*() const noexcept {
        return *_state_ptr;
    }
};

inline void trace_state::trace_internal(sstring message) {
    if (is_in_state(state::inactive)) {
        throw std::logic_error("trying to use a trace() before begin() for \"" + message + "\" tracepoint");
    }

    // We don't want the total amount of pending, active and flushing records to
    // bypass two times the maximum number of pending records.
    //
    // If either records are being created too fast or a backend doesn't
    // keep up we want to start dropping records.
    // In any case, this should be rare, therefore we don't try to optimize this
    // flow.
    if (!_local_tracing_ptr->have_records_budget()) {
        tracing_logger.trace("{}: Maximum number of traces is reached. Some traces are going to be dropped", session_id());
        if ((++_local_tracing_ptr->stats.dropped_records) % tracing::log_warning_period == 1) {
            tracing_logger.warn("Maximum records limit is hit {} times", _local_tracing_ptr->stats.dropped_records);
        }

        return;
    }

    try {
        auto e = elapsed();
        _records->events_recs.emplace_back(std::move(message), e, i_tracing_backend_helper::wall_clock::now());
        _records->consume_from_budget();

        // If we have aggregated enough records - schedule them for write already.
        //
        // We prefer the traces to be written after the session is over. However
        // if there is a session that creates a lot of traces - we want to write
        // them before we start to drop new records.
        //
        // We don't want to write records of a tracing session if we trace only
        // slow queries and the elapsed time is still below the slow query
        // logging threshold.
        if (_records->events_recs.size() >= tracing::exp_trace_events_per_session && (full_tracing() || should_log_slow_query(e))) {
            _local_tracing_ptr->schedule_for_write(_records);
            _local_tracing_ptr->write_maybe();
        }
    } catch (...) {
        // Bump up an error counter and ignore
        ++_local_tracing_ptr->stats.trace_errors;
    }
}

template <typename... A>
void trace_state::trace(const char* fmt, A&&... a) noexcept {
    try {
        trace_internal(seastar::format(fmt, std::forward<A>(a)...));
    } catch (...) {
        // Bump up an error counter and ignore
        ++_local_tracing_ptr->stats.trace_errors;
    }
}

inline elapsed_clock::duration trace_state::elapsed() {
    using namespace std::chrono;
    std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
    elapsed_clock::duration elapsed = elapsed_clock::now() - _start;
    std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);

    return elapsed;
}

inline void set_page_size(const trace_state_ptr& p, int32_t val) {
    if (p) {
        p->set_page_size(val);
    }
}

inline void set_request_size(const trace_state_ptr& p, size_t s) noexcept {
    if (p) {
        p->set_request_size(s);
    }
}

inline void set_response_size(const trace_state_ptr& p, size_t s) noexcept {
    if (p) {
        p->set_response_size(s);
    }
}

inline void set_batchlog_endpoints(const trace_state_ptr& p, const std::unordered_set<gms::inet_address>& val) {
    if (p) {
        p->set_batchlog_endpoints(val);
    }
}

inline void set_consistency_level(const trace_state_ptr& p, db::consistency_level val) {
    if (p) {
        p->set_consistency_level(val);
    }
}

inline void set_optional_serial_consistency_level(const trace_state_ptr& p, const std::experimental::optional<db::consistency_level>& val) {
    if (p) {
        p->set_optional_serial_consistency_level(val);
    }
}

inline void add_query(const trace_state_ptr& p, sstring_view val) {
    if (p) {
        p->add_query(std::move(val));
    }
}

inline void set_user_timestamp(const trace_state_ptr& p, api::timestamp_type val) {
    if (p) {
        p->set_user_timestamp(val);
    }
}

inline void add_prepared_statement(const trace_state_ptr& p, prepared_checked_weak_ptr& prepared) {
    if (p) {
        p->add_prepared_statement(prepared);
    }
}

inline void set_username(const trace_state_ptr& p, const stdx::optional<auth::authenticated_user>& user) {
    if (p) {
        p->set_username(user);
    }
}

inline void add_table_name(const trace_state_ptr& p, const sstring& ks_name, const sstring& cf_name) {
    if (p) {
        p->add_table_name(ks_name + "." + cf_name);
    }
}

inline bool should_return_id_in_response(const trace_state_ptr& p) {
    if (p) {
        return p->write_on_close();
    }
    return false;
}

/**
 * A helper for conditional invoking trace_state::begin() functions.
 *
 * If trace state is initialized the operation takes place immediatelly,
 * otherwise nothing happens.
 *
 * @tparam A
 * @param p trace state handle
 * @param a optional parameters for trace_state::begin()
 */
template <typename... A>
inline void begin(const trace_state_ptr& p, A&&... a) {
    if (p) {
        p->begin(std::forward<A>(a)...);
    }
}

/**
 * A helper for conditional invoking trace_state::trace() function.
 *
 * Create a trace entry if a given trace state @param p is initialized.
 * Otherwise, it @param p is not initialized - do nothing.
 * Trace message may be passed as a printf-like format string with the
 * corresponding positional parameters.
 *
 * If @param p is initialized both trace message string and positional
 * parameters are going to be copied and the final string is going to be build
 * later. Therefore a caller has to take this into an account and make sure
 * that positional parameters are both copiable and that the copy is not
 * expensive.
 *
 * @param A
 * @param p trace state handle
 * @param a trace message format string with optional parameters
 */
template <typename... A>
inline void trace(const trace_state_ptr& p, A&&... a) noexcept {
    if (p) {
        p->trace(std::forward<A>(a)...);
    }
}

inline std::experimental::optional<trace_info> make_trace_info(const trace_state_ptr& state) {
    // We want to trace the remote replicas' operations only when a full tracing
    // is requested or when a slow query logging is enabled and the session is
    // still active.
    //
    // When only a slow query logging is enabled we don't really care what
    // happens on a remote replica after a Client has received a response for
    // his/her query.
    if (state && (state->full_tracing() || (state->log_slow_query() && !state->is_in_state(trace_state::state::background)))) {
        return trace_info{state->session_id(), state->type(), state->write_on_close(), state->raw_props(), state->slow_query_threshold_us(), state->slow_query_ttl_sec(), state->my_span_id()};
    }

    return std::experimental::nullopt;
}

inline void stop_foreground(const trace_state_ptr& state) noexcept {
    if (state) {
        state->stop_foreground_and_write();
    }
}

inline void stop_foreground_prepared(const trace_state_ptr& state, const cql3::query_options* prepared_options_ptr) noexcept {
    if (state) {
        state->stop_foreground_and_write(prepared_options_ptr);
    }
}

// global_trace_state_ptr is a helper class that may be used for creating spans
// of an existing tracing session on other shards. When a tracing span on a
// different shard is needed global_trace_state_ptr would create a secondary
// tracing session on that shard similarly to what we do when we create tracing
// spans on remote Nodes.
//
// The usage is straight forward:
// 1. Create a global_trace_state_ptr from the existing trace_state_ptr object.
// 2. Pass it to the execution unit that (possibly) runs on a different shard
//    and pass the global_trace_state_ptr object instead of a trace_state_ptr
//    object.
class global_trace_state_ptr {
    unsigned _cpu_of_origin;
    trace_state_ptr _ptr;
public:
    // Note: the trace_state_ptr must come from the current shard
    global_trace_state_ptr(trace_state_ptr t)
            : _cpu_of_origin(engine().cpu_id())
            , _ptr(std::move(t))
    { }

    // May be invoked across shards.
    global_trace_state_ptr(const global_trace_state_ptr& other)
            : global_trace_state_ptr(other.get())
    { }

    // May be invoked across shards.
    global_trace_state_ptr(global_trace_state_ptr&& other)
            : global_trace_state_ptr(other.get())
    { }

    global_trace_state_ptr& operator=(const global_trace_state_ptr&) = delete;

    // May be invoked across shards.
    trace_state_ptr get() const {
        // optimize the "tracing not enabled" case
        if (!_ptr) {
            return nullptr;
        }

        if (_cpu_of_origin != engine().cpu_id()) {
            auto opt_trace_info = make_trace_info(_ptr);
            if (opt_trace_info) {
                trace_state_ptr new_trace_state = tracing::get_local_tracing_instance().create_session(*opt_trace_info);
                begin(new_trace_state);
                return new_trace_state;
            } else {
                return nullptr;
            }
        }

        return _ptr;
    }

    // May be invoked across shards.
    operator trace_state_ptr() const { return get(); }
};
}
