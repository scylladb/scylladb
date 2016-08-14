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
#include "mutation.hh"
#include "utils/UUID_gen.hh"
#include "tracing/tracing.hh"
#include "gms/inet_address.hh"

namespace tracing {

extern logging::logger trace_state_logger;

class trace_state final {
private:
    lw_shared_ptr<one_session_records> _records;
    bool _write_on_close;
    // Used for calculation of time passed since the beginning of a tracing
    // session till each tracing event.
    elapsed_clock::time_point _start;
    // TRUE for a primary trace_state object
    bool _primary;

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
    } _state = state::inactive;

    std::chrono::system_clock::rep _started_at;
    gms::inet_address _client;
    sstring _request;
    int _pending_trace_events = 0;
    shared_ptr<tracing> _local_tracing_ptr;

    struct params_values {
        std::experimental::optional<std::unordered_set<gms::inet_address>> batchlog_endpoints;
        std::experimental::optional<api::timestamp_type> user_timestamp;
        std::experimental::optional<sstring> query;
        std::experimental::optional<db::consistency_level> cl;
        std::experimental::optional<db::consistency_level> serial_cl;
        std::experimental::optional<int32_t> page_size;
    };

    class params_ptr {
    private:
        std::unique_ptr<params_values> _vals;

        params_values* get_ptr_safe() {
            if (!_vals) {
                _vals = std::make_unique<params_values>();
            }
            return _vals.get();
        }

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
    trace_state(trace_type type, bool write_on_close, const std::experimental::optional<utils::UUID>& session_id = std::experimental::nullopt)
        : _write_on_close(write_on_close)
        , _primary(!session_id)
        , _local_tracing_ptr(tracing::get_local_tracing_instance().shared_from_this())
    {
        _records = make_lw_shared<one_session_records>();
        _records->session_id = session_id ? *session_id : utils::UUID_gen::get_time_UUID();
        _records->ttl = ttl_by_type(type);
        _records->session_rec.command = type;
    }

    ~trace_state();

    /**
     * Stop a foreground state and write pending records to I/O.
     *
     * @note The tracing session's "duration" is the time it was in the "foreground"
     * state.
     */
    void stop_foreground_and_write();

    const utils::UUID& get_session_id() const {
        return _records->session_id;
    }

    trace_type get_type() const {
        return _records->session_rec.command;
    }

    bool get_write_on_close() const {
        return _write_on_close;
    }

private:
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
        _state = state::foreground;
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
    void set_batchlog_endpoints(const std::unordered_set<gms::inet_address>& val) {
        _params_ptr->batchlog_endpoints.emplace(val);
    }

    /**
     * Stores a consistency level of a query being traced.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'consistency_level' key.
     *
     * @param val the consistency level
     */
    void set_consistency_level(db::consistency_level val) {
        _params_ptr->cl.emplace(val);
    }

    /**
     * Stores an optional serial consistency level of a query being traced.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'serial_consistency_level' key.
     *
     * @param val the optional value with a serial consistency level
     */
    void set_optional_serial_consistency_level(const std::experimental::optional<db::consistency_level>& val) {
        if (val) {
            _params_ptr->serial_cl.emplace(*val);
        }
    }

    /**
     * Stores a page size of a query being traced.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'page_size' key.
     *
     * @param val the PAGE size
     */
    void set_page_size(int32_t val) {
        if (val > 0) {
            _params_ptr->page_size.emplace(val);
        }
    }

    /**
     * Store a query string.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'query' key.
     *
     * @param val the query string
     */
    void set_query(const sstring& val) {
        _params_ptr->query.emplace(val);
    }

    /**
     * Store a user provided timestamp.
     *
     * This value will eventually be stored in a params<string, string> map of a tracing session
     * with a 'user_timestamp' key.
     *
     * @param val the timestamp
     */
    void set_user_timestamp(api::timestamp_type val) {
        _params_ptr->user_timestamp.emplace(val);
    }

    /**
     * Fill the map in a session's record with the values set so far.
     *
     * @param params_map the map to fill
     */
    void build_parameters_map();

    /**
     * Add a single trace entry - a special case for a simple string.
     *
     * @param msg trace message
     */
    void trace(sstring msg);
    void trace(const char* msg) {
        trace(sstring(msg));
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
    void trace(const char* fmt, A&&... a);

    template <typename... A>
    friend void begin(const trace_state_ptr& p, A&&... a);

    template <typename... A>
    friend void trace(const trace_state_ptr& p, A&&... a);

    friend void set_page_size(const trace_state_ptr& p, int32_t val);
    friend void set_batchlog_endpoints(const trace_state_ptr& p, const std::unordered_set<gms::inet_address>& val);
    friend void set_consistency_level(const trace_state_ptr& p, db::consistency_level val);
    friend void set_optional_serial_consistency_level(const trace_state_ptr& p, const std::experimental::optional<db::consistency_level>&val);
    friend void set_query(const trace_state_ptr& p, const sstring& val);
    friend void set_user_timestamp(const trace_state_ptr& p, api::timestamp_type val);
};

inline void trace_state::trace(sstring message) {
    if (_state == state::inactive) {
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
        tracing_logger.trace("{}: Maximum number of traces is reached. Some traces are going to be dropped", get_session_id());
        if ((++_local_tracing_ptr->stats.dropped_records) % tracing::log_warning_period == 1) {
            tracing_logger.warn("Maximum records limit is hit {} times", _local_tracing_ptr->stats.dropped_records);
        }

        return;
    }

    try {
        _records->events_recs.emplace_back(std::move(message), elapsed(), i_tracing_backend_helper::wall_clock::now());
        _records->consume_from_budget();

        // If we have aggregated enough records - schedule them for write already.
        //
        // We prefer the traces to be written after the session is over. However
        // if there is a session that creates a lot of traces - we want to write
        // them before we start to drop new records.
        if (_records->events_recs.size() >= tracing::exp_trace_events_per_session) {
            _local_tracing_ptr->schedule_for_write(_records);
            _local_tracing_ptr->write_maybe();
        }
    } catch (...) {
        // Bump up an error counter and ignore
        ++_local_tracing_ptr->stats.trace_errors;
    }
}

template <typename... A>
void trace_state::trace(const char* fmt, A&&... a) {
    try {
        trace(seastar::format(fmt, std::forward<A>(a)...));
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

inline void set_query(const trace_state_ptr& p, const sstring& val) {
    if (p) {
        p->set_query(val);
    }
}

inline void set_user_timestamp(const trace_state_ptr& p, api::timestamp_type val) {
    if (p) {
        p->set_user_timestamp(val);
    }
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
inline void trace(const trace_state_ptr& p, A&&... a) {
    if (p) {
        p->trace(std::forward<A>(a)...);
    }
}

inline std::experimental::optional<trace_info> make_trace_info(const trace_state_ptr& state) {
    if (state) {
        return trace_info{state->get_session_id(), state->get_type(), state->get_write_on_close()};
    }

    return std::experimental::nullopt;
}

inline void stop_foreground(const trace_state_ptr& state) {
    if (state) {
        state->stop_foreground_and_write();
    }
}
}
