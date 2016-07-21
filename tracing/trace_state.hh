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

class trace_state final {
    using clock_type = std::chrono::steady_clock;

private:
    utils::UUID _session_id;
    trace_type _type;
    bool _write_on_close;
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
    int _pending_trace_events = 0;
    shared_ptr<tracing> _local_tracing_ptr;
    i_tracing_backend_helper& _local_backend;

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
        : _session_id(session_id ? *session_id : utils::UUID_gen::get_time_UUID())
        , _type(type)
        , _write_on_close(write_on_close)
        , _ttl(ttl_by_type(_type))
        , _primary(!session_id)
        , _local_tracing_ptr(tracing::get_local_tracing_instance().shared_from_this())
        , _local_backend(_local_tracing_ptr->backend_helper())
    { }

    ~trace_state();

    const utils::UUID& get_session_id() const {
        return _session_id;
    }

    trace_type get_type() const {
        return _type;
    }

    bool get_write_on_close() const {
        return _write_on_close;
    }

private:
    /**
     * Returns the number of microseconds passed since the beginning of this
     * tracing session.
     *
     * @return number of microseconds passed since the beginning of this session
     */
    int elapsed();

    /**
     * Initiates a tracing session.
     *
     * Starts the tracing session time measurments.
     * This overload is meant for secondary sessions.
     */
    void begin() {
        std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
        _start = clock_type::now();
        std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
        _tracing_began = true;
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
        _started_at = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        _request = std::move(request);
        _client = std::move(client);
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

    std::unordered_map<sstring, sstring> get_params();

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
    if (!_tracing_began) {
        throw std::logic_error("trying to use a trace() before begin() for \"" + message + "\" tracepoint");
    }

    if (_pending_trace_events >= tracing::max_trace_events_per_session) {
        return;
    }

    _local_backend.write_event_record(_session_id, std::move(message), elapsed(), _ttl, i_tracing_backend_helper::wall_clock::now());
    ++_pending_trace_events;
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

inline int trace_state::elapsed() {
    using namespace std::chrono;
    std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);
    auto elapsed = duration_cast<microseconds>(clock_type::now() - _start).count();
    std::atomic_signal_fence(std::memory_order::memory_order_seq_cst);

    if (elapsed > std::numeric_limits<int>::max()) {
        return std::numeric_limits<int>::max();
    }

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
}
