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

#include <tuple>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include "tracing/tracing.hh"
#include "table_helper.hh"

namespace tracing {

class trace_keyspace_helper final : public i_tracing_backend_helper {
public:
    static const sstring KEYSPACE_NAME;
    static const sstring SESSIONS;
    static const sstring SESSIONS_TIME_IDX;
    static const sstring EVENTS;

    // Performance related tables
    static const sstring NODE_SLOW_QUERY_LOG;
    static const sstring NODE_SLOW_QUERY_LOG_TIME_IDX;

private:
    static constexpr int bad_column_family_message_period = 10000;

    seastar::gate _pending_writes;
    int64_t _slow_query_last_nanos = 0;
    service::query_state _dummy_query_state;

    cql3::query_processor* _qp_anchor;
    table_helper _sessions;
    table_helper _sessions_time_idx;
    table_helper _events;
    table_helper _slow_query_log;
    table_helper _slow_query_log_time_idx;

    struct stats {
        uint64_t tracing_errors = 0;
        uint64_t bad_column_family_errors = 0;
    } _stats;

    seastar::metrics::metric_groups _metrics;

public:
    trace_keyspace_helper(tracing& tr);
    virtual ~trace_keyspace_helper() {}

    // Create keyspace and tables.
    // Should be called after DB service is initialized - relies on it.
    //
    // TODO: Create a stub_tracing_session object to discard the traces
    // requested during the initialization phase.
    virtual future<> start(cql3::query_processor& qp) override;

    virtual future<> stop() override {
        return _pending_writes.close().then([this] { _qp_anchor = nullptr; });
    };

    virtual void write_records_bulk(records_bulk& bulk) override;
    virtual std::unique_ptr<backend_session_state_base> allocate_session_state() const override;

private:
    /**
     * Write records of a single tracing session
     *
     * @param records records to write
     */
    void write_one_session_records(lw_shared_ptr<one_session_records> records);

    /**
     * Flush mutations of one particular tracing session. First "events"
     * mutations and then, when they are complete, a "sessions" mutation.
     *
     * @note This function guaranties that it'll handle exactly the same number
     * of records @param records had when the function was invoked.
     *
     * @param records records describing the session's records
     *
     * @return A future that resolves when applying of above mutations is
     *         complete.
     */
    future<> flush_one_session_mutations(lw_shared_ptr<one_session_records> records);

    /**
     * Apply events records mutations.
     *
     * @param records all session records
     * @param events_records events recods to apply
     * @param start_point a time point when this tracing session data writing has started
     *
     * @return a future that resolves when the mutation has been written.
     *
     * @note A caller must ensure that @param events_records is alive till the
     * returned future resolves.
     */
    future<> apply_events_mutation(cql3::query_processor& qp, lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records);

    /**
     * Create a mutation data for a new session record
     *
     * @param all_records_handle handle to access an object with all records of this session
     *
     * @return the relevant cql3::query_options object with the mutation data
     */
    static cql3::query_options make_session_mutation_data(const one_session_records& all_records_handle);

    /**
     * Create a mutation data for a new session_idx record
     *
     * @param all_records_handle handle to access an object with all records of this session
     *
     * @return the relevant cql3::query_options object with the mutation data
     */
    static cql3::query_options make_session_time_idx_mutation_data(const one_session_records& all_records_handle);

    /**
     * Create mutation for a new slow_query_log record
     *
     * @param all_records_handle handle to access an object with all records of this session
     * @param start_time_id time UUID generated from the query start time
     *
     * @return the relevant mutation
     */
    static cql3::query_options make_slow_query_mutation_data(const one_session_records& all_records_handle, const utils::UUID& start_time_id);

    /**
     * Create mutation for a new slow_query_log_time_idx record
     *
     * @param all_records_handle handle to access an object with all records of this session
     * @param start_time_id time UUID generated from the query start time
     *
     * @return the relevant mutation
     */
    static cql3::query_options make_slow_query_time_idx_mutation_data(const one_session_records& all_records_handle, const utils::UUID& start_time_id);

    /**
     * Create a mutation data for a new trace point record
     *
     * @param session_records handle to access an object with all records of this session.
     *                        It's needed here in order to update the last event's mutation
     *                        timestamp value stored inside it.
     * @param record data describing this trace event
     *
     * @return a vector with the mutation data
     */
    std::vector<cql3::raw_value> make_event_mutation_data(one_session_records& session_records, const event_record& record);

    /**
     * Converts a @param elapsed to an int32_t value of microseconds.
     *
     * @param elapsed the duration to convert
     *
     * @return the amount of microseconds in a @param elapsed or a std::numeric_limits<int32_t>::max()
     * if their amount doesn't fit in the int32_t type.
     */
    static int32_t elapsed_to_micros(elapsed_clock::duration elapsed) {
        auto elapsed_micros = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        if (elapsed_micros > std::numeric_limits<int32_t>::max()) {
            return std::numeric_limits<int32_t>::max();
        }

        return elapsed_micros;
    }
};
}
