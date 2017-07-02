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

#include <tuple>
#include <seastar/core/gate.hh>
#include <seastar/core/apply.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/util/gcc6-concepts.hh>
#include "database.hh"
#include "tracing/tracing.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/modification_statement.hh"

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

    /**
     * \class table_helper
     * \brief A helper class that unites the operations on a single table under the same roof.
     */
    class table_helper {
    private:
        const sstring _name; /** a table name */
        const sstring _create_cql; /** a CQL CREATE TABLE statement for the table */
        const sstring _insert_cql; /** a CQL INSERT statement */

        cql3::statements::prepared_statement::checked_weak_ptr _prepared_stmt; /** a raw prepared statement object (containing the INSERT statement) */
        shared_ptr<cql3::statements::modification_statement> _insert_stmt; /** INSERT prepared statement */

        trace_keyspace_helper& _ks_helper; /** a reference to the trace_keyspace_helper object */

    public:
        table_helper(sstring name, trace_keyspace_helper& ks_helper, sstring create_cql, sstring insert_cql)
            : _name(std::move(name))
            , _create_cql(std::move(create_cql))
            , _insert_cql(std::move(insert_cql))
            , _ks_helper(ks_helper) {}

        /**
         * Tries to create a table using create_cql command.
         *
         * @return A future that resolves when the operation is complete. Any
         *         possible errors are ignored.
         */
        future<> setup_table() const;

        /**
         * @return a future that resolves when the given t_helper is ready to be used for
         * data insertion.
         */
        future<> cache_table_info();

        /**
         * @return The table name
         */
        const sstring& name() const {
            return _name;
        }

        /**
         * @return A pointer to the INSERT prepared statement
         */
        shared_ptr<cql3::statements::modification_statement> insert_stmt() const {
            return _insert_stmt;
        }

        /**
         * Execute a single insertion into the table.
         *
         * @tparam OptMaker cql_options maker functor type
         * @tparam Args OptMaker arguments' types
         * @param opt_maker cql_options maker functor
         * @param opt_maker_args opt_maker arguments
         */
        template <typename OptMaker, typename... Args>
        GCC6_CONCEPT( requires seastar::CanApply<OptMaker, Args...> )
        future<> insert(OptMaker opt_maker, Args&&... opt_maker_args) {
            return cache_table_info().then([this, opt_maker = std::move(opt_maker), args = std::forward_as_tuple(std::forward<Args>(opt_maker_args)...)] () mutable {
                return do_with(apply(opt_maker, std::move(args)), [this] (auto& opts) {
                    return _insert_stmt->execute(service::get_storage_proxy(), _ks_helper.get_dummy_qs(), opts);
                });
            }).discard_result();
        }
    };

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
    virtual future<> start() override;

    virtual future<> stop() override {
        return _pending_writes.close();
    };

    virtual void write_records_bulk(records_bulk& bulk) override;
    virtual std::unique_ptr<backend_session_state_base> allocate_session_state() const override;

    service::query_state& get_dummy_qs() { return _dummy_query_state; }

private:
    /**
     * Write records of a single tracing session
     *
     * @param records records to write
     */
    void write_one_session_records(lw_shared_ptr<one_session_records> records);

    /**
     * Makes a monotonically increasing value in 100ns ("nanos") based on the given time
     * stamp and the "nanos" value of the previous event.
     *
     * If the amount of 100s of ns evaluated from the @param tp is equal to the
     * given @param last_event_nanos increment @param last_event_nanos by one
     * and return a time point based its new value.
     *
     * @param last_event_nanos a reference to the last nanos to align the given time point to.
     * @param tp the amount of time passed since the Epoch that will be used for the calculation.
     *
     * @return the monotonically increasing vlaue in 100s of ns based on the
     * given time stamp and on the "nanos" value of the previous event.
     */
    static wall_clock::time_point make_monotonic_UUID_tp(int64_t& last_event_nanos, wall_clock::time_point tp) {
        using namespace std::chrono;

        auto tp_nanos = duration_cast<nanoseconds>(tp.time_since_epoch()).count() / 100;
        if (tp_nanos > last_event_nanos) {
            last_event_nanos = tp_nanos;
            return tp;
        } else {
            return wall_clock::time_point(nanoseconds((++last_event_nanos) * 100));
        }
    }

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
    future<> apply_events_mutation(lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records);

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

struct bad_column_family : public std::exception {
private:
    sstring _cf;
    sstring _what;
public:
    bad_column_family(const sstring& cf)
        : _cf(cf)
        , _what(sprint("%s.%s doesn't meet expected schema.", trace_keyspace_helper::KEYSPACE_NAME, _cf))
    { }
    bad_column_family(const sstring& cf, const std::exception& e)
        : _cf(cf)
        , _what(sprint("%s.%s doesn't meet expected schema: %s", trace_keyspace_helper::KEYSPACE_NAME, _cf, e.what()))
    { }
    const char* what() const noexcept override {
        return _what.c_str();
    }
};
}
