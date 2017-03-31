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

#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include "database.hh"
#include "tracing/tracing.hh"
#include "cql3/query_processor.hh"

namespace tracing {

class trace_keyspace_helper final : public i_tracing_backend_helper {
public:
    static const sstring KEYSPACE_NAME;
    static const sstring SESSIONS;
    static const sstring EVENTS;

    // Performance related tables
    static const sstring NODE_SLOW_QUERY_LOG;

private:
    static constexpr int bad_column_family_message_period = 10000;

    seastar::gate _pending_writes;
    int64_t _slow_query_last_nanos = 0;

    sstring _sessions_create_cql;
    sstring _events_create_cql;
    sstring _node_slow_query_log_cql;

    utils::UUID _slow_query_log_id;
    const column_definition* _slow_session_id_column;
    const column_definition* _slow_date_column;
    const column_definition* _slow_command_column;
    const column_definition* _slow_duration_column;
    const column_definition* _slow_parameters_column;
    const column_definition* _slow_source_ip_column;
    const column_definition* _slow_table_names_column;
    const column_definition* _slow_username_column;

    utils::UUID _sessions_id;
    const column_definition* _client_column;
    const column_definition* _coordinator_column;
    const column_definition* _request_column;
    const column_definition* _started_at_column;
    const column_definition* _command_column;
    const column_definition* _duration_column;
    const column_definition* _parameters_column;

    utils::UUID _events_id;
    const column_definition* _activity_column;
    const column_definition* _source_column;
    const column_definition* _thread_column;
    const column_definition* _source_elapsed_column;

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
     * Tries to create a table with a given name and using the provided CQL
     * command.
     *
     * @param name a table name
     * @param cql a CQL command for creating a table
     *
     * @return A future that resolves when the operation is complete. Any
     *         possible errors are ignored.
     */
    future<> setup_table(const sstring& name, const sstring& cql) const;

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
     *
     * @return a future that resolves when the mutation has been written.
     *
     * @note A caller must ensure that @param events_records is alive till the
     * returned future resolves.
     */
    future<> apply_events_mutation(lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records);

    /**
     * Get a schema_ptr by a table (UU)ID. If not found will try to get it by
     * name. If not found will issue a table creation and throw
     * no_such_column_family exception. If table is found by name but its
     * schema doesn't match the expected schema will throw bad_column_family
     * exception.
     *
     * @param ColumnsHandlesChecker a "bool (const schema_ptr&)" function that
     *                              checks the schema and cached a table's ID
     *                              and column definitions in case of a match
     * @param id table ID
     * @param table_name table name
     * @param cql CQL command for a table creation
     * @param check_and_cache a ColumnsHandlesChecker instance
     *
     * @return a schema_ptr as requested
     */
    template <typename ColumnsHandlesChecker>
    schema_ptr get_schema_ptr_or_create(utils::UUID& id, const sstring& table_name, const sstring& cql, ColumnsHandlesChecker check_and_cache) const;

    /**
     * Cache definitions of a system_traces.sessions table: table ID and column
     * definitions.
     *
     * @param s schema handle
     * @return TRUE if succeeded to cache all relevant information. FALSE will
     *         be returned if something is wrong with the table: it was dropped
     *         or its columns definitions are not as expected.
     */
    bool cache_sessions_table_handles(const schema_ptr& s);

    /**
     * Cache definitions of a system_traces.node_slow_log table: table ID and column
     * definitions.
     *
     * @param s schema handle
     * @return TRUE if succeeded to cache all relevant information. FALSE will
     *         be returned if something is wrong with the table: it was dropped
     *         or its columns definitions are not as expected.
     */
    bool cache_node_slow_log_table_handles(const schema_ptr& s);

    /**
     * Cache definitions of a system_traces.events table: table ID and column
     * definitions.
     *
     * @param s schema handle
     * @return TRUE if succeeded to cache all relevant information. FALSE will
     *         be returned if something is wrong with the table: it was dropped
     *         or its columns definitions are not as expected.
     */
    bool cache_events_table_handles(const schema_ptr& s);

    /**
     * Create a mutation for a new session record
     *
     * @param all_records_handle handle to access an object with all records of this session
     *
     * @return the relevant mutation
     */
    mutation make_session_mutation(const one_session_records& all_records_handle);

    /**
     * Create mutation for a new slow_query_log record
     *
     * @param all_records_handle handle to access an object with all records of this session
     *
     * @return the relevant mutation
     */
    mutation make_slow_query_mutation(const one_session_records& all_records_handle);

    /**
     * Create a mutation for a new trace point record
     *
     * @param session_records handle to access an object with all records of this session.
     *                        It's needed here in order to update the last event's mutation
     *                        timestamp value stored inside it.
     * @param record data describing this trace event
     *
     * @return the relevant mutation
     */
    mutation make_event_mutation(one_session_records& session_records, const event_record& record);

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

    bool check_column_definition(const column_definition* column_def, const shared_ptr<const abstract_type>& abstracet_type_inst) const {
        return column_def && column_def->type == abstracet_type_inst;
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
    const char* what() const noexcept override {
        return _what.c_str();
    }
};

template <typename ColumnsHandlesChecker>
schema_ptr trace_keyspace_helper::get_schema_ptr_or_create(utils::UUID& id, const sstring& table_name, const sstring& cql, ColumnsHandlesChecker check_and_cache) const {
    auto& db = cql3::get_local_query_processor().db().local();
    schema_ptr schema;
    try {
        schema = db.find_schema(id);
    } catch (...) {
        // if schema is not found by ID - try to find it by its name
        try {
            schema = db.find_schema(KEYSPACE_NAME, table_name);
        } catch (...) {
            // if not found - create and throw
            setup_table(table_name, cql).discard_result();
            throw;
        }
        // Update the ID and column definitions. If columns do not match our
        // expectations - throw an exception
        if (!check_and_cache(schema)) {
            throw bad_column_family(table_name);
        }
    }

    return schema;
}
}
