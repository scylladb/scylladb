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
#include "database.hh"
#include "tracing/tracing.hh"
#include "cql3/query_processor.hh"

namespace tracing {

class trace_keyspace_helper : public i_tracing_backend_helper {
public:
    static const sstring KEYSPACE_NAME;
    static const sstring SESSIONS;
    static const sstring EVENTS;

private:
    using mutation_maker = std::function<mutation (const utils::UUID& session_id)>;

    static constexpr int bad_column_family_message_period = 10000;

    struct events_mutation_makers {
        std::vector<mutation_maker> makers;
    public:
        events_mutation_makers() {
            makers.reserve(tracing::max_trace_events_per_session);
        }
    };

    // a hash table of session ID to one session mutation and a vector of events mutations
    std::unordered_map<utils::UUID, std::pair<mutation_maker, events_mutation_makers>> _mutation_makers;

    seastar::gate _pending_writes;

    sstring _sessions_create_cql;
    sstring _events_create_cql;

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

    scollectd::registrations _registrations;

public:
    trace_keyspace_helper();
    virtual ~trace_keyspace_helper() {}

    // Create keyspace and tables.
    // Should be called after DB service is initialized - relies on it.
    //
    // TODO: Create a stub_tracing_session object to discard the traces
    // requested during the initialization phase.
    virtual future<> start() override;

    virtual void flush() override;
    virtual future<> stop() override {
        flush();
        return _pending_writes.close();
    };

    virtual void store_session_record(const utils::UUID& session_id,
                                      gms::inet_address client,
                                      std::unordered_map<sstring, sstring> parameters,
                                      sstring request,
                                      long started_at,
                                      trace_type command,
                                      int elapsed,
                                      gc_clock::duration ttl) override;

    virtual void store_event_record(const utils::UUID& session_id,
                                    sstring message,
                                    int elapsed,
                                    gc_clock::duration ttl) override;

private:
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
    future<> setup_table(const sstring& name, const sstring& cql);

    /**
     * Flush mutations of one particular tracing session. First "events"
     * mutations and then, when they are complete, a "sessions" mutation.
     *
     * @param session_id ID of a tracing session
     * @param mutation_makers a pair of a "sessions" mutation maker and an array
     *                        of "events" mutations makers.
     *
     * @return A future that resolves when applying of above mutations is
     *         complete.
     */
    future<> flush_one_session_mutations(utils::UUID session_id, std::pair<mutation_maker, events_mutation_makers>& mutation_makers);

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
    schema_ptr get_schema_ptr_or_create(utils::UUID& id, const sstring& table_name, const sstring& cql, ColumnsHandlesChecker check_and_cache);

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
     * Cache definitions of a system_traces.events table: table ID and column
     * definitions.
     *
     * @param s schema handle
     * @return TRUE if succeeded to cache all relevant information. FALSE will
     *         be returned if something is wrong with the table: it was dropped
     *         or its columns definitions are not as expected.
     */
    bool cache_events_table_handles(const schema_ptr& s);

    mutation make_session_mutation(const utils::UUID& session_id,
                                   gms::inet_address client,
                                   const std::unordered_map<sstring, sstring>& parameters,
                                   const sstring& request,
                                   long started_at,
                                   const sstring& command,
                                   int elapsed,
                                   gc_clock::duration ttl);

    mutation make_event_mutation(const utils::UUID& session_id,
                                 const sstring& message,
                                 int elapsed,
                                 const sstring& thread_name,
                                 gc_clock::duration ttl);
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
schema_ptr trace_keyspace_helper::get_schema_ptr_or_create(utils::UUID& id, const sstring& table_name, const sstring& cql, ColumnsHandlesChecker check_and_cache) {
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
