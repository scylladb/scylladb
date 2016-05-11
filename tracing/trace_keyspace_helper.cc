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

#include "types.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "service/migration_manager.hh"
#include "cql3/statements/create_table_statement.hh"

namespace tracing {

static logging::logger logger("trace_keyspace_helper");

const sstring trace_keyspace_helper::KEYSPACE_NAME("system_traces");
const sstring trace_keyspace_helper::SESSIONS("sessions");
const sstring trace_keyspace_helper::EVENTS("events");

trace_keyspace_helper::trace_keyspace_helper()
            : _registrations{
        scollectd::add_polled_metric(scollectd::type_instance_id("tracing_keyspace_helper"
                        , scollectd::per_cpu_plugin_instance
                        , "total_operations", "tracing_errors")
                        , scollectd::make_typed(scollectd::data_type::DERIVE, _stats.tracing_errors)),
        scollectd::add_polled_metric(scollectd::type_instance_id("tracing_keyspace_helper"
                        , scollectd::per_cpu_plugin_instance
                        , "total_operations", "bad_column_family_errors")
                        , scollectd::make_typed(scollectd::data_type::DERIVE, _stats.bad_column_family_errors))} {

        _sessions_create_cql = sprint("CREATE TABLE %s.%s ("
                                      "session_id uuid,"
                                      "command text,"
                                      "client inet,"
                                      "coordinator inet,"
                                      "duration int,"
                                      "parameters map<text, text>,"
                                      "request text,"
                                      "started_at timestamp,"
                                      "PRIMARY KEY ((session_id)))", KEYSPACE_NAME, SESSIONS);

        _events_create_cql = sprint("CREATE TABLE %s.%s ("
                                    "session_id uuid,"
                                    "event_id timeuuid,"
                                    "activity text,"
                                    "source inet,"
                                    "source_elapsed int,"
                                    "thread text,"
                                    "PRIMARY KEY ((session_id), event_id))", KEYSPACE_NAME, EVENTS);
}

future<> trace_keyspace_helper::setup_table(const sstring& name, const sstring& cql) {
    auto& qp = cql3::get_local_query_processor();
    auto& db = qp.db().local();

    if (db.has_schema(KEYSPACE_NAME, name)) {
        return make_ready_future<>();
    }

    // We don't care it it fails really - this may happen due to concurrent
    // "CREATE TABLE" invocation on different Nodes.
    // The important thing is that it will converge eventually (some traces may
    // be lost in a process but that's ok).
    return qp.process(cql, db::consistency_level::ONE).discard_result().handle_exception([this] (auto ep) {});
}

bool trace_keyspace_helper::cache_sessions_table_handles(const schema_ptr& schema) {
    auto session_id_column = schema->get_column_definition("session_id");
    _client_column = schema->get_column_definition("client");
    _coordinator_column = schema->get_column_definition("coordinator");
    _request_column = schema->get_column_definition("request");
    _started_at_column = schema->get_column_definition("started_at");
    _command_column = schema->get_column_definition("command");
    _duration_column = schema->get_column_definition("duration");
    _parameters_column = schema->get_column_definition("parameters");

    if (session_id_column && session_id_column->type == uuid_type &&
        _client_column && _client_column->type == inet_addr_type &&
        _coordinator_column && _coordinator_column->type == inet_addr_type &&
        _request_column && _request_column->type == utf8_type &&
        _started_at_column && _started_at_column->type == timestamp_type &&
        _command_column && _command_column->type == utf8_type &&
        _duration_column && _duration_column->type == int32_type &&
        _parameters_column && _parameters_column->type && map_type_impl::get_instance(utf8_type, utf8_type, true)) {
        // store a table ID only if its format meets our demands
        _sessions_id = schema->id();
        return true;
    } else {
        _sessions_id = utils::UUID();
        return false;
    }
}

bool trace_keyspace_helper::cache_events_table_handles(const schema_ptr& schema) {
    auto session_id_column = schema->get_column_definition("session_id");
    auto event_id_column = schema->get_column_definition("event_id");
    _activity_column = schema->get_column_definition("activity");
    _source_column = schema->get_column_definition("source");
    _thread_column = schema->get_column_definition("thread");
    _source_elapsed_column = schema->get_column_definition("source_elapsed");

    if (session_id_column && session_id_column->type == uuid_type &&
        event_id_column && event_id_column->type == timeuuid_type &&
        _activity_column && _activity_column->type == utf8_type &&
        _source_column && _source_column->type == inet_addr_type &&
        _thread_column && _thread_column->type == utf8_type &&
        _source_elapsed_column && _source_elapsed_column->type == int32_type) {
        // store a table ID only if its format meets our demands
        _events_id = schema->id();
        return true;
    } else {
        _events_id = utils::UUID();
        return false;
    }
}

future<> trace_keyspace_helper::start() {
    if (engine().cpu_id() == 0) {
        return seastar::async([this] {
            auto& db = cql3::get_local_query_processor().db().local();

            // Create a keyspace
            if (!db.has_keyspace(KEYSPACE_NAME)) {
                std::map<sstring, sstring> opts;
                opts["replication_factor"] = "2";
                auto ksm = keyspace_metadata::new_keyspace(KEYSPACE_NAME, "org.apache.cassandra.locator.SimpleStrategy", std::move(opts), true);
                service::get_local_migration_manager().announce_new_keyspace(ksm, false).get();
            }

            // Create tables
            setup_table(SESSIONS, _sessions_create_cql).get();
            setup_table(EVENTS, _events_create_cql).get();
        });
    } else {
        return make_ready_future<>();
    }
}

void trace_keyspace_helper::store_session_record(const utils::UUID& session_id,
                                                 gms::inet_address client,
                                                 std::unordered_map<sstring, sstring> parameters,
                                                 sstring request,
                                                 long started_at,
                                                 trace_type command,
                                                 int elapsed,
                                                 gc_clock::duration ttl) {
    try {
        _mutation_makers[session_id].first = [request = std::move(request), client, parameters = std::move(parameters), started_at, command, elapsed, ttl, this] (const utils::UUID& session_id) {
            return make_session_mutation(session_id, client, parameters, request, started_at, type_to_string(command), elapsed, ttl);
        };
    } catch (...) {
        // OOM: ignore
    }
}

void trace_keyspace_helper::store_event_record(const utils::UUID& session_id,
                                               sstring message,
                                               int elapsed,
                                               gc_clock::duration ttl) {
    try {
        _mutation_makers[session_id].second.makers.emplace_back([message = std::move(message), elapsed, ttl, this] (const utils::UUID& session_id) {
            return make_event_mutation(session_id, message, elapsed, tracing::get_local_tracing_instance().get_thread_name(), ttl);
        });
    } catch (...) {
        // OOM: ignore
    }
}

mutation trace_keyspace_helper::make_session_mutation(
        const utils::UUID& session_id,
        gms::inet_address client,
        const std::unordered_map<sstring, sstring>& parameters,
        const sstring& request,
        long started_at,
        const sstring& command,
        int elapsed,
        gc_clock::duration ttl) {
    schema_ptr schema = get_schema_ptr_or_create(_sessions_id, SESSIONS, _sessions_create_cql,
                                                 [this] (const schema_ptr& s) { return cache_sessions_table_handles(s); });

    auto key = partition_key::from_singular(*schema, session_id);
    auto timestamp = api::new_timestamp();
    mutation m(key, schema);
    auto& cells = m.partition().clustered_row(clustering_key::make_empty(*schema)).cells();

    cells.apply(*_client_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(client.addr()), ttl));
    cells.apply(*_coordinator_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr()), ttl));
    cells.apply(*_request_column, atomic_cell::make_live(timestamp, utf8_type->decompose(request), ttl));
    cells.apply(*_started_at_column, atomic_cell::make_live(timestamp, timestamp_type->decompose(started_at), ttl));
    cells.apply(*_command_column, atomic_cell::make_live(timestamp, utf8_type->decompose(command), ttl));
    cells.apply(*_duration_column, atomic_cell::make_live(timestamp, int32_type->decompose((int32_t)elapsed), ttl));

    std::vector<std::pair<bytes, atomic_cell>> map_cell;
    for (auto& param_pair : parameters) {
        map_cell.emplace_back(utf8_type->decompose(param_pair.first), atomic_cell::make_live(timestamp, utf8_type->decompose(param_pair.second), ttl));
    }

    map_type_impl::mutation map_mutation{{}, map_cell};
    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
    cells.apply(*_parameters_column, my_map_type->serialize_mutation_form(map_mutation));

    return m;
}

mutation trace_keyspace_helper::make_event_mutation(const utils::UUID& session_id,
                                                    const sstring& message,
                                                    int elapsed,
                                                    const sstring& thread_name,
                                                    gc_clock::duration ttl) {
    schema_ptr schema = get_schema_ptr_or_create(_events_id, EVENTS, _events_create_cql,
                                                 [this] (const schema_ptr& s) { return cache_events_table_handles(s); });

    auto key = partition_key::from_singular(*schema, session_id);
    auto timestamp = api::new_timestamp();
    mutation m(key, schema);
    auto& cells = m.partition().clustered_row(clustering_key::from_singular(*schema, utils::UUID_gen::get_time_UUID())).cells();

    cells.apply(*_activity_column, atomic_cell::make_live(timestamp, utf8_type->decompose(message), ttl));
    cells.apply(*_source_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr()), ttl));
    cells.apply(*_thread_column, atomic_cell::make_live(timestamp, utf8_type->decompose(thread_name), ttl));

    assert(elapsed >= 0);
    cells.apply(*_source_elapsed_column, atomic_cell::make_live(timestamp, int32_type->decompose(elapsed), ttl));

    return m;
}

future<> trace_keyspace_helper::flush_one_session_mutations(utils::UUID session_id, std::pair<mutation_maker, events_mutation_makers>& mutation_makers) {
    return make_ready_future<>().then([this, session_id, events_makers = std::move(mutation_makers.second.makers)] {
        if (events_makers.size()) {
            logger.debug("{}: events number is {}", session_id, events_makers.size());
            mutation m((*events_makers.begin())(session_id));
            std::for_each(std::next(events_makers.begin()), events_makers.end(), [&m, &session_id] (const mutation_maker& maker) mutable { m.apply(maker(session_id)); });
            return service::get_local_storage_proxy().mutate({std::move(m)}, db::consistency_level::ANY);
        } else {
            return make_ready_future<>();
        }
    }).then([session_id = std::move(session_id), session_maker = std::move(mutation_makers.first)] {
        if (session_maker) {
            logger.debug("{}: storing a session event", session_id);
            return service::get_local_storage_proxy().mutate({session_maker(session_id)}, db::consistency_level::ANY);
        } else {
            return make_ready_future<>();
        }
    });
}

void trace_keyspace_helper::flush() {
    parallel_for_each(_mutation_makers,[this](decltype(_mutation_makers)::value_type& uuid_mutation_makers) {
        return with_gate(_pending_writes, [this, &uuid_mutation_makers]  {
            logger.debug("{}: flushing traces", uuid_mutation_makers.first);
            return this->flush_one_session_mutations(uuid_mutation_makers.first, uuid_mutation_makers.second);
        }).handle_exception([this] (auto ep) {
            try {
                ++_stats.tracing_errors;
                std::rethrow_exception(ep);
            } catch (exceptions::overloaded_exception&) {
                logger.warn("Too many nodes are overloaded to save trace events");
            } catch (bad_column_family& e) {
                if (_stats.bad_column_family_errors++ % bad_column_family_message_period == 0) {
                    logger.warn("Tracing is enabled but {}", e.what());
                }
            } catch (...) {
                // TODO: Handle some more exceptions maybe?
            }
        });
    }).discard_result();

    // We can clear the hash table here because we cared to capture all relevant
    // data from it in lambdas' capture-lists inside
    // flush_one_session_mutations() before any asynchronous call.
    _mutation_makers.clear();
}

using registry = class_registrator<i_tracing_backend_helper, trace_keyspace_helper>;
static registry registrator1("trace_keyspace_helper");

}
