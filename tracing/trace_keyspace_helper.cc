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

struct trace_keyspace_backend_sesssion_state final : public backend_session_state_base {
    int64_t last_nanos = 0;
    virtual ~trace_keyspace_backend_sesssion_state() {}
};

trace_keyspace_helper::trace_keyspace_helper(tracing& tr)
            : i_tracing_backend_helper(tr)
            , _registrations{
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
                                      "PRIMARY KEY ((session_id))) "
                                      "WITH default_time_to_live = 86400", KEYSPACE_NAME, SESSIONS);

        _events_create_cql = sprint("CREATE TABLE %s.%s ("
                                    "session_id uuid,"
                                    "event_id timeuuid,"
                                    "activity text,"
                                    "source inet,"
                                    "source_elapsed int,"
                                    "thread text,"
                                    "PRIMARY KEY ((session_id), event_id)) "
                                    "WITH default_time_to_live = 86400", KEYSPACE_NAME, EVENTS);
}

future<> trace_keyspace_helper::setup_table(const sstring& name, const sstring& cql) const {
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

void trace_keyspace_helper::write_one_session_records(lw_shared_ptr<one_session_records> records) {
    with_gate(_pending_writes, [this, records = std::move(records)] {
        auto num_records = records->size();
        return this->flush_one_session_mutations(std::move(records)).finally([this, num_records] { _local_tracing.write_complete(num_records); });
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
    }).discard_result();
}

void trace_keyspace_helper::write_records_bulk(records_bulk& bulk) {
    logger.trace("Writing {} sessions", bulk.size());
    std::for_each(bulk.begin(), bulk.end(), [this] (records_bulk::value_type& one_session_records_ptr) {
        write_one_session_records(std::move(one_session_records_ptr));
    });
}

mutation trace_keyspace_helper::make_session_mutation(const one_session_records& session_records) {
    schema_ptr schema = get_schema_ptr_or_create(_sessions_id, SESSIONS, _sessions_create_cql,
                                                 [this] (const schema_ptr& s) { return cache_sessions_table_handles(s); });

    auto key = partition_key::from_singular(*schema, session_records.session_id);
    auto ttl = session_records.ttl;
    const session_record& record = session_records.session_rec;
    auto timestamp = api::new_timestamp();
    mutation m(key, schema);
    auto& cells = m.partition().clustered_row(clustering_key::make_empty(*schema)).cells();

    cells.apply(*_client_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(record.client.addr()), ttl));
    cells.apply(*_coordinator_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr()), ttl));
    cells.apply(*_request_column, atomic_cell::make_live(timestamp, utf8_type->decompose(record.request), ttl));
    cells.apply(*_started_at_column, atomic_cell::make_live(timestamp, timestamp_type->decompose(record.started_at), ttl));
    cells.apply(*_command_column, atomic_cell::make_live(timestamp, utf8_type->decompose(type_to_string(record.command)), ttl));
    cells.apply(*_duration_column, atomic_cell::make_live(timestamp, int32_type->decompose((int32_t)record.elapsed), ttl));

    std::vector<std::pair<bytes, atomic_cell>> map_cell;
    for (auto& param_pair : record.parameters) {
        map_cell.emplace_back(utf8_type->decompose(param_pair.first), atomic_cell::make_live(timestamp, utf8_type->decompose(param_pair.second), ttl));
    }

    map_type_impl::mutation map_mutation{{}, map_cell};
    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
    cells.apply(*_parameters_column, my_map_type->serialize_mutation_form(map_mutation));

    return m;
}

mutation trace_keyspace_helper::make_event_mutation(one_session_records& session_records, const event_record& record) {
    schema_ptr schema = get_schema_ptr_or_create(_events_id, EVENTS, _events_create_cql,
                                                 [this] (const schema_ptr& s) { return cache_events_table_handles(s); });

    auto key = partition_key::from_singular(*schema, session_records.session_id);
    auto ttl = session_records.ttl;
    auto backend_state_ptr = static_cast<trace_keyspace_backend_sesssion_state*>(session_records.backend_state_ptr.get());
    int64_t& last_event_nanos = backend_state_ptr->last_nanos;
    auto timestamp = api::new_timestamp();
    mutation m(key, schema);
    auto& cells = m.partition().clustered_row(clustering_key::from_singular(*schema, utils::UUID_gen::get_time_UUID(make_monotonic_UUID_tp(last_event_nanos, record.event_time_point)))).cells();

    cells.apply(*_activity_column, atomic_cell::make_live(timestamp, utf8_type->decompose(record.message), ttl));
    cells.apply(*_source_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr()), ttl));
    cells.apply(*_thread_column, atomic_cell::make_live(timestamp, utf8_type->decompose(_local_tracing.get_thread_name()), ttl));
    cells.apply(*_source_elapsed_column, atomic_cell::make_live(timestamp, int32_type->decompose(record.elapsed), ttl));

    return m;
}

future<> trace_keyspace_helper::apply_events_mutation(lw_shared_ptr<one_session_records> records) {
    std::deque<event_record>& events_records = records->events_recs;

    if (events_records.empty()) {
        return make_ready_future<>();
    }

    logger.trace("{}: storing {} events records", records->session_id, events_records.size());

    mutation m(make_event_mutation(*records, *events_records.begin()));

    return do_with(std::move(m), std::move(events_records), [this, records] (mutation& m, std::deque<event_record>& events_records) {
        return do_for_each(std::next(events_records.begin()), events_records.end(), [this, &m, &events_records, all_records = records] (event_record& one_event_record) {
            m.apply(make_event_mutation(*all_records, one_event_record));
            return make_ready_future<>();
        }).then([&m] {
            return service::get_local_storage_proxy().mutate({std::move(m)}, db::consistency_level::ANY, nullptr);
        });
    });
}

future<> trace_keyspace_helper::flush_one_session_mutations(lw_shared_ptr<one_session_records> records) {
    return futurize<void>::apply([this, records] {
        return apply_events_mutation(records);
    }).then([this, records] {
        if (records->session_rec.ready()) {
            logger.trace("{}: storing a session event", records->session_id);
            return service::get_local_storage_proxy().mutate({make_session_mutation(*records)}, db::consistency_level::ANY, nullptr);
        } else {
            return make_ready_future<>();
        }
    });
}

std::unique_ptr<backend_session_state_base> trace_keyspace_helper::allocate_session_state() const {
    return std::make_unique<trace_keyspace_backend_sesssion_state>();
}

using registry = class_registrator<i_tracing_backend_helper, trace_keyspace_helper, tracing&>;
static registry registrator1("trace_keyspace_helper");

}
