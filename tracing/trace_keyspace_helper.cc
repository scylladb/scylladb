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

const sstring trace_keyspace_helper::NODE_SLOW_QUERY_LOG("node_slow_log");

struct trace_keyspace_backend_sesssion_state final : public backend_session_state_base {
    int64_t last_nanos = 0;
    semaphore write_sem {1};
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

        _node_slow_query_log_cql = sprint("CREATE TABLE %s.%s ("
                                    "node_ip inet,"
                                    "shard int,"
                                    "session_id uuid,"
                                    "date timestamp,"
                                    "start_time timeuuid,"
                                    "command text,"
                                    "duration int,"
                                    "parameters map<text, text>,"
                                    "source_ip inet,"
                                    "table_names set<text>,"
                                    "username text,"
                                    "PRIMARY KEY (start_time, node_ip, shard)) "
                                    "WITH default_time_to_live = 86400", KEYSPACE_NAME, NODE_SLOW_QUERY_LOG);
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

    if (check_column_definition(session_id_column, uuid_type) &&
        check_column_definition(_client_column, inet_addr_type) &&
        check_column_definition(_coordinator_column, inet_addr_type) &&
        check_column_definition(_request_column, utf8_type) &&
        check_column_definition(_started_at_column, timestamp_type) &&
        check_column_definition(_command_column, utf8_type) &&
        check_column_definition(_duration_column, int32_type) &&
        check_column_definition(_parameters_column, map_type_impl::get_instance(utf8_type, utf8_type, true))) {
        // store a table ID only if its format meets our demands
        _sessions_id = schema->id();
        return true;
    } else {
        _sessions_id = utils::UUID();
        return false;
    }
}

bool trace_keyspace_helper::cache_node_slow_log_table_handles(const schema_ptr& schema) {
    auto node_ip_column = schema->get_column_definition("node_ip");
    auto shard_column = schema->get_column_definition("shard");
    _slow_session_id_column = schema->get_column_definition("session_id");
    _slow_date_column = schema->get_column_definition("date");
    auto start_time_column = schema->get_column_definition("start_time");
    _slow_command_column = schema->get_column_definition("command");
    _slow_duration_column = schema->get_column_definition("duration");
    _slow_parameters_column = schema->get_column_definition("parameters");
    _slow_source_ip_column = schema->get_column_definition("source_ip");
    _slow_table_names_column = schema->get_column_definition("table_names");
    _slow_username_column = schema->get_column_definition("username");

    if (check_column_definition(node_ip_column, inet_addr_type) &&
        check_column_definition(shard_column, int32_type) &&
        check_column_definition(_slow_session_id_column, uuid_type) &&
        check_column_definition(_slow_date_column, timestamp_type) &&
        check_column_definition(start_time_column, timeuuid_type) &&
        check_column_definition(_slow_command_column, utf8_type) &&
        check_column_definition(_slow_duration_column, int32_type) &&
        check_column_definition(_slow_parameters_column, map_type_impl::get_instance(utf8_type, utf8_type, true)) &&
        check_column_definition(_slow_source_ip_column, inet_addr_type) &&
        check_column_definition(_slow_table_names_column, set_type_impl::get_instance(utf8_type, true)) &&
        check_column_definition(_slow_username_column, utf8_type)) {
        // store a table ID only if its format meets our demands
        _slow_query_log_id = schema->id();
        return true;
    } else {
        _slow_query_log_id = utils::UUID();
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

    if (check_column_definition(session_id_column, uuid_type) &&
        check_column_definition(event_id_column, timeuuid_type) &&
        check_column_definition(_activity_column, utf8_type) &&
        check_column_definition(_source_column, inet_addr_type) &&
        check_column_definition(_thread_column, utf8_type) &&
        check_column_definition(_source_elapsed_column, int32_type)) {
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
            setup_table(NODE_SLOW_QUERY_LOG, _node_slow_query_log_cql).get();
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
        } catch (std::logic_error& e) {
            logger.error(e.what());
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
    gc_clock::duration ttl = session_records.ttl;
    const session_record& record = session_records.session_rec;
    auto timestamp = api::new_timestamp();
    mutation m(key, schema);
    auto& cells = m.partition().clustered_row(clustering_key::make_empty(*schema)).cells();

    cells.apply(*_client_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(record.client.addr()), ttl));
    cells.apply(*_coordinator_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr()), ttl));
    cells.apply(*_request_column, atomic_cell::make_live(timestamp, utf8_type->decompose(record.request), ttl));
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(record.started_at.time_since_epoch()).count();
    cells.apply(*_started_at_column, atomic_cell::make_live(timestamp, timestamp_type->decompose(millis_since_epoch), ttl));
    cells.apply(*_command_column, atomic_cell::make_live(timestamp, utf8_type->decompose(type_to_string(record.command)), ttl));
    cells.apply(*_duration_column, atomic_cell::make_live(timestamp, int32_type->decompose(elapsed_to_micros(record.elapsed)), ttl));

    std::vector<std::pair<bytes, atomic_cell>> map_cell;
    for (auto& param_pair : record.parameters) {
        map_cell.emplace_back(utf8_type->decompose(param_pair.first), atomic_cell::make_live(timestamp, utf8_type->decompose(param_pair.second), ttl));
    }

    map_type_impl::mutation map_mutation{{}, map_cell};
    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
    cells.apply(*_parameters_column, my_map_type->serialize_mutation_form(map_mutation));

    return m;
}

mutation trace_keyspace_helper::make_slow_query_mutation(const one_session_records& session_records) {
    schema_ptr schema = get_schema_ptr_or_create(_slow_query_log_id, NODE_SLOW_QUERY_LOG, _node_slow_query_log_cql,
                                                 [this] (const schema_ptr& s) { return cache_node_slow_log_table_handles(s); });

    const session_record& record = session_records.session_rec;

    auto key = partition_key::from_singular(*schema, utils::UUID_gen::get_time_UUID(make_monotonic_UUID_tp(_slow_query_last_nanos, record.started_at)));
    auto timestamp = api::new_timestamp();
    gc_clock::duration ttl = record.slow_query_record_ttl;
    mutation m(key, schema);

    std::vector<bytes> full_components;
    full_components.reserve(2);
    full_components.emplace_back(inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr()));
    full_components.emplace_back(int32_type->decompose((int32_t)(engine().cpu_id())));
    auto& cells = m.partition().clustered_row(clustering_key::from_exploded(*schema, full_components)).cells();

    // the corresponding tracing session ID
    cells.apply(*_slow_session_id_column, atomic_cell::make_live(timestamp, uuid_type->decompose(session_records.session_id), ttl));

    // timestamp when the query began
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(record.started_at.time_since_epoch()).count();
    cells.apply(*_slow_date_column, atomic_cell::make_live(timestamp, timestamp_type->decompose(millis_since_epoch), ttl));

    // query command is stored on a parameters map with a 'query' key
    auto it = record.parameters.find("query");
    if (it == record.parameters.end()) {
        throw std::logic_error("No \"query\" parameter set for a session requesting a slow_query_log record");
    }
    cells.apply(*_slow_command_column, atomic_cell::make_live(timestamp, utf8_type->decompose(it->second), ttl));

    // query duration
    cells.apply(*_slow_duration_column, atomic_cell::make_live(timestamp, int32_type->decompose(elapsed_to_micros(record.elapsed)), ttl));

    // parameters map
    std::vector<std::pair<bytes, atomic_cell>> map_cell;
    for (auto& param_pair : record.parameters) {
        map_cell.emplace_back(utf8_type->decompose(param_pair.first), atomic_cell::make_live(timestamp, utf8_type->decompose(param_pair.second), ttl));
    }

    map_type_impl::mutation map_mutation{{}, map_cell};
    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
    cells.apply(*_slow_parameters_column, my_map_type->serialize_mutation_form(map_mutation));

    // IP of a Client that sent this query
    cells.apply(*_slow_source_ip_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(record.client.addr()), ttl));

    // set of tables involved in this query
    std::vector<std::pair<bytes, atomic_cell>> set_cell;
    for (auto& table_name : record.tables) {
        set_cell.emplace_back(utf8_type->decompose(table_name), atomic_cell::make_live(timestamp, utf8_type->decompose(table_name), ttl));
    }

    set_type_impl::mutation set_mutation{{}, set_cell};
    auto my_set_type = set_type_impl::get_instance(utf8_type, true);
    cells.apply(*_slow_table_names_column, my_set_type->serialize_mutation_form(set_mutation));

    // user name used for this query
    cells.apply(*_slow_username_column, atomic_cell::make_live(timestamp, utf8_type->decompose(record.username), ttl));

    return m;
}

mutation trace_keyspace_helper::make_event_mutation(one_session_records& session_records, const event_record& record) {
    schema_ptr schema = get_schema_ptr_or_create(_events_id, EVENTS, _events_create_cql,
                                                 [this] (const schema_ptr& s) { return cache_events_table_handles(s); });

    auto key = partition_key::from_singular(*schema, session_records.session_id);
    gc_clock::duration ttl = session_records.ttl;
    auto backend_state_ptr = static_cast<trace_keyspace_backend_sesssion_state*>(session_records.backend_state_ptr.get());
    int64_t& last_event_nanos = backend_state_ptr->last_nanos;
    auto timestamp = api::new_timestamp();
    mutation m(key, schema);
    auto& cells = m.partition().clustered_row(clustering_key::from_singular(*schema, utils::UUID_gen::get_time_UUID(make_monotonic_UUID_tp(last_event_nanos, record.event_time_point)))).cells();

    cells.apply(*_activity_column, atomic_cell::make_live(timestamp, utf8_type->decompose(record.message), ttl));
    cells.apply(*_source_column, atomic_cell::make_live(timestamp, inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr()), ttl));
    cells.apply(*_thread_column, atomic_cell::make_live(timestamp, utf8_type->decompose(_local_tracing.get_thread_name()), ttl));
    cells.apply(*_source_elapsed_column, atomic_cell::make_live(timestamp, int32_type->decompose(elapsed_to_micros(record.elapsed)), ttl));

    return m;
}

future<> trace_keyspace_helper::apply_events_mutation(lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records) {
    if (events_records.empty()) {
        return make_ready_future<>();
    }

    logger.trace("{}: storing {} events records", records->session_id, events_records.size());

    mutation m(make_event_mutation(*records, *events_records.begin()));

    return do_with(std::move(m), [this, records, &events_records] (mutation& m) {
        return do_for_each(std::next(events_records.begin()), events_records.end(), [this, &m, all_records = records] (event_record& one_event_record) {
            m.apply(make_event_mutation(*all_records, one_event_record));
            return make_ready_future<>();
        }).then([&m] {
            return service::get_local_storage_proxy().mutate({std::move(m)}, db::consistency_level::ANY, nullptr);
        });
    });
}

future<> trace_keyspace_helper::flush_one_session_mutations(lw_shared_ptr<one_session_records> records) {
    // grab events records available so far
    return do_with(std::move(records->events_recs), [this, records] (std::deque<event_record>& events_records) {
        records->events_recs.clear();

        // Check if a session's record is ready before handling events' records.
        //
        // New event's records and a session's record may become ready while a
        // mutation with the current events' records is being written. We don't want
        // to allow the situation when a session's record is written before the last
        // event record from the same session.
        bool session_record_is_ready = records->session_rec.ready();

        // From this point on - all new data will have to be handled in the next write event
        records->data_consumed();

        // We want to serialize the creation of events mutations in order to ensure
        // that mutations the events that were created first are going to be created
        // first too.
        auto backend_state_ptr = static_cast<trace_keyspace_backend_sesssion_state*>(records->backend_state_ptr.get());
        semaphore& write_sem = backend_state_ptr->write_sem;
        return with_semaphore(write_sem, 1, [this, records, session_record_is_ready, &events_records] {
            return apply_events_mutation(records, events_records).then([this, session_record_is_ready, records] {
                if (session_record_is_ready) {
                    logger.trace("{}: storing a session event", records->session_id);
                    return service::get_local_storage_proxy().mutate({make_session_mutation(*records)}, db::consistency_level::ANY, nullptr).then([this, records] {
                        if (!records->do_log_slow_query) {
                            return make_ready_future<>();
                        }

                        logger.trace("{}: storing a slow query event", records->session_id);
                        return service::get_local_storage_proxy().mutate({make_slow_query_mutation(*records)}, db::consistency_level::ANY, nullptr);
                    });
                } else {
                    return make_ready_future<>();
                }
            });
        }).finally([records] {});
    });
}

std::unique_ptr<backend_session_state_base> trace_keyspace_helper::allocate_session_state() const {
    return std::make_unique<trace_keyspace_backend_sesssion_state>();
}

using registry = class_registrator<i_tracing_backend_helper, trace_keyspace_helper, tracing&>;
static registry registrator1("trace_keyspace_helper");

}
