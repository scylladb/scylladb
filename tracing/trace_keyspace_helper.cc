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
#include <seastar/core/metrics.hh>
#include "types.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "service/migration_manager.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/batch_statement.hh"

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
            , _dummy_query_state(service::client_state(service::client_state::external_tag{}))
            , _sessions(SESSIONS, *this,
                        sprint("CREATE TABLE IF NOT EXISTS %s.%s ("
                                  "session_id uuid,"
                                  "command text,"
                                  "client inet,"
                                  "coordinator inet,"
                                  "duration int,"
                                  "parameters map<text, text>,"
                                  "request text,"
                                  "started_at timestamp,"
                                  "PRIMARY KEY ((session_id))) "
                                  "WITH default_time_to_live = 86400", KEYSPACE_NAME, SESSIONS),

                        sprint("INSERT INTO %s.%s ("
                                  "session_id,"
                                  "command,"
                                  "client,"
                                  "coordinator,"
                                  "duration,"
                                  "parameters,"
                                  "request,"
                                  "started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                                  "USING TTL ?", KEYSPACE_NAME, SESSIONS))
            , _events(EVENTS, *this,
                      sprint("CREATE TABLE IF NOT EXISTS %s.%s ("
                                "session_id uuid,"
                                "event_id timeuuid,"
                                "activity text,"
                                "source inet,"
                                "source_elapsed int,"
                                "thread text,"
                                "PRIMARY KEY ((session_id), event_id)) "
                                "WITH default_time_to_live = 86400", KEYSPACE_NAME, EVENTS),

                      sprint("INSERT INTO %s.%s ("
                                "session_id, "
                                "event_id, "
                                "activity, "
                                "source, "
                                "source_elapsed, "
                                "thread) VALUES (?, ?, ?, ?, ?, ?) "
                                "USING TTL ?", KEYSPACE_NAME, EVENTS))

            , _slow_query_log(NODE_SLOW_QUERY_LOG, *this,
                              sprint("CREATE TABLE IF NOT EXISTS %s.%s ("
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
                                        "WITH default_time_to_live = 86400", KEYSPACE_NAME, NODE_SLOW_QUERY_LOG),

                              sprint("INSERT INTO %s.%s ("
                                        "node_ip,"
                                        "shard,"
                                        "session_id,"
                                        "date,"
                                        "start_time,"
                                        "command,"
                                        "duration,"
                                        "parameters,"
                                        "source_ip,"
                                        "table_names,"
                                        "username) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                                        "USING TTL ?", KEYSPACE_NAME, NODE_SLOW_QUERY_LOG))
{
    namespace sm = seastar::metrics;

    _metrics.add_group("tracing_keyspace_helper", {
        sm::make_derive("tracing_errors", [this] { return _stats.tracing_errors; },
                        sm::description("Counts a number of errors during writing to a system_traces keyspace. "
                                        "One error may cause one or more tracing records to be lost.")),

        sm::make_derive("bad_column_family_errors", [this] { return _stats.bad_column_family_errors; },
                        sm::description("Counts a number of times write failed due to one of the tables in the system_traces keyspace has an incompatible schema. "
                                        "One error may result one or more tracing records to be lost. "
                                        "Non-zero value indicates that the administrator has to take immediate steps to fix the corresponding schema. "
                                        "The appropriate error message will be printed in the syslog.")),
    });
}

future<> trace_keyspace_helper::table_helper::setup_table() const {
    auto& qp = cql3::get_local_query_processor();
    auto& db = qp.db().local();

    if (db.has_schema(KEYSPACE_NAME, _name)) {
        return make_ready_future<>();
    }

    ::shared_ptr<cql3::statements::raw::cf_statement> parsed = static_pointer_cast<
                    cql3::statements::raw::cf_statement>(cql3::query_processor::parse_statement(_create_cql));
    parsed->prepare_keyspace(KEYSPACE_NAME);
    ::shared_ptr<cql3::statements::create_table_statement> statement =
                    static_pointer_cast<cql3::statements::create_table_statement>(
                                    parsed->prepare(db, qp.get_cql_stats())->statement);
    auto schema = statement->get_cf_meta_data();

    // Generate the CF UUID based on its KF names. This is needed to ensure that
    // all Nodes that create it would create it with the same UUID and we don't
    // hit the #420 issue.
    auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);

    // We don't care it it fails really - this may happen due to concurrent
    // "CREATE TABLE" invocation on different Nodes.
    // The important thing is that it will converge eventually (some traces may
    // be lost in a process but that's ok).
    return service::get_local_migration_manager().announce_new_column_family(b.build(), false).discard_result().handle_exception([this] (auto ep) {});;
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
                // We use min_timestamp so that default keyspace metadata will loose with any manual adjustments. See issue #2129.
                service::get_local_migration_manager().announce_new_keyspace(ksm, api::min_timestamp, false).get();
            }

            _dummy_query_state.get_client_state().set_keyspace(cql3::get_local_query_processor().db(), KEYSPACE_NAME);

            // Create tables
            _sessions.setup_table().get();
            _events.setup_table().get();
            _slow_query_log.setup_table().get();
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

cql3::query_options trace_keyspace_helper::make_session_mutation_data(const one_session_records& session_records) {
    const session_record& record = session_records.session_rec;
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(record.started_at.time_since_epoch()).count();
    std::vector<std::pair<data_value, data_value>> parameters_values_vector;
    parameters_values_vector.reserve(record.parameters.size());
    std::for_each(record.parameters.begin(), record.parameters.end(), [&parameters_values_vector] (auto& val_pair) { parameters_values_vector.emplace_back(val_pair.first, val_pair.second); });
    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);

    std::vector<cql3::raw_value> values {
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(utf8_type->decompose(type_to_string(record.command))),
        cql3::raw_value::make_value(inet_addr_type->decompose(record.client.addr())),
        cql3::raw_value::make_value(inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr())),
        cql3::raw_value::make_value(int32_type->decompose(elapsed_to_micros(record.elapsed))),
        cql3::raw_value::make_value(make_map_value(my_map_type, map_type_impl::native_type(std::move(parameters_values_vector))).serialize()),
        cql3::raw_value::make_value(utf8_type->decompose(record.request)),
        cql3::raw_value::make_value(timestamp_type->decompose(millis_since_epoch)),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(session_records.ttl.count())))
    };

    return cql3::query_options(db::consistency_level::ANY, std::experimental::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest());
}

cql3::query_options trace_keyspace_helper::make_slow_query_mutation_data(const one_session_records& session_records, const utils::UUID& start_time_id) {
    const session_record& record = session_records.session_rec;
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(record.started_at.time_since_epoch()).count();

    // query command is stored on a parameters map with a 'query' key
    auto query_str_it = record.parameters.find("query");
    if (query_str_it == record.parameters.end()) {
        throw std::logic_error("No \"query\" parameter set for a session requesting a slow_query_log record");
    }

    // parameters map
    std::vector<std::pair<data_value, data_value>> parameters_values_vector;
    parameters_values_vector.reserve(record.parameters.size());
    std::for_each(record.parameters.begin(), record.parameters.end(), [&parameters_values_vector] (auto& val_pair) { parameters_values_vector.emplace_back(val_pair.first, val_pair.second); });
    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);

    // set of tables involved in this query
    std::vector<data_value> tables_names_vector;
    tables_names_vector.reserve(record.tables.size());
    std::for_each(record.tables.begin(), record.tables.end(), [&tables_names_vector] (auto& val) { tables_names_vector.emplace_back(val); });
    auto my_set_type = set_type_impl::get_instance(utf8_type, true);

    std::vector<cql3::raw_value> values({
        cql3::raw_value::make_value(inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr())),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(engine().cpu_id()))),
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(timestamp_type->decompose(millis_since_epoch)),
        cql3::raw_value::make_value(timeuuid_type->decompose(start_time_id)),
        cql3::raw_value::make_value(utf8_type->decompose(query_str_it->second)),
        cql3::raw_value::make_value(int32_type->decompose(elapsed_to_micros(record.elapsed))),
        cql3::raw_value::make_value(make_map_value(my_map_type, map_type_impl::native_type(std::move(parameters_values_vector))).serialize()),
        cql3::raw_value::make_value(inet_addr_type->decompose(record.client.addr())),
        cql3::raw_value::make_value(make_set_value(my_set_type, set_type_impl::native_type(std::move(tables_names_vector))).serialize()),
        cql3::raw_value::make_value(utf8_type->decompose(record.username)),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(record.slow_query_record_ttl.count())))
    });

    return cql3::query_options(db::consistency_level::ANY, std::experimental::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest());
}

std::vector<cql3::raw_value> trace_keyspace_helper::make_event_mutation_data(one_session_records& session_records, const event_record& record) {
    auto backend_state_ptr = static_cast<trace_keyspace_backend_sesssion_state*>(session_records.backend_state_ptr.get());

    std::vector<cql3::raw_value> values({
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(timeuuid_type->decompose(utils::UUID_gen::get_time_UUID(make_monotonic_UUID_tp(backend_state_ptr->last_nanos, record.event_time_point)))),
        cql3::raw_value::make_value(utf8_type->decompose(record.message)),
        cql3::raw_value::make_value(inet_addr_type->decompose(utils::fb_utilities::get_broadcast_address().addr())),
        cql3::raw_value::make_value(int32_type->decompose(elapsed_to_micros(record.elapsed))),
        cql3::raw_value::make_value(utf8_type->decompose(_local_tracing.get_thread_name())),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(session_records.ttl.count())))
    });

    return values;
}

future<> trace_keyspace_helper::apply_events_mutation(lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records) {
    if (events_records.empty()) {
        return now();
    }

    return _events.cache_table_info().then([this, records, &events_records] {
        logger.trace("{}: storing {} events records", records->session_id, events_records.size());

        std::vector<shared_ptr<cql3::statements::modification_statement>> modifications(events_records.size(), _events.insert_stmt());
        std::vector<std::vector<cql3::raw_value>> values;
        auto& qp = cql3::get_local_query_processor();

        values.reserve(events_records.size());
        std::for_each(events_records.begin(), events_records.end(), [&values, all_records = records, this] (event_record& one_event_record) { values.emplace_back(make_event_mutation_data(*all_records, one_event_record)); });

        return do_with(
            cql3::query_options::make_batch_options(cql3::query_options(db::consistency_level::ANY, std::experimental::nullopt, std::vector<cql3::raw_value>{}, false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest()), std::move(values)),
            cql3::statements::batch_statement(cql3::statements::batch_statement::type::UNLOGGED, std::move(modifications), cql3::attributes::none(), qp.get_cql_stats()),
            [this] (auto& batch_options, auto& batch) {
                return batch.execute(service::get_storage_proxy(), _dummy_query_state, batch_options).then([] (shared_ptr<transport::messages::result_message> res) { return now(); });
            }
        );
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
        // that mutations for events that were created first are going to be
        // created first too.
        auto backend_state_ptr = static_cast<trace_keyspace_backend_sesssion_state*>(records->backend_state_ptr.get());
        semaphore& write_sem = backend_state_ptr->write_sem;
        return with_semaphore(write_sem, 1, [this, records, session_record_is_ready, &events_records] {
            return apply_events_mutation(records, events_records).then([this, session_record_is_ready, records] {
                if (session_record_is_ready) {

                    // if session is finished - store a session and a session time index entries
                    logger.trace("{}: going to store a session event", records->session_id);
                    return _sessions.insert(make_session_mutation_data, *records).then([this, records] {
                        if (!records->do_log_slow_query) {
                            return now();
                        }
                        auto start_time_id = utils::UUID_gen::get_time_UUID(make_monotonic_UUID_tp(_slow_query_last_nanos, records->session_rec.started_at));
                        logger.trace("{}: going to store a slow query event", records->session_id);
                        return _slow_query_log.insert(make_slow_query_mutation_data, *records, start_time_id);
                    });
                } else {
                    return now();
                }
            });
        }).finally([records] {});
    });
}

std::unique_ptr<backend_session_state_base> trace_keyspace_helper::allocate_session_state() const {
    return std::make_unique<trace_keyspace_backend_sesssion_state>();
}

future<> trace_keyspace_helper::table_helper::cache_table_info() {
    if (_prepared_stmt) {
        return now();
    } else {
        // if prepared statement has been invalidated - drop cached pointers
        _insert_stmt = nullptr;
    }

    return cql3::get_local_query_processor().prepare(_insert_cql, _ks_helper.get_dummy_qs().get_client_state(), false).then([this] (shared_ptr<transport::messages::result_message::prepared> msg_ptr) {
        _prepared_stmt = std::move(msg_ptr->get_prepared());
        shared_ptr<cql3::cql_statement> cql_stmt = _prepared_stmt->statement;
        _insert_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(cql_stmt);
    }).handle_exception([this] (auto eptr) {
        // One of the possible causes for an error here could be the table that doesn't exist.
        this->setup_table().discard_result();

        // We throw the bad_column_family exception because the caller
        // expects and accounts this type of errors.
        try {
            std::rethrow_exception(eptr);
        } catch (std::exception& e) {
            throw bad_column_family(_name, e);
        } catch (...) {
            throw bad_column_family(_name);
        }
    });
}

using registry = class_registrator<i_tracing_backend_helper, trace_keyspace_helper, tracing&>;
static registry registrator1("trace_keyspace_helper");

}
