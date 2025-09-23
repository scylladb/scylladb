/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */
#include <seastar/core/metrics.hh>
#include "types/types.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/cql_config.hh"
#include "types/set.hh"
#include "types/map.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "utils/class_registrator.hh"
#include "service/storage_proxy.hh"

namespace tracing {

using namespace std::chrono_literals;

static logging::logger tlogger("trace_keyspace_helper");

static service::client_state& tracing_client_state() {
    static timeout_config tracing_db_timeout_config {
        5s, 5s, 5s, 5s, 5s, 5s, 5s,
    };
    static thread_local service::client_state s(service::client_state::internal_tag{}, tracing_db_timeout_config);
    return s;
}

struct trace_keyspace_backend_sesssion_state final : public backend_session_state_base {
    int64_t last_nanos = 0;
    semaphore write_sem {1};
    virtual ~trace_keyspace_backend_sesssion_state() {}
};

trace_keyspace_helper::trace_keyspace_helper(tracing& tr)
            : i_tracing_backend_helper(tr)
            , _pending_writes("trace_keyspace_helper::pending_writes")
            , _dummy_query_state(tracing_client_state(), empty_service_permit())
            , _sessions(KEYSPACE_NAME, SESSIONS,
                        fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                                  "session_id uuid,"
                                  "command text,"
                                  "client inet,"
                                  "coordinator inet,"
                                  "duration int,"
                                  "parameters map<text, text>,"
                                  "request text,"
                                  "started_at timestamp,"
                                  "request_size int,"
                                  "response_size int,"
                                  "username text,"
                                  "PRIMARY KEY ((session_id))) "
                                  "WITH default_time_to_live = 86400", KEYSPACE_NAME, SESSIONS),

                        fmt::format("INSERT INTO {}.{} ("
                                  "session_id,"
                                  "command,"
                                  "client,"
                                  "coordinator,"
                                  "duration,"
                                  "parameters,"
                                  "request,"
                                  "started_at,"
                                  "request_size,"
                                  "response_size,"
                                  "username) VALUES ("
                                  ":session_id,"
                                  ":command,"
                                  ":client,"
                                  ":coordinator,"
                                  ":duration,"
                                  ":parameters,"
                                  ":request,"
                                  ":started_at,"
                                  ":request_size,"
                                  ":response_size,"
                                  ":username) USING TTL :ttl", KEYSPACE_NAME, SESSIONS),

                        fmt::format("INSERT INTO {}.{} ("
                                  "session_id,"
                                  "command,"
                                  "client,"
                                  "coordinator,"
                                  "duration,"
                                  "parameters,"
                                  "request,"
                                  "started_at,"
                                  "request_size,"
                                  "response_size) VALUES ("
                                  ":session_id,"
                                  ":command,"
                                  ":client,"
                                  ":coordinator,"
                                  ":duration,"
                                  ":parameters,"
                                  ":request,"
                                  ":started_at,"
                                  ":request_size,"
                                  ":response_size) USING TTL :ttl", KEYSPACE_NAME, SESSIONS))

            , _sessions_time_idx(KEYSPACE_NAME, SESSIONS_TIME_IDX,
                                 fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                                           "minute timestamp,"
                                           "started_at timestamp,"
                                           "session_id uuid,"
                                           "PRIMARY KEY (minute, started_at, session_id)) "
                                           "WITH default_time_to_live = 86400", KEYSPACE_NAME, SESSIONS_TIME_IDX),

                                 fmt::format("INSERT INTO {}.{} ("
                                           "minute,"
                                           "started_at,"
                                           "session_id) VALUES (?, ?, ?) "
                                           "USING TTL ?", KEYSPACE_NAME, SESSIONS_TIME_IDX))

            , _events(KEYSPACE_NAME, EVENTS,
                      fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                                "session_id uuid,"
                                "event_id timeuuid,"
                                "activity text,"
                                "source inet,"
                                "source_elapsed int,"
                                "thread text,"
                                "scylla_parent_id bigint,"
                                "scylla_span_id bigint,"
                                "PRIMARY KEY ((session_id), event_id)) "
                                "WITH default_time_to_live = 86400", KEYSPACE_NAME, EVENTS),

                      fmt::format("INSERT INTO {}.{} ("
                                "session_id, "
                                "event_id, "
                                "activity, "
                                "source, "
                                "source_elapsed, "
                                "thread,"
                                "scylla_parent_id,"
                                "scylla_span_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                                "USING TTL ?", KEYSPACE_NAME, EVENTS))

            , _slow_query_log(KEYSPACE_NAME, NODE_SLOW_QUERY_LOG,
                              fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
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

                              fmt::format("INSERT INTO {}.{} ("
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

            , _slow_query_log_time_idx(KEYSPACE_NAME, NODE_SLOW_QUERY_LOG_TIME_IDX,
                                       fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                                                 "minute timestamp,"
                                                 "started_at timestamp,"
                                                 "session_id uuid,"
                                                 "start_time timeuuid,"
                                                 "node_ip inet,"
                                                 "shard int,"
                                                 "PRIMARY KEY (minute, started_at, session_id)) "
                                                 "WITH default_time_to_live = 86400", KEYSPACE_NAME, NODE_SLOW_QUERY_LOG_TIME_IDX),

                                       fmt::format("INSERT INTO {}.{} ("
                                                 "minute,"
                                                 "started_at,"
                                                 "session_id,"
                                                 "start_time,"
                                                 "node_ip,"
                                                 "shard) VALUES (?, ?, ?, ?, ?, ?)"
                                                 "USING TTL ?", KEYSPACE_NAME, NODE_SLOW_QUERY_LOG_TIME_IDX))
{
    namespace sm = seastar::metrics;

    _metrics.add_group("tracing_keyspace_helper", {
        sm::make_counter("tracing_errors", [this] { return _stats.tracing_errors; },
                        sm::description("Counts a number of errors during writing to a system_traces keyspace. "
                                        "One error may cause one or more tracing records to be lost.")),

        sm::make_counter("bad_column_family_errors", [this] { return _stats.bad_column_family_errors; },
                        sm::description("Counts a number of times write failed due to one of the tables in the system_traces keyspace has an incompatible schema. "
                                        "One error may result one or more tracing records to be lost. "
                                        "Non-zero value indicates that the administrator has to take immediate steps to fix the corresponding schema. "
                                        "The appropriate error message will be printed in the syslog.")),
    });
}

future<> trace_keyspace_helper::start(cql3::query_processor& qp, service::migration_manager& mm) {
    _qp_anchor = &qp;
    _mm_anchor = &mm;
    return table_helper::setup_keyspace(qp, mm, KEYSPACE_NAME, "org.apache.cassandra.locator.SimpleStrategy", "2", _dummy_query_state,
            { &_sessions, &_sessions_time_idx, &_events, &_slow_query_log, &_slow_query_log_time_idx }, std::nullopt);
}

gms::inet_address trace_keyspace_helper::my_address() const noexcept {
    return _qp_anchor->proxy().my_address();
}

void trace_keyspace_helper::write_one_session_records(lw_shared_ptr<one_session_records> records) {
    // Future is waited on indirectly in `stop()` (via `_pending_writes`).
    (void)with_gate(_pending_writes, [this, records = std::move(records)] {
        auto num_records = records->size();
        return this->flush_one_session_mutations(std::move(records)).finally([this, num_records] { _local_tracing.write_complete(num_records); });
    }).handle_exception([this] (auto ep) {
        try {
            ++_stats.tracing_errors;
            std::rethrow_exception(ep);
        } catch (exceptions::overloaded_exception&) {
            tlogger.warn("Too many nodes are overloaded to save trace events");
        } catch (bad_column_family& e) {
            if (_stats.bad_column_family_errors++ % bad_column_family_message_period == 0) {
                tlogger.warn("Tracing is enabled but {}", e.what());
            }
        } catch (std::logic_error& e) {
            tlogger.error("{}", e.what());
        } catch (...) {
            // TODO: Handle some more exceptions maybe?
        }
    }).discard_result();
}

void trace_keyspace_helper::write_records_bulk(records_bulk& bulk) {
    tlogger.trace("Writing {} sessions", bulk.size());
    std::for_each(bulk.begin(), bulk.end(), [this] (records_bulk::value_type& one_session_records_ptr) {
        write_one_session_records(std::move(one_session_records_ptr));
    });
}

cql3::query_options trace_keyspace_helper::make_session_mutation_data(gms::inet_address my_address, const one_session_records& session_records) {
    const session_record& record = session_records.session_rec;
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(record.started_at.time_since_epoch()).count();
    std::vector<std::pair<data_value, data_value>> parameters_values_vector;
    parameters_values_vector.reserve(record.parameters.size());
    std::for_each(record.parameters.begin(), record.parameters.end(), [&parameters_values_vector] (auto& val_pair) { parameters_values_vector.emplace_back(val_pair.first, val_pair.second); });
    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
    std::vector<std::string_view> names {
        "session_id",
        "command",
        "client",
        "coordinator",
        "duration",
        "parameters",
        "request",
        "started_at",
        "request_size",
        "response_size",
        "username",
        "ttl"
    };
    std::vector<cql3::raw_value> values {
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(utf8_type->decompose(type_to_string(record.command))),
        cql3::raw_value::make_value(inet_addr_type->decompose(record.client.addr())),
        cql3::raw_value::make_value(inet_addr_type->decompose(my_address.addr())),
        cql3::raw_value::make_value(int32_type->decompose(elapsed_to_micros(record.elapsed))),
        cql3::raw_value::make_value(make_map_value(my_map_type, map_type_impl::native_type(std::move(parameters_values_vector))).serialize()),
        cql3::raw_value::make_value(utf8_type->decompose(record.request)),
        cql3::raw_value::make_value(timestamp_type->decompose(millis_since_epoch)),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(record.request_size))),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(record.response_size))),
        cql3::raw_value::make_value(utf8_type->decompose(record.username)),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(session_records.ttl.count())))
    };

    return cql3::query_options(cql3::default_cql_config,
            db::consistency_level::ANY, std::move(names), std::move(values), false, cql3::query_options::specific_options::DEFAULT);
}

cql3::query_options trace_keyspace_helper::make_session_time_idx_mutation_data(gms::inet_address my_address, const one_session_records& session_records) {
    auto started_at_duration = session_records.session_rec.started_at.time_since_epoch();
    // timestamp in minutes when the query began
    auto minutes_in_millis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration_cast<std::chrono::minutes>(started_at_duration)).count();
    // timestamp when the query began
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(started_at_duration).count();

    std::vector<cql3::raw_value> values {
        cql3::raw_value::make_value(timestamp_type->decompose(minutes_in_millis)),
        cql3::raw_value::make_value(timestamp_type->decompose(millis_since_epoch)),
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(int32_type->decompose(int32_t(session_records.ttl.count())))
    };

    return cql3::query_options(cql3::default_cql_config,
            db::consistency_level::ANY, std::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT);
}

cql3::query_options trace_keyspace_helper::make_slow_query_mutation_data(gms::inet_address my_address, const one_session_records& session_records, const utils::UUID& start_time_id) {
    const session_record& record = session_records.session_rec;
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(record.started_at.time_since_epoch()).count();

    // query command is stored on a parameters map with a 'query' key
    const auto query_str_it = record.parameters.find("query");
    if (query_str_it == record.parameters.end()) {
        tlogger.trace("No \"query\" parameter set for a session requesting a slow_query_log record");
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
        cql3::raw_value::make_value(inet_addr_type->decompose(my_address.addr())),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(this_shard_id()))),
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(timestamp_type->decompose(millis_since_epoch)),
        cql3::raw_value::make_value(timeuuid_type->decompose(start_time_id)),
        query_str_it != record.parameters.end()
                ? cql3::raw_value::make_value(utf8_type->decompose(query_str_it->second))
                : cql3::raw_value::make_null(),
        cql3::raw_value::make_value(int32_type->decompose(elapsed_to_micros(record.elapsed))),
        cql3::raw_value::make_value(make_map_value(my_map_type, map_type_impl::native_type(std::move(parameters_values_vector))).serialize()),
        cql3::raw_value::make_value(inet_addr_type->decompose(record.client.addr())),
        cql3::raw_value::make_value(make_set_value(my_set_type, set_type_impl::native_type(std::move(tables_names_vector))).serialize()),
        cql3::raw_value::make_value(utf8_type->decompose(record.username)),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(record.slow_query_record_ttl.count())))
    });

    return cql3::query_options(cql3::default_cql_config,
            db::consistency_level::ANY, std::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT);
}

cql3::query_options trace_keyspace_helper::make_slow_query_time_idx_mutation_data(gms::inet_address my_address, const one_session_records& session_records, const utils::UUID& start_time_id) {
    auto started_at_duration = session_records.session_rec.started_at.time_since_epoch();
    // timestamp in minutes when the query began
    auto minutes_in_millis = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration_cast<std::chrono::minutes>(started_at_duration)).count();
    // timestamp when the query began
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(started_at_duration).count();

    std::vector<cql3::raw_value> values({
        cql3::raw_value::make_value(timestamp_type->decompose(minutes_in_millis)),
        cql3::raw_value::make_value(timestamp_type->decompose(millis_since_epoch)),
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(timeuuid_type->decompose(start_time_id)),
        cql3::raw_value::make_value(inet_addr_type->decompose(my_address.addr())),
        cql3::raw_value::make_value(int32_type->decompose(int32_t(this_shard_id()))),
        cql3::raw_value::make_value(int32_type->decompose(int32_t(session_records.session_rec.slow_query_record_ttl.count())))
    });

    return cql3::query_options(cql3::default_cql_config,
            db::consistency_level::ANY, std::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT);
}

std::vector<cql3::raw_value> trace_keyspace_helper::make_event_mutation_data(gms::inet_address my_address, one_session_records& session_records, const event_record& record) {
    auto backend_state_ptr = static_cast<trace_keyspace_backend_sesssion_state*>(session_records.backend_state_ptr.get());

    std::vector<cql3::raw_value> values({
        cql3::raw_value::make_value(uuid_type->decompose(session_records.session_id)),
        cql3::raw_value::make_value(timeuuid_type->decompose(utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(backend_state_ptr->last_nanos, record.event_time_point)))),
        cql3::raw_value::make_value(utf8_type->decompose(record.message)),
        cql3::raw_value::make_value(inet_addr_type->decompose(my_address.addr())),
        cql3::raw_value::make_value(int32_type->decompose(elapsed_to_micros(record.elapsed))),
        cql3::raw_value::make_value(utf8_type->decompose(fmt::format("{}/{}", _local_tracing.get_thread_name(), record.scheduling_group_name))),
        cql3::raw_value::make_value(long_type->decompose(int64_t(session_records.parent_id.get_id()))),
        cql3::raw_value::make_value(long_type->decompose(int64_t(session_records.my_span_id.get_id()))),
        cql3::raw_value::make_value(int32_type->decompose((int32_t)(session_records.ttl.count())))
    });

    return values;
}

future<> trace_keyspace_helper::apply_events_mutation(cql3::query_processor& qp, service::migration_manager& mm, lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records) {
    if (events_records.empty()) {
        return now();
    }

    return _events.cache_table_info(qp, mm, _dummy_query_state).then([this, &qp, records, &events_records] {
        tlogger.trace("{}: storing {} events records: parent_id {} span_id {}", records->session_id, events_records.size(), records->parent_id, records->my_span_id);

        std::vector<cql3::statements::batch_statement::single_statement> modifications(events_records.size(), cql3::statements::batch_statement::single_statement(_events.insert_stmt(), false));
        std::vector<cql3::raw_value_vector_with_unset> values;

        values.reserve(events_records.size());
        std::for_each(events_records.begin(), events_records.end(), [&values, all_records = records, this] (event_record& one_event_record) { values.emplace_back(make_event_mutation_data(my_address(), *all_records, one_event_record)); });

        return do_with(
            cql3::query_options::make_batch_options(cql3::query_options(cql3::default_cql_config, db::consistency_level::ANY, std::nullopt, std::vector<cql3::raw_value>{}, false, cql3::query_options::specific_options::DEFAULT), std::move(values)),
            cql3::statements::batch_statement(cql3::statements::batch_statement::type::UNLOGGED, std::move(modifications), cql3::attributes::none(), qp.get_cql_stats()),
            [this, &qp] (auto& batch_options, auto& batch) {
                return batch.execute(qp, _dummy_query_state, batch_options, std::nullopt).then([] (shared_ptr<cql_transport::messages::result_message> res) { return now(); });
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
            // This code is inside the _pending_writes gate and the qp pointer
            // is cleared on ::stop() after the gate is closed.
            SCYLLA_ASSERT(_qp_anchor != nullptr && _mm_anchor != nullptr);
            cql3::query_processor& qp = *_qp_anchor;
            service::migration_manager& mm = *_mm_anchor;
            return apply_events_mutation(qp, mm, records, events_records).then([this, &qp, &mm, session_record_is_ready, records] {
                if (session_record_is_ready) {

                    // if session is finished - store a session and a session time index entries
                    tlogger.trace("{}: going to store a session event", records->session_id);
                    return _sessions.insert(qp, mm, _dummy_query_state, make_session_mutation_data, my_address(), std::ref(*records)).then([this, &qp, &mm, records] {
                        tlogger.trace("{}: going to store a {} entry", records->session_id, _sessions_time_idx.name());
                        return _sessions_time_idx.insert(qp, mm, _dummy_query_state, make_session_time_idx_mutation_data, my_address(), std::ref(*records));
                    }).then([this, &qp, &mm, records] {
                        if (!records->do_log_slow_query) {
                            return now();
                        }

                        // if slow query log is requested - store a slow query log and a slow query log time index entries
                        auto start_time_id = utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(_slow_query_last_nanos, records->session_rec.started_at));
                        tlogger.trace("{}: going to store a slow query event", records->session_id);
                        return _slow_query_log.insert(qp, mm, _dummy_query_state, make_slow_query_mutation_data, my_address(), std::ref(*records), start_time_id).then([this, &qp, &mm, records, start_time_id] {
                            tlogger.trace("{}: going to store a {} entry", records->session_id, _slow_query_log_time_idx.name());
                            return _slow_query_log_time_idx.insert(qp, mm, _dummy_query_state, make_slow_query_time_idx_mutation_data, my_address(), std::ref(*records), start_time_id);
                        });
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

using registry_default = class_registrator<i_tracing_backend_helper, trace_keyspace_helper, tracing&>;
static registry_default registrator_default("trace_keyspace_helper");

}
