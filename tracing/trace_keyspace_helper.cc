/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */
#include <seastar/core/metrics.hh>
#include "types/types.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/query_processor.hh"
#include "mutation/mutation.hh"
#include "mutation/collection_mutation.hh"
#include "db_clock.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/cql_config.hh"
#include "types/set.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
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
            , _sessions_cql(fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
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
                                  "WITH default_time_to_live = 86400", KEYSPACE_NAME, SESSIONS))

            , _sessions_time_idx_cql(fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                                           "minute timestamp,"
                                           "started_at timestamp,"
                                           "session_id uuid,"
                                           "PRIMARY KEY (minute, started_at, session_id)) "
                                           "WITH default_time_to_live = 86400", KEYSPACE_NAME, SESSIONS_TIME_IDX))

            , _events_cql(fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                                "session_id uuid,"
                                "event_id timeuuid,"
                                "activity text,"
                                "source inet,"
                                "source_elapsed int,"
                                "thread text,"
                                "scylla_parent_id bigint,"
                                "scylla_span_id bigint,"
                                "PRIMARY KEY ((session_id), event_id)) "
                                "WITH default_time_to_live = 86400", KEYSPACE_NAME, EVENTS))

            , _slow_query_log_cql(fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
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
                                        "WITH default_time_to_live = 86400", KEYSPACE_NAME, NODE_SLOW_QUERY_LOG))

            , _slow_query_log_time_idx_cql(fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                                                 "minute timestamp,"
                                                 "started_at timestamp,"
                                                 "session_id uuid,"
                                                 "start_time timeuuid,"
                                                 "node_ip inet,"
                                                 "shard int,"
                                                 "PRIMARY KEY (minute, started_at, session_id)) "
                                                 "WITH default_time_to_live = 86400", KEYSPACE_NAME, NODE_SLOW_QUERY_LOG_TIME_IDX))
{
    namespace sm = seastar::metrics;

    _metrics.add_group("tracing_keyspace_helper", {
        sm::make_counter("tracing_errors", [this] { return _stats.tracing_errors; },
                        sm::description("Counts a number of errors during writing to a system_traces keyspace. "
                                        "One error may cause one or more tracing records to be lost.")).set_skip_when_empty(),

        sm::make_counter("bad_column_family_errors", [this] { return _stats.bad_column_family_errors; },
                        sm::description("Counts a number of times write failed due to one of the tables in the system_traces keyspace has an incompatible schema. "
                                        "One error may result one or more tracing records to be lost. "
                                        "Non-zero value indicates that the administrator has to take immediate steps to fix the corresponding schema. "
                                        "The appropriate error message will be printed in the syslog.")).set_skip_when_empty(),
    });
}

future<> trace_keyspace_helper::start(cql3::query_processor& qp, service::migration_manager& mm) {
    _qp_anchor = &qp;
    _mm_anchor = &mm;
    return table_helper::setup_keyspace(qp, mm, KEYSPACE_NAME, "org.apache.cassandra.locator.SimpleStrategy", "2", _dummy_query_state,
            std::vector<sstring>{ _sessions_cql, _sessions_time_idx_cql, _events_cql, _slow_query_log_cql, _slow_query_log_time_idx_cql });
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

// The serialized form of a text value is its UTF-8 bytes.
static bytes_view text_value(std::string_view text) {
    return to_bytes_view(text);
}

// The schema of a table of the trace keyspace, which setup_keyspace() created at start. A table
// that is gone is reported as one that doesn't meet the expected schema, like any other schema
// problem of the trace tables.
static schema_ptr table_schema(cql3::query_processor& qp, std::string_view table) {
    try {
        return qp.db().find_schema(trace_keyspace_helper::KEYSPACE_NAME, table);
    } catch (const data_dictionary::no_such_column_family& e) {
        throw bad_column_family(sstring(trace_keyspace_helper::KEYSPACE_NAME), sstring(table), e);
    }
}

// The definition of a column of a trace table, without which the table doesn't meet the expected
// schema.
static const column_definition& column(const schema& s, std::string_view name) {
    const auto* def = s.get_column_definition(to_bytes(name));
    if (!def) {
        throw bad_column_family(s.ks_name(), s.cf_name(), std::runtime_error(format("no {} column", name)));
    }
    return *def;
}

static db_clock::time_point to_db_clock(std::chrono::system_clock::time_point tp) {
    return db_clock::time_point(std::chrono::duration_cast<db_clock::duration>(tp.time_since_epoch()));
}

mutation trace_keyspace_helper::make_session_mutation(cql3::query_processor& qp, const one_session_records& session_records) {
    auto schema = table_schema(qp, SESSIONS);
    const session_record& record = session_records.session_rec;

    const auto timestamp = _dummy_query_state.get_timestamp();
    const gc_clock::duration ttl = session_records.ttl;
    const auto now = gc_clock::now();
    const auto expiry = now + ttl;
    // A TTL of 0 means no TTL, as with an INSERT ... USING TTL 0
    auto make_cell = [&] (const abstract_type& type, bytes_view value, atomic_cell::collection_member cm = atomic_cell::collection_member::no) {
        return ttl.count() ? atomic_cell::make_live(type, timestamp, value, expiry, ttl, cm) : atomic_cell::make_live(type, timestamp, value, cm);
    };

    mutation m(schema, partition_key::from_single_value(*schema, uuid_type->decompose(session_records.session_id)));
    auto& row = m.partition().clustered_row(*schema, clustering_key::make_empty());
    row.apply(ttl.count() ? row_marker(timestamp, ttl, expiry) : row_marker(timestamp));
    auto set_cell = [&] (std::string_view name, bytes_view value) {
        const auto& def = column(*schema, name);
        row.cells().apply(def, make_cell(*def.type, value));
    };
    set_cell("command", text_value(type_to_string(record.command)));
    set_cell("client", inet_addr_type->decompose(record.client.addr()));
    set_cell("coordinator", inet_addr_type->decompose(my_address().addr()));
    set_cell("duration", int32_type->decompose(elapsed_to_micros(record.elapsed)));
    set_cell("request", text_value(record.request));
    set_cell("started_at", timestamp_type->decompose(to_db_clock(record.started_at)));
    set_cell("request_size", int32_type->decompose(int32_t(record.request_size)));
    set_cell("response_size", int32_type->decompose(int32_t(record.response_size)));
    // The sessions table of a cluster created before the username column was added to it has no
    // such column; the record is stored without it then, as it always was.
    if (const auto* username_def = schema->get_column_definition("username")) {
        row.cells().apply(*username_def, make_cell(*username_def->type, text_value(record.username)));
    }
    // An INSERT of a map overwrites the whole value: a tombstone just before the new cells. The
    // parameters are kept sorted, the order the cells of a map must be in.
    const auto& parameters_def = column(*schema, "parameters");
    collection_mutation_writer parameters(tombstone(timestamp - 1, now));
    for (const auto& [key, value] : record.parameters) {
        parameters.push_back(managed_bytes_view(text_value(key)),
                make_cell(*utf8_type, text_value(value), atomic_cell::collection_member::yes));
    }
    row.cells().apply(parameters_def, std::move(parameters).finish());
    return m;
}

mutation trace_keyspace_helper::make_session_time_idx_mutation(cql3::query_processor& qp, const one_session_records& session_records) {
    auto schema = table_schema(qp, SESSIONS_TIME_IDX);
    if (schema->partition_key_size() != 1 || schema->clustering_key_size() != 2) {
        throw bad_column_family(sstring(KEYSPACE_NAME), sstring(SESSIONS_TIME_IDX), std::runtime_error("unexpected primary key"));
    }
    const auto timestamp = _dummy_query_state.get_timestamp();
    const gc_clock::duration ttl = session_records.ttl;
    const auto expiry = gc_clock::now() + ttl;

    const auto started_at = to_db_clock(session_records.session_rec.started_at);
    // The minute the query began in is the partition key
    const auto minute = db_clock::time_point(std::chrono::duration_cast<std::chrono::minutes>(started_at.time_since_epoch()));

    mutation m(schema, partition_key::from_single_value(*schema, timestamp_type->decompose(minute)));
    auto& row = m.partition().clustered_row(*schema, clustering_key::from_exploded(*schema,
            {timestamp_type->decompose(started_at), uuid_type->decompose(session_records.session_id)}));
    row.apply(ttl.count() ? row_marker(timestamp, ttl, expiry) : row_marker(timestamp));
    return m;
}

mutation trace_keyspace_helper::make_slow_query_mutation(cql3::query_processor& qp, const one_session_records& session_records, const utils::UUID& start_time_id) {
    auto schema = table_schema(qp, NODE_SLOW_QUERY_LOG);
    if (schema->partition_key_size() != 1 || schema->clustering_key_size() != 2) {
        throw bad_column_family(sstring(KEYSPACE_NAME), sstring(NODE_SLOW_QUERY_LOG), std::runtime_error("unexpected primary key"));
    }
    const session_record& record = session_records.session_rec;

    const auto timestamp = _dummy_query_state.get_timestamp();
    const gc_clock::duration ttl = record.slow_query_record_ttl;
    const auto now = gc_clock::now();
    const auto expiry = now + ttl;
    // A TTL of 0 means no TTL, as with an INSERT ... USING TTL 0
    auto make_cell = [&] (const abstract_type& type, bytes_view value, atomic_cell::collection_member cm = atomic_cell::collection_member::no) {
        return ttl.count() ? atomic_cell::make_live(type, timestamp, value, expiry, ttl, cm) : atomic_cell::make_live(type, timestamp, value, cm);
    };

    // The log is keyed by the start time of the query, then by the node and the shard that ran it
    mutation m(schema, partition_key::from_single_value(*schema, timeuuid_type->decompose(start_time_id)));
    auto& row = m.partition().clustered_row(*schema, clustering_key::from_exploded(*schema,
            {inet_addr_type->decompose(my_address().addr()), int32_type->decompose(int32_t(this_shard_id()))}));
    row.apply(ttl.count() ? row_marker(timestamp, ttl, expiry) : row_marker(timestamp));
    auto set_cell = [&] (std::string_view name, bytes_view value) {
        const auto& def = column(*schema, name);
        row.cells().apply(def, make_cell(*def.type, value));
    };
    set_cell("session_id", uuid_type->decompose(session_records.session_id));
    set_cell("date", timestamp_type->decompose(to_db_clock(record.started_at)));
    // The query is stored in the parameters map under the "query" key; a session without one has
    // no command, as the INSERT bound a null for it.
    if (const auto query = record.parameters.find("query"); query != record.parameters.end()) {
        set_cell("command", text_value(query->second));
    } else {
        tlogger.trace("No \"query\" parameter set for a session requesting a slow_query_log record");
    }
    set_cell("duration", int32_type->decompose(elapsed_to_micros(record.elapsed)));
    set_cell("source_ip", inet_addr_type->decompose(record.client.addr()));
    set_cell("username", text_value(record.username));
    // An INSERT of a collection overwrites the whole value: a tombstone just before the new cells.
    // The parameters and the table names are kept sorted, the order the cells of a collection must
    // be in.
    collection_mutation_writer parameters(tombstone(timestamp - 1, now));
    for (const auto& [key, value] : record.parameters) {
        parameters.push_back(managed_bytes_view(text_value(key)),
                make_cell(*utf8_type, text_value(value), atomic_cell::collection_member::yes));
    }
    row.cells().apply(column(*schema, "parameters"), std::move(parameters).finish());
    // The cell of a set element has an empty value
    const auto& table_names_def = column(*schema, "table_names");
    const auto& table_names_type = static_cast<const set_type_impl&>(*table_names_def.type);
    collection_mutation_writer table_names(tombstone(timestamp - 1, now));
    for (const auto& table : record.tables) {
        table_names.push_back(managed_bytes_view(text_value(table)),
                make_cell(*table_names_type.value_comparator(), bytes_view(), atomic_cell::collection_member::yes));
    }
    row.cells().apply(table_names_def, std::move(table_names).finish());
    return m;
}

mutation trace_keyspace_helper::make_slow_query_time_idx_mutation(cql3::query_processor& qp, const one_session_records& session_records, const utils::UUID& start_time_id) {
    auto schema = table_schema(qp, NODE_SLOW_QUERY_LOG_TIME_IDX);
    if (schema->partition_key_size() != 1 || schema->clustering_key_size() != 2) {
        throw bad_column_family(sstring(KEYSPACE_NAME), sstring(NODE_SLOW_QUERY_LOG_TIME_IDX), std::runtime_error("unexpected primary key"));
    }
    const session_record& record = session_records.session_rec;

    const auto timestamp = _dummy_query_state.get_timestamp();
    const gc_clock::duration ttl = record.slow_query_record_ttl;
    const auto expiry = gc_clock::now() + ttl;

    const auto started_at = to_db_clock(record.started_at);
    // The minute the query began in is the partition key
    const auto minute = db_clock::time_point(std::chrono::duration_cast<std::chrono::minutes>(started_at.time_since_epoch()));

    mutation m(schema, partition_key::from_single_value(*schema, timestamp_type->decompose(minute)));
    auto& row = m.partition().clustered_row(*schema, clustering_key::from_exploded(*schema,
            {timestamp_type->decompose(started_at), uuid_type->decompose(session_records.session_id)}));
    row.apply(ttl.count() ? row_marker(timestamp, ttl, expiry) : row_marker(timestamp));
    auto set_cell = [&] (std::string_view name, bytes_view value) {
        const auto& def = column(*schema, name);
        // A TTL of 0 means no TTL, as with an INSERT ... USING TTL 0
        row.cells().apply(def, ttl.count() ? atomic_cell::make_live(*def.type, timestamp, value, expiry, ttl) : atomic_cell::make_live(*def.type, timestamp, value));
    };
    set_cell("start_time", timeuuid_type->decompose(start_time_id));
    set_cell("node_ip", inet_addr_type->decompose(my_address().addr()));
    set_cell("shard", int32_type->decompose(int32_t(this_shard_id())));
    return m;
}

mutation trace_keyspace_helper::make_events_mutation(cql3::query_processor& qp, one_session_records& session_records, const std::deque<event_record>& events_records) {
    auto schema = table_schema(qp, EVENTS);
    auto backend_state_ptr = static_cast<trace_keyspace_backend_sesssion_state*>(session_records.backend_state_ptr.get());

    // What an INSERT ... USING TTL of the record would write: the timestamp of the query state the
    // statements ran with, and a row marker and cells that all expire together.
    const auto timestamp = _dummy_query_state.get_timestamp();
    const gc_clock::duration ttl = session_records.ttl;
    const auto expiry = gc_clock::now() + ttl;

    const auto& activity_def = column(*schema, "activity");
    const auto& source_def = column(*schema, "source");
    const auto& source_elapsed_def = column(*schema, "source_elapsed");
    const auto& thread_def = column(*schema, "thread");
    const auto& parent_id_def = column(*schema, "scylla_parent_id");
    const auto& span_id_def = column(*schema, "scylla_span_id");

    // The values every row of the session shares.
    const auto source = inet_addr_type->decompose(my_address().addr());
    const auto parent_id = long_type->decompose(int64_t(session_records.parent_id.get_id()));
    const auto span_id = long_type->decompose(int64_t(session_records.my_span_id.get_id()));
    const auto& thread_name = _local_tracing.get_thread_name();

    mutation m(schema, partition_key::from_single_value(*schema, uuid_type->decompose(session_records.session_id)));
    for (const event_record& record : events_records) {
        auto event_id = utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(backend_state_ptr->last_nanos, record.event_time_point));
        auto& row = m.partition().clustered_row(*schema, clustering_key::from_single_value(*schema, timeuuid_type->decompose(event_id)));
        const auto thread = fmt::format("{}/{}", thread_name, record.scheduling_group_name);
        row.apply(ttl.count() ? row_marker(timestamp, ttl, expiry) : row_marker(timestamp));
        auto set_cell = [&] (const column_definition& def, bytes_view value) {
            // A TTL of 0 means no TTL, as with an INSERT ... USING TTL 0
            row.cells().apply(def, ttl.count() ? atomic_cell::make_live(*def.type, timestamp, value, expiry, ttl) : atomic_cell::make_live(*def.type, timestamp, value));
        };
        set_cell(activity_def, text_value(record.message));
        set_cell(source_def, source);
        set_cell(source_elapsed_def, int32_type->decompose(elapsed_to_micros(record.elapsed)));
        set_cell(thread_def, text_value(thread));
        set_cell(parent_id_def, parent_id);
        set_cell(span_id_def, span_id);
    }
    return m;
}

future<> trace_keyspace_helper::apply_events_mutation(cql3::query_processor& qp, lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records) {
    if (events_records.empty()) {
        return now();
    }

    tlogger.trace("{}: storing {} events records: parent_id {} span_id {}", records->session_id, events_records.size(), records->parent_id, records->my_span_id);

    // The records of a session are stored with one mutation, built here directly rather than
    // by a batch of INSERT statements, one per record, which costs the statement execution on
    // top of the write and ends up merged into the same one mutation anyway.
    utils::chunked_vector<mutation> mutations;
    mutations.emplace_back(make_events_mutation(qp, *records, events_records));
    return write_mutations(qp, std::move(mutations));
}

future<> trace_keyspace_helper::write_mutations(cql3::query_processor& qp, utils::chunked_vector<mutation> mutations) {
    auto timeout = db::timeout_clock::now() + _dummy_query_state.get_client_state().get_timeout_config().write_timeout;
    return qp.proxy().mutate(std::move(mutations), db::consistency_level::ANY, timeout, trace_state_ptr(), _dummy_query_state.get_permit(), db::allow_per_partition_rate_limit::yes);
}

future<> trace_keyspace_helper::store_session_record(cql3::query_processor& qp, lw_shared_ptr<one_session_records> records) {
    tlogger.trace("{}: storing the session record", records->session_id);
    utils::chunked_vector<mutation> mutations;
    mutations.emplace_back(make_session_mutation(qp, *records));
    mutations.emplace_back(make_session_time_idx_mutation(qp, *records));
    return write_mutations(qp, std::move(mutations));
}

future<> trace_keyspace_helper::store_slow_query_record(cql3::query_processor& qp, lw_shared_ptr<one_session_records> records) {
    tlogger.trace("{}: storing the slow query log record", records->session_id);
    auto start_time_id = utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(_slow_query_last_nanos, records->session_rec.started_at));
    utils::chunked_vector<mutation> mutations;
    mutations.emplace_back(make_slow_query_mutation(qp, *records, start_time_id));
    mutations.emplace_back(make_slow_query_time_idx_mutation(qp, *records, start_time_id));
    return write_mutations(qp, std::move(mutations));
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
            return apply_events_mutation(qp, records, events_records).then([this, &qp, session_record_is_ready, records] {
                if (session_record_is_ready) {

                    // if session is finished - store a session and a session time index entries
                    return store_session_record(qp, records).then([this, &qp, records] {
                        if (!records->do_log_slow_query) {
                            return now();
                        }

                        // if slow query log is requested - store a slow query log and a slow query log time index entries
                        return store_slow_query_record(qp, records);
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

}
