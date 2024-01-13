/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include "tracing/tracing.hh"
#include "table_helper.hh"
#include "cql3/values.hh"

namespace tracing {

class trace_keyspace_helper final : public i_tracing_backend_helper {
public:
    static constexpr std::string_view KEYSPACE_NAME = "system_traces";
    static constexpr std::string_view SESSIONS = "sessions";
    static constexpr std::string_view SESSIONS_TIME_IDX = "sessions_time_idx";
    static constexpr std::string_view EVENTS = "events";

    // Performance related tables
    static constexpr std::string_view NODE_SLOW_QUERY_LOG = "node_slow_log";
    static constexpr std::string_view NODE_SLOW_QUERY_LOG_TIME_IDX = "node_slow_log_time_idx";
private:
    static constexpr int bad_column_family_message_period = 10000;

    seastar::gate _pending_writes;
    int64_t _slow_query_last_nanos = 0;
    service::query_state _dummy_query_state;

    cql3::query_processor* _qp_anchor;
    service::migration_manager* _mm_anchor;

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
    virtual future<> start(cql3::query_processor& qp, service::migration_manager& mm) override;

    virtual future<> shutdown() override {
        return _pending_writes.close().then([this] {
            _qp_anchor = nullptr;
            _mm_anchor = nullptr;
        });
    };

    virtual void write_records_bulk(records_bulk& bulk) override;
    virtual std::unique_ptr<backend_session_state_base> allocate_session_state() const override;

private:
    // Valid only after start() sets _qp_anchor
    gms::inet_address my_address() const noexcept;

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
    future<> apply_events_mutation(cql3::query_processor& qp, service::migration_manager& mm, lw_shared_ptr<one_session_records> records, std::deque<event_record>& events_records);

    /**
     * Create a mutation data for a new session record
     *
     * @param all_records_handle handle to access an object with all records of this session
     *
     * @return the relevant cql3::query_options object with the mutation data
     */
    static cql3::query_options make_session_mutation_data(gms::inet_address my_address, const one_session_records& all_records_handle);

    /**
     * Create a mutation data for a new session_idx record
     *
     * @param all_records_handle handle to access an object with all records of this session
     *
     * @return the relevant cql3::query_options object with the mutation data
     */
    static cql3::query_options make_session_time_idx_mutation_data(gms::inet_address my_address, const one_session_records& all_records_handle);

    /**
     * Create mutation for a new slow_query_log record
     *
     * @param all_records_handle handle to access an object with all records of this session
     * @param start_time_id time UUID generated from the query start time
     *
     * @return the relevant mutation
     */
    static cql3::query_options make_slow_query_mutation_data(gms::inet_address my_address, const one_session_records& all_records_handle, const utils::UUID& start_time_id);

    /**
     * Create mutation for a new slow_query_log_time_idx record
     *
     * @param all_records_handle handle to access an object with all records of this session
     * @param start_time_id time UUID generated from the query start time
     *
     * @return the relevant mutation
     */
    static cql3::query_options make_slow_query_time_idx_mutation_data(gms::inet_address my_address, const one_session_records& all_records_handle, const utils::UUID& start_time_id);

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
    std::vector<cql3::raw_value> make_event_mutation_data(gms::inet_address my_address, one_session_records& session_records, const event_record& record);

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
