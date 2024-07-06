/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#include <seastar/core/metrics.hh>
#include <seastar/core/coroutine.hh>
#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"
#include "utils/class_registrator.hh"
#include "utils/UUID_gen.hh"

namespace tracing {

logging::logger tracing_logger("tracing");
const gc_clock::duration tracing::tracing::write_period = std::chrono::seconds(2);
const std::chrono::seconds tracing::tracing::default_slow_query_record_ttl = std::chrono::seconds(86400);
const std::chrono::microseconds tracing::tracing::default_slow_query_duraion_threshold = std::chrono::milliseconds(500);

std::vector<sstring> trace_type_names = {
    "NONE",
    "QUERY",
    "REPAIR"
};

tracing::tracing(sstring tracing_backend_helper_class_name)
        : _write_timer([this] { write_timer_callback(); })
        , _thread_name(seastar::format("shard {:d}", this_shard_id()))
        , _tracing_backend_helper_class_name(std::move(tracing_backend_helper_class_name))
        , _gen(std::random_device()())
        , _slow_query_duration_threshold(default_slow_query_duraion_threshold)
        , _slow_query_record_ttl(default_slow_query_record_ttl) {
    namespace sm = seastar::metrics;

    _metrics.add_group("tracing", {
        sm::make_counter("dropped_sessions", stats.dropped_sessions,
                        sm::description("Counts a number of dropped sessions due to too many pending sessions/records. "
                                        "High value indicates that backend is saturated with the rate with which new tracing records are created.")),

        sm::make_counter("dropped_records", stats.dropped_records,
                        sm::description("Counts a number of dropped records due to too many pending records. "
                                        "High value indicates that backend is saturated with the rate with which new tracing records are created.")),

        sm::make_counter("trace_records_count", stats.trace_records_count,
                        sm::description("This metric is a rate of tracing records generation.")),

        sm::make_counter("trace_errors", stats.trace_errors,
                        sm::description("Counts a number of trace records dropped due to an error (e.g. OOM).")),

        sm::make_gauge("active_sessions", _active_sessions,
                        sm::description("Holds a number of a currently active tracing sessions.")),

        sm::make_gauge("cached_records", _cached_records,
                        sm::description(seastar::format("Holds a number of tracing records cached in the tracing sessions that are not going to be written in the next write event. "
                                                        "If sum of this metric, pending_for_write_records and flushing_records is close to {} we are likely to start dropping tracing records.", max_pending_trace_records + write_event_records_threshold))),

        sm::make_gauge("pending_for_write_records", _pending_for_write_records_count,
                        sm::description(seastar::format("Holds a number of tracing records that are going to be written in the next write event. "
                                                        "If sum of this metric, cached_records and flushing_records is close to {} we are likely to start dropping tracing records.", max_pending_trace_records + write_event_records_threshold))),

        sm::make_gauge("flushing_records", _flushing_records,
                        sm::description(seastar::format("Holds a number of tracing records that currently being written to the I/O backend. "
                                                        "If sum of this metric, cached_records and pending_for_write_records is close to {} we are likely to start dropping tracing records.", max_pending_trace_records + write_event_records_threshold))),
    });
}

bool tracing::may_create_new_session(const std::optional<utils::UUID>& session_id) {
    // Don't create a session if its records are likely to be dropped
    if (!have_records_budget(exp_trace_events_per_session) || _active_sessions >= max_pending_sessions + write_event_sessions_threshold) {
        if (session_id) {
            tracing_logger.trace("{}: Too many outstanding tracing records or sessions. Dropping a secondary session", *session_id);
        } else {
            tracing_logger.trace("Too many outstanding tracing records or sessions. Dropping a primary session");
        }

        if (++stats.dropped_sessions % tracing::log_warning_period == 1) {
            tracing_logger.warn("Dropped {} sessions: open_sessions {}, cached_records {} pending_for_write_records {}, flushing_records {}",
                        stats.dropped_sessions, _active_sessions, _cached_records, _pending_for_write_records_count, _flushing_records);
        }

        return false;
    }

    return true;
}

trace_state_ptr tracing::create_session(trace_type type, trace_state_props_set props) noexcept {
    if (!started()) {
        return nullptr;
    }

    try {
        // Don't create a session if its records are likely to be dropped
        if (!may_create_new_session()) {
            return nullptr;
        }

        // Ignore events if they are disabled and this is not a full tracing request (probability tracing or user request)
        props.set_if<trace_state_props::ignore_events>(!props.contains<trace_state_props::full_tracing>() && ignore_trace_events_enabled());

        ++_active_sessions;
        return make_lw_shared<trace_state>(type, props);
    } catch (...) {
        // return an uninitialized state in case of any error (OOM?)
        return nullptr;
    }
}

trace_state_ptr tracing::create_session(const trace_info& secondary_session_info) noexcept {
    if (!started()) {
        return nullptr;
    }

    try {
        // Don't create a session if its records are likely to be dropped
        if (!may_create_new_session(secondary_session_info.session_id)) {
            return nullptr;
        }

        ++_active_sessions;
        return make_lw_shared<trace_state>(secondary_session_info);
    } catch (...) {
        // return an uninitialized state in case of any error (OOM?)
        return nullptr;
    }
}

future<> tracing::start(cql3::query_processor& qp, service::migration_manager& mm) {
    try {
        _tracing_backend_helper_ptr = create_object<i_tracing_backend_helper>(_tracing_backend_helper_class_name, *this);
    } catch (no_such_class& e) {
        tracing_logger.error("Can't create tracing backend helper {}: not supported", _tracing_backend_helper_class_name);
        throw;
    } catch (...) {
        throw;
    }

    co_await _tracing_backend_helper_ptr->start(qp, mm);
    _down = false;
    _write_timer.arm(write_period);
}

void tracing::write_timer_callback() {
    if (_down) {
        return;
    }

    tracing_logger.trace("Timer kicks in: {}", _pending_for_write_records_bulk.size() ? "writing" : "not writing");
    write_pending_records();
    _write_timer.arm(write_period);
}

future<> tracing::shutdown() {
    if (_down) {
        co_return;
    }

    tracing_logger.info("Asked to shut down");
    write_pending_records();
    _down = true;
    _write_timer.cancel();
    co_await _tracing_backend_helper_ptr->shutdown();
    tracing_logger.info("Tracing is down");
}

future<> tracing::stop() {
    co_await shutdown();
}

void tracing::set_trace_probability(double p) {
    if (p < 0 || p > 1) {
        throw std::out_of_range("trace probability must be in a [0,1] range");
    }

    _trace_probability = p;
    _normalized_trace_probability = std::llround(_trace_probability * (_gen.max() + 1));

    tracing_logger.info("Setting tracing probability to {} (normalized {})", _trace_probability, _normalized_trace_probability);
}

one_session_records::one_session_records(trace_type type, std::chrono::seconds slow_query_ttl, std::chrono::seconds slow_query_rec_ttl,
            std::optional<utils::UUID> session_id_, span_id parent_id_)
    : _local_tracing_ptr(tracing::get_local_tracing_instance().shared_from_this())
    , session_id(session_id_ ? *session_id_ : utils::UUID_gen::get_time_UUID())
    , session_rec(type, slow_query_rec_ttl)
    , ttl(slow_query_ttl)
    , backend_state_ptr(_local_tracing_ptr->allocate_backend_session_state())
    , budget_ptr(_local_tracing_ptr->get_cached_records_ptr())
    , parent_id(parent_id_)
    , my_span_id(span_id::make_span_id())
{
}

}
