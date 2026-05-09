
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#ifndef SERVICE_QUERY_STATE_HH
#define SERVICE_QUERY_STATE_HH

#include "service/client_state.hh"
#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"
#include "service_permit.hh"
#include "utils/latency.hh"

namespace utils {
class timed_rate_moving_average_summary_and_histogram;
}

namespace qos {
class service_level_controller;
}
namespace service {

// Carries a started latency counter and a pointer to the histogram
// that should be marked when the response is flushed to the client.
// The counter is started at the transport layer when the request
// arrives and the histogram pointer is set by the statement layer
// once the operation type (read/write/range/cas) is known.
// The transport layer stops the counter and marks the histogram
// after the response has been flushed to the OS socket.
struct deferred_latency_mark {
    utils::latency_counter lc;
    utils::timed_rate_moving_average_summary_and_histogram* histogram = nullptr;
};

class query_state final {
private:
    client_state& _client_state;
    tracing::trace_state_ptr _trace_state_ptr;
    service_permit _permit;
    std::optional<deferred_latency_mark> _deferred_latency;

public:
    query_state(client_state& client_state, service_permit permit)
            : _client_state(client_state)
            , _trace_state_ptr(tracing::trace_state_ptr())
            , _permit(std::move(permit))
    {}

    query_state(client_state& client_state, tracing::trace_state_ptr trace_state_ptr, service_permit permit)
        : _client_state(client_state)
        , _trace_state_ptr(std::move(trace_state_ptr))
        , _permit(std::move(permit))
    { }

    const tracing::trace_state_ptr& get_trace_state() const {
        return _trace_state_ptr;
    }

    tracing::trace_state_ptr& get_trace_state() {
        return _trace_state_ptr;
    }

    client_state& get_client_state() {
        return _client_state;
    }

    const client_state& get_client_state() const {
        return _client_state;
    }
    api::timestamp_type get_timestamp() {
        return _client_state.get_timestamp();
    }

    service_permit get_permit() const& {
        return _permit;
    }

    service_permit&& get_permit() && {
        return std::move(_permit);
    }

    qos::service_level_controller& get_service_level_controller() const {
        return _client_state.get_service_level_controller();
    }

    // Start the latency counter. Called from the transport layer
    // when the request first arrives.
    void start_latency() {
        _deferred_latency.emplace();
        _deferred_latency->lc.start();
    }

    // Set the histogram that should be marked when the response
    // is flushed. Called from the statement layer once the
    // operation type is known.
    void set_latency_histogram(utils::timed_rate_moving_average_summary_and_histogram& hist) {
        if (_deferred_latency) {
            _deferred_latency->histogram = &hist;
        }
    }

    bool has_deferred_latency() const {
        return _deferred_latency.has_value();
    }

    // Extract the deferred latency mark. The transport layer
    // calls this to take ownership and mark the histogram after
    // the response is flushed.
    std::optional<deferred_latency_mark> take_deferred_latency() {
        return std::exchange(_deferred_latency, std::nullopt);
    }

};

}

#endif
