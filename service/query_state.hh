
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef SERVICE_QUERY_STATE_HH
#define SERVICE_QUERY_STATE_HH

#include "service/client_state.hh"
#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"
#include "service_permit.hh"

namespace qos {
class service_level_controller;
}
namespace service {

class query_state final {
private:
    client_state& _client_state;
    tracing::trace_state_ptr _trace_state_ptr;
    service_permit _permit;

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

};

}

#endif
