
/*
 * Copyright (C) 2015 ScyllaDB
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

#ifndef SERVICE_QUERY_STATE_HH
#define SERVICE_QUERY_STATE_HH

#include "service/client_state.hh"
#include "tracing/tracing.hh"

namespace service {

class query_state final {
private:
    client_state _client_state;
    tracing::trace_state_ptr _trace_state_ptr;

public:
    query_state(client_state client_state)
        : _client_state(client_state)
        , _trace_state_ptr(_client_state.get_trace_state())
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
};

}

#endif
