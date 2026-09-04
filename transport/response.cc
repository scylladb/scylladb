/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "transport/response.hh"
#include "transport/event.hh"
#include "tracing/trace_state.hh"

namespace cql_transport {

seastar::shared_ptr<response> make_schema_change_event_response(const event::schema_change& event, uint8_t version) {
    auto r = seastar::make_shared<response>(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    r->write_string("SCHEMA_CHANGE");
    r->serialize(event, version);
    return r;
}

}
