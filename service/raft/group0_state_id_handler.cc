/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/group0_state_id_handler.hh"
#include "gms/gossiper.hh"
#include "log.hh"

namespace service {

static logging::logger slogger("group0_tombstone_gc_handler");

group0_state_id_handler::group0_state_id_handler(gms::gossiper& gossiper)
    : _gossiper(gossiper) {
}

future<> group0_state_id_handler::advertise_state_id(utils::UUID state_id) {
    if (_state_id_last_advertised && utils::timeuuid_tri_compare(_state_id_last_advertised, state_id) > 0) {
        slogger.debug("Skipping advertisement of stale state id {}", state_id);
        return make_ready_future();
    }

    _state_id_last_advertised = state_id;

    const auto gc_time = to_gc_clock(db_clock::time_point(utils::UUID_gen::unix_timestamp(state_id)));
    slogger.debug("Advertising state id: {} (gc_time: {})", state_id, gc_time);
    return _gossiper.add_local_application_state(gms::application_state::GROUP0_STATE_ID, gms::versioned_value::state_id(service::state_id(state_id)));
}

} // namespace service
