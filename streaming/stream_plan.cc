/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_plan.hh"
#include "streaming/stream_result_future.hh"
#include "streaming/stream_state.hh"

namespace streaming {

extern logging::logger sslog;

stream_plan& stream_plan::request_ranges(inet_address from, sstring keyspace, dht::token_range_vector ranges) {
    return request_ranges(from, keyspace, std::move(ranges), {});
}

stream_plan& stream_plan::request_ranges(inet_address from, sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families) {
    _range_added = true;
    auto session = _coordinator->get_or_create_session(_mgr, from);
    session->add_stream_request(keyspace, std::move(ranges), std::move(column_families));
    session->set_reason(_reason);
    session->set_topo_guard(_topo_guard);
    return *this;
}

stream_plan& stream_plan::transfer_ranges(inet_address to, sstring keyspace, dht::token_range_vector ranges) {
    return transfer_ranges(to, std::move(keyspace), std::move(ranges), {});
}

stream_plan& stream_plan::transfer_ranges(inet_address to, sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families) {
    _range_added = true;
    auto session = _coordinator->get_or_create_session(_mgr, to);
    session->add_transfer_ranges(std::move(keyspace), std::move(ranges), std::move(column_families));
    session->set_reason(_reason);
    session->set_topo_guard(_topo_guard);
    return *this;
}

future<stream_state> stream_plan::execute() {
    sslog.debug("[Stream #{}] Executing stream_plan description={} range_added={}", _plan_id, _description, _range_added);
    if (!_range_added) {
        stream_state state(_plan_id, _description, std::vector<session_info>());
        return make_ready_future<stream_state>(std::move(state));
    }
    if (_aborted) {
        throw std::runtime_error(format("steam_plan {} is aborted", _plan_id));
    }
    return stream_result_future::init_sending_side(_mgr, _plan_id, _description, _handlers, _coordinator);
}

stream_plan& stream_plan::listeners(std::vector<stream_event_handler*> handlers) {
    std::copy(handlers.begin(), handlers.end(), std::back_inserter(_handlers));
    return *this;
}

void stream_plan::do_abort() {
    _aborted = true;
    _coordinator->abort_all_stream_sessions();
}

void stream_plan::abort() noexcept {
    try {
        // FIXME: do_abort() can throw, because its underlying implementation
        // allocates a vector and calls vector::push_back(). Let's make it noexcept too
        do_abort();
    } catch (...) {
        try {
            sslog.error("Failed to abort stream plan: {}", std::current_exception());
        } catch (...) {
            // Nothing else we can do.
        }
    }
}

}
