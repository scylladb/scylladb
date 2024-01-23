/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_session_state.hh"
#include "streaming/stream_coordinator.hh"
#include "log.hh"

namespace streaming {

extern logging::logger sslog;

using gms::inet_address;

bool stream_coordinator::has_active_sessions() const {
    for (auto const& x : _peer_sessions) {
        auto state = x.second->get_state();
        if (state != stream_session_state::COMPLETE && state != stream_session_state::FAILED) {
            return true;
        }
    }
    return false;
}

std::vector<shared_ptr<stream_session>> stream_coordinator::get_all_stream_sessions() const {
    std::vector<shared_ptr<stream_session>> results;
    for (auto const& x : _peer_sessions) {
        results.push_back(x.second);
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_all_session_info() const {
    std::vector<session_info> results;
    for (auto const& x : _peer_sessions) {
        auto& session = x.second;
        results.push_back(session->get_session_info());
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_peer_session_info(inet_address peer) const {
    std::vector<session_info> results;
    auto it = _peer_sessions.find(peer);
    if (it != _peer_sessions.end()) {
        auto const& session = it->second;
        results.push_back(session->get_session_info());
    }
    return results;
}

bool stream_coordinator::is_receiving() const {
    return _is_receiving;
}

std::set<inet_address> stream_coordinator::get_peers() const {
    std::set<inet_address> results;
    for (auto const& x : _peer_sessions) {
        results.insert(x.first);
    }
    return results;
}

void stream_coordinator::abort_all_stream_sessions() {
    for (auto& session : get_all_stream_sessions()) {
        session->abort();
    }
}

void stream_coordinator::connect_all_stream_sessions() {
    for (auto& x : _peer_sessions) {
        auto& session = x.second;
        session->start();
        sslog.debug("[Stream #{}] Beginning stream session with {}", session->plan_id(), session->peer);
    }
}

} // namespace streaming
