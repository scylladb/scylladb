/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
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

#include "streaming/stream_detail.hh"
#include "streaming/stream_session_state.hh"
#include "streaming/stream_coordinator.hh"
#include "log.hh"

namespace streaming {

extern logging::logger sslog;

using gms::inet_address;

bool stream_coordinator::has_active_sessions() {
    for (auto& x : _peer_sessions) {
        auto state = x.second->get_state();
        if (state != stream_session_state::COMPLETE && state != stream_session_state::FAILED) {
            return true;
        }
    }
    return false;
}

std::vector<shared_ptr<stream_session>> stream_coordinator::get_all_stream_sessions() {
    std::vector<shared_ptr<stream_session>> results;
    for (auto& x : _peer_sessions) {
        results.push_back(x.second);
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_all_session_info() {
    std::vector<session_info> results;
    for (auto& x : _peer_sessions) {
        auto& session = x.second;
        results.push_back(session->get_session_info());
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_peer_session_info(inet_address peer) {
    std::vector<session_info> results;
    auto it = _peer_sessions.find(peer);
    if (it != _peer_sessions.end()) {
        auto& session = it->second;
        results.push_back(session->get_session_info());
    }
    return results;
}

bool stream_coordinator::is_receiving() {
    return _is_receiving;
}

std::set<inet_address> stream_coordinator::get_peers() {
    std::set<inet_address> results;
    for (auto& x : _peer_sessions) {
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
