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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
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
        if (x.second.is_active_session()) {
            return true;
        }
    }
    return false;
}

std::vector<shared_ptr<stream_session>> stream_coordinator::get_all_stream_sessions() {
    std::vector<shared_ptr<stream_session>> results;
    for (auto& x : _peer_sessions) {
        results.push_back(x.second._stream_session);
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_all_session_info() {
    std::vector<session_info> results;
    for (auto& x : _peer_sessions) {
        results.push_back(x.second._stream_session->get_session_info());
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_peer_session_info(inet_address peer) {
    std::vector<session_info> results;
    auto it = _peer_sessions.find(peer);
    if (it != _peer_sessions.end()) {
        results.push_back(it->second._stream_session->get_session_info());
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

stream_coordinator::host_streaming_data& stream_coordinator::get_host_data(inet_address peer) {
    auto it = _peer_sessions.find(peer);
    if (it == _peer_sessions.end()) {
        throw std::runtime_error(sprint("Unknown peer requested: %s", peer));
    }
    return it->second;
}

stream_coordinator::host_streaming_data& stream_coordinator::get_or_create_host_data(inet_address peer) {
    auto it = _peer_sessions.find(peer);
    if (it == _peer_sessions.end()) {
        it = _peer_sessions.emplace(peer, host_streaming_data()).first;
    }
    return it->second;
}

bool stream_coordinator::host_streaming_data::is_active_session() {
    auto state = _stream_session->get_state();
    if (state != stream_session_state::COMPLETE && state != stream_session_state::FAILED) {
        return true;
    }
    return false;
}

shared_ptr<stream_session> stream_coordinator::host_streaming_data::get_or_create_session(inet_address peer) {
    if (!_stream_session) {
        _stream_session = make_shared<stream_session>(peer);
    }
    return _stream_session;
}

void stream_coordinator::host_streaming_data::update_progress(progress_info info) {
    _stream_session->update_progress(std::move(info));
}

void stream_coordinator::connect_all_stream_sessions() {
    for (auto& data : _peer_sessions) {
        data.second.connect();
    }
}

void stream_coordinator::host_streaming_data::connect() {
    _stream_session->start();
    sslog.info("[Stream #{}] Beginning stream session with {}", _stream_session->plan_id(), _stream_session->peer);
}

} // namespace streaming
