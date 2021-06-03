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
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/distributed.hh>
#include "streaming/stream_manager.hh"
#include "streaming/stream_result_future.hh"
#include "log.hh"
#include "streaming/stream_session_state.hh"
#include <seastar/core/metrics.hh>

namespace streaming {

extern logging::logger sslog;

distributed<stream_manager> _the_stream_manager;


stream_manager::stream_manager() {
    namespace sm = seastar::metrics;

    _metrics.add_group("streaming", {
        sm::make_derive("total_incoming_bytes", [this] { return _total_incoming_bytes; },
                        sm::description("Total number of bytes received on this shard.")),

        sm::make_derive("total_outgoing_bytes", [this] { return _total_outgoing_bytes; },
                        sm::description("Total number of bytes sent on this shard.")),
    });
}

void stream_manager::register_sending(shared_ptr<stream_result_future> result) {
#if 0
    result.addEventListener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            initiatedStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _initiated_streams[result->plan_id] = std::move(result);
}

void stream_manager::register_receiving(shared_ptr<stream_result_future> result) {
#if 0
    result->add_event_listener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            receivingStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _receiving_streams[result->plan_id] = std::move(result);
}

shared_ptr<stream_result_future> stream_manager::get_sending_stream(UUID plan_id) const {
    auto it = _initiated_streams.find(plan_id);
    if (it != _initiated_streams.end()) {
        return it->second;
    }
    return {};
}

shared_ptr<stream_result_future> stream_manager::get_receiving_stream(UUID plan_id) const {
    auto it = _receiving_streams.find(plan_id);
    if (it != _receiving_streams.end()) {
        return it->second;
    }
    return {};
}

void stream_manager::remove_stream(UUID plan_id) {
    sslog.debug("stream_manager: removing plan_id={}", plan_id);
    _initiated_streams.erase(plan_id);
    _receiving_streams.erase(plan_id);
    // FIXME: Do not ignore the future
    (void)remove_progress_on_all_shards(plan_id).handle_exception([plan_id] (auto ep) {
        sslog.info("stream_manager: Fail to remove progress for plan_id={}: {}", plan_id, ep);
    });
}

void stream_manager::show_streams() const {
    for (auto const& x : _initiated_streams) {
        sslog.debug("stream_manager:initiated_stream: plan_id={}", x.first);
    }
    for (auto const& x : _receiving_streams) {
        sslog.debug("stream_manager:receiving_stream: plan_id={}", x.first);
    }
}

std::vector<shared_ptr<stream_result_future>> stream_manager::get_all_streams() const {
    std::vector<shared_ptr<stream_result_future>> result;
    for (auto& x : _initiated_streams) {
        result.push_back(x.second);
    }
    for (auto& x : _receiving_streams) {
        result.push_back(x.second);
    }
    return result;
}

void stream_manager::update_progress(UUID cf_id, gms::inet_address peer, progress_info::direction dir, size_t fm_size) {
    auto& sbytes = _stream_bytes[cf_id];
    if (dir == progress_info::direction::OUT) {
        sbytes[peer].bytes_sent += fm_size;
        _total_outgoing_bytes += fm_size;
    } else {
        sbytes[peer].bytes_received += fm_size;
        _total_incoming_bytes += fm_size;
    }
}

future<> stream_manager::update_all_progress_info() {
    return seastar::async([this] {
        for (auto sr: get_all_streams()) {
            for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
                session->update_progress().get();
            }
        }
    });
}

void stream_manager::remove_progress(UUID plan_id) {
    _stream_bytes.erase(plan_id);
}

stream_bytes stream_manager::get_progress(UUID plan_id, gms::inet_address peer) const {
    auto it = _stream_bytes.find(plan_id);
    if (it == _stream_bytes.end()) {
        return stream_bytes();
    }
    auto const& sbytes = it->second;
    auto i = sbytes.find(peer);
    if (i == sbytes.end()) {
        return stream_bytes();
    }
    return i->second;
}

stream_bytes stream_manager::get_progress(UUID plan_id) const {
    auto it = _stream_bytes.find(plan_id);
    if (it == _stream_bytes.end()) {
        return stream_bytes();
    }
    stream_bytes ret;
    for (auto const& x : it->second) {
        ret += x.second;
    }
    return ret;
}

future<> stream_manager::remove_progress_on_all_shards(UUID plan_id) {
    return get_stream_manager().invoke_on_all([plan_id] (auto& sm) {
        sm.remove_progress(plan_id);
    });
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(UUID plan_id, gms::inet_address peer) const {
    return get_stream_manager().map_reduce0(
        [plan_id, peer] (auto& sm) {
            return sm.get_progress(plan_id, peer);
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(UUID plan_id) const {
    return get_stream_manager().map_reduce0(
        [plan_id] (auto& sm) {
            return sm.get_progress(plan_id);
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(gms::inet_address peer) const {
    return get_stream_manager().map_reduce0(
        [peer] (auto& sm) {
            stream_bytes ret;
            for (auto& sbytes : sm._stream_bytes) {
                ret += sbytes.second[peer];
            }
            return ret;
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards() const {
    return get_stream_manager().map_reduce0(
        [] (auto& sm) {
            stream_bytes ret;
            for (auto& sbytes : sm._stream_bytes) {
                for (auto& sb : sbytes.second) {
                    ret += sb.second;
                }
            }
            return ret;
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

stream_bytes stream_manager::get_progress_on_local_shard() const {
    stream_bytes ret;
    for (auto const& sbytes : _stream_bytes) {
        for (auto const& sb : sbytes.second) {
            ret += sb.second;
        }
    }
    return ret;
}

bool stream_manager::has_peer(inet_address endpoint) const {
    for (auto sr : get_all_streams()) {
        for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
            if (session->peer == endpoint) {
                return true;
            }
        }
    }
    return false;
}

void stream_manager::fail_sessions(inet_address endpoint) {
    for (auto sr : get_all_streams()) {
        for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
            if (session->peer == endpoint) {
                session->close_session(stream_session_state::FAILED);
            }
        }
    }
}

void stream_manager::fail_all_sessions() {
    for (auto sr : get_all_streams()) {
        for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
            session->close_session(stream_session_state::FAILED);
        }
    }
}

void stream_manager::on_remove(inet_address endpoint) {
    if (has_peer(endpoint)) {
        sslog.info("stream_manager: Close all stream_session with peer = {} in on_remove", endpoint);
        //FIXME: discarded future.
        (void)get_stream_manager().invoke_on_all([endpoint] (auto& sm) {
            sm.fail_sessions(endpoint);
        }).handle_exception([endpoint] (auto ep) {
            sslog.warn("stream_manager: Fail to close sessions peer = {} in on_remove", endpoint);
        });
    }
}

void stream_manager::on_restart(inet_address endpoint, endpoint_state ep_state) {
    if (has_peer(endpoint)) {
        sslog.info("stream_manager: Close all stream_session with peer = {} in on_restart", endpoint);
        //FIXME: discarded future.
        (void)get_stream_manager().invoke_on_all([endpoint] (auto& sm) {
            sm.fail_sessions(endpoint);
        }).handle_exception([endpoint] (auto ep) {
            sslog.warn("stream_manager: Fail to close sessions peer = {} in on_restart", endpoint);
        });
    }
}

void stream_manager::on_dead(inet_address endpoint, endpoint_state ep_state) {
    if (has_peer(endpoint)) {
        sslog.info("stream_manager: Close all stream_session with peer = {} in on_dead", endpoint);
        //FIXME: discarded future.
        (void)get_stream_manager().invoke_on_all([endpoint] (auto& sm) {
            sm.fail_sessions(endpoint);
        }).handle_exception([endpoint] (auto ep) {
            sslog.warn("stream_manager: Fail to close sessions peer = {} in on_dead", endpoint);
        });
    }
}

} // namespace streaming
