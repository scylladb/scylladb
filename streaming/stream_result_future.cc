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

#include "streaming/stream_result_future.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_exception.hh"
#include "log.hh"
#include <cfloat>

namespace streaming {

extern logging::logger sslog;

future<stream_state> stream_result_future::init_sending_side(UUID plan_id_, sstring description_,
        std::vector<stream_event_handler*> listeners_, shared_ptr<stream_coordinator> coordinator_) {
    auto sr = ::make_shared<stream_result_future>(plan_id_, description_, coordinator_);
    get_local_stream_manager().register_sending(sr);

    for (auto& listener : listeners_) {
        sr->add_event_listener(listener);
    }

    sslog.info("[Stream #{}] Executing streaming plan for {} with peers={}, master", plan_id_,  description_, coordinator_->get_peers());

    // Initialize and start all sessions
    for (auto& session : coordinator_->get_all_stream_sessions()) {
        session->init(sr);
    }
    coordinator_->connect_all_stream_sessions();

    return sr->_done.get_future();
}

shared_ptr<stream_result_future> stream_result_future::init_receiving_side(UUID plan_id, sstring description, inet_address from) {
    auto& sm = get_local_stream_manager();
    auto sr = sm.get_receiving_stream(plan_id);
    if (sr) {
        auto err = sprint("[Stream #%s] GOT PREPARE_MESSAGE from %s, description=%s,"
                          "stream_plan exists, duplicated message received?", plan_id, description, from);
        sslog.warn(err.c_str());
        throw std::runtime_error(err);
    }
    sslog.info("[Stream #{}] Executing streaming plan for {} with peers={}, slave", plan_id, description, from);
    bool is_receiving = true;
    sr = ::make_shared<stream_result_future>(plan_id, description, is_receiving);
    sm.register_receiving(sr);
    return sr;
}

void stream_result_future::handle_session_prepared(shared_ptr<stream_session> session) {
    auto si = session->make_session_info();
    sslog.debug("[Stream #{}] Prepare completed with {}. Receiving {}, sending {}",
               session->plan_id(),
               session->peer,
               si.get_total_files_to_receive(),
               si.get_total_files_to_send());
    auto event = session_prepared_event(plan_id, si);
    session->get_session_info() = si;
    fire_stream_event(std::move(event));
}

void stream_result_future::handle_session_complete(shared_ptr<stream_session> session) {
    sslog.debug("[Stream #{}] Session with {} is complete, state={}", session->plan_id(), session->peer, session->get_state());
    auto event = session_complete_event(session);
    fire_stream_event(std::move(event));
    auto si = session->make_session_info();
    session->get_session_info() = si;
    maybe_complete();
}

template <typename Event>
void stream_result_future::fire_stream_event(Event event) {
    // delegate to listener
    for (auto listener : _event_listeners) {
        listener->handle_stream_event(std::move(event));
    }
}

void stream_result_future::maybe_complete() {
    auto has_active_sessions = _coordinator->has_active_sessions();
    auto plan_id = this->plan_id;
    sslog.debug("[Stream #{}] stream_result_future: has_active_sessions={}", plan_id, has_active_sessions);
    if (!has_active_sessions) {
        auto& sm = get_local_stream_manager();
        if (sslog.is_enabled(logging::log_level::debug)) {
            sm.show_streams();
        }
        auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(lowres_clock::now() - _start_time).count();
        auto stats = make_lw_shared<sstring>("");
        //FIXME: discarded future.
        (void)sm.get_progress_on_all_shards(plan_id).then([plan_id, duration, stats] (auto sbytes) {
            auto tx_bw = sstring("0");
            auto rx_bw = sstring("0");
            if (std::fabs(duration) > FLT_EPSILON) {
                tx_bw = format("{:.2f}", sbytes.bytes_sent / duration / 1024);
                rx_bw = format("{:.2f}", sbytes.bytes_received  / duration / 1024);
            }
            *stats = format("tx={:d} KiB, {} KiB/s, rx={:d} KiB, {} KiB/s", sbytes.bytes_sent / 1024, tx_bw, sbytes.bytes_received / 1024, rx_bw);
        }).handle_exception([plan_id] (auto ep) {
            sslog.warn("[Stream #{}] Fail to get progress on all shards: {}", plan_id, ep);
        }).finally([this, plan_id, stats, &sm] () {
            sm.remove_stream(plan_id);
            auto final_state = get_current_state();
            if (final_state.has_failed_session()) {
                sslog.warn("[Stream #{}] Streaming plan for {} failed, peers={}, {}", plan_id, description, _coordinator->get_peers(), *stats);
                _done.set_exception(stream_exception(final_state, "Stream failed"));
            } else {
                sslog.info("[Stream #{}] Streaming plan for {} succeeded, peers={}, {}", plan_id, description, _coordinator->get_peers(), *stats);
                _done.set_value(final_state);
            }
        });
    }
}

stream_state stream_result_future::get_current_state() {
    return stream_state(plan_id, description, _coordinator->get_all_session_info());
}

void stream_result_future::handle_progress(progress_info progress) {
    fire_stream_event(progress_event(plan_id, std::move(progress)));
}

} // namespace streaming
