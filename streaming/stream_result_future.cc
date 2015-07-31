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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#include "streaming/stream_result_future.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_exception.hh"
#include "log.hh"

namespace streaming {

extern logging::logger sslog;

future<stream_state> stream_result_future::init(UUID plan_id_, sstring description_, std::vector<stream_event_handler*> listeners_, shared_ptr<stream_coordinator> coordinator_) {
    auto future = create_and_register(plan_id_, description_, coordinator_);
    for (auto& listener : listeners_) {
        future->add_event_listener(listener);
    }

    sslog.info("[Stream #{}] Executing streaming plan for {}", plan_id_,  description_);

    // Initialize and start all sessions
    for (auto& session : coordinator_->get_all_stream_sessions()) {
        session->init(future);
    }
    coordinator_->connect_all_stream_sessions();

    return future->_done.get_future();
}

void stream_result_future::init_receiving_side(int session_index, UUID plan_id,
    sstring description, inet_address from, bool keep_ss_table_level) {
    auto& sm = get_local_stream_manager();
    auto f = sm.get_receiving_stream(plan_id);
    if (f == nullptr) {
        sslog.info("[Stream #{} ID#{}] Creating new streaming plan for {}", plan_id, session_index, description);
        // The main reason we create a StreamResultFuture on the receiving side is for JMX exposure.
        // TODO: stream_result_future needs a ref to stream_coordinator.
        bool is_receiving = true;
        sm.register_receiving(make_shared<stream_result_future>(plan_id, description, keep_ss_table_level, is_receiving));
    }
    sslog.info("[Stream #{} ID#{}] Received streaming plan for {}", plan_id, session_index, description);
}

void stream_result_future::handle_session_prepared(shared_ptr<stream_session> session) {
    auto si = session->get_session_info();
    sslog.info("[Stream #{} ID#{}] Prepare completed. Receiving {} files({} bytes), sending {} files({} bytes)",
               session->plan_id(),
               session->session_index(),
               si.get_total_files_to_receive(),
               si.get_total_size_to_receive(),
               si.get_total_files_to_send(),
               si.get_total_size_to_send());
    auto event = session_prepared_event(plan_id, si);
    _coordinator->add_session_info(std::move(si));
    fire_stream_event(std::move(event));
}

void stream_result_future::handle_session_complete(shared_ptr<stream_session> session) {
    sslog.info("[Stream #{}] Session with {} is complete", session->plan_id(), session->peer);
    auto event = session_complete_event(session);
    fire_stream_event(std::move(event));
    auto si = session->get_session_info();
    _coordinator->add_session_info(std::move(si));
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
    if (!_coordinator->has_active_sessions()) {
        auto final_state = get_current_state();
        if (final_state.has_failed_session()) {
            sslog.warn("[Stream #{}] Stream failed", plan_id);
            _done.set_exception(stream_exception(final_state, "Stream failed"));
        } else {
            sslog.info("[Stream #{}] All sessions completed", plan_id);
            _done.set_value(final_state);
        }
    }
}

stream_state stream_result_future::get_current_state() {
    return stream_state(plan_id, description, _coordinator->get_all_session_info());
}

void stream_result_future::handle_progress(progress_info progress) {
    _coordinator->update_progress(progress);
    fire_stream_event(progress_event(plan_id, std::move(progress)));
}

shared_ptr<stream_result_future> stream_result_future::create_and_register(UUID plan_id_, sstring description_, shared_ptr<stream_coordinator> coordinator_) {
    auto future = make_shared<stream_result_future>(plan_id_, description_, coordinator_);
    auto& sm = get_local_stream_manager();
    sm.register_sending(future);
    return future;
}

} // namespace streaming
