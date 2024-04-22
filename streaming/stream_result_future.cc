/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_result_future.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_exception.hh"
#include "log.hh"
#include <cfloat>
#include <fmt/ranges.h>

namespace streaming {

extern logging::logger sslog;

future<stream_state> stream_result_future::init_sending_side(stream_manager& mgr, streaming::plan_id plan_id_, sstring description_,
        std::vector<stream_event_handler*> listeners_, shared_ptr<stream_coordinator> coordinator_) {
    auto sr = ::make_shared<stream_result_future>(mgr, plan_id_, description_, coordinator_);
    mgr.register_sending(sr);

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

shared_ptr<stream_result_future> stream_result_future::init_receiving_side(stream_manager& mgr, streaming::plan_id plan_id, sstring description, inet_address from) {
    auto sr = mgr.get_receiving_stream(plan_id);
    if (sr) {
        auto err = fmt::format("[Stream #{}] GOT PREPARE_MESSAGE from {}, description={},"
                          "stream_plan exists, duplicated message received?", plan_id, description, from);
        sslog.warn("{}", err);
        throw std::runtime_error(err);
    }
    sslog.info("[Stream #{}] Executing streaming plan for {} with peers={}, slave", plan_id, description, from);
    bool is_receiving = true;
    sr = ::make_shared<stream_result_future>(mgr, plan_id, description, is_receiving);
    mgr.register_receiving(sr);
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
        listener->handle_stream_event(event);
    }
}

void stream_result_future::maybe_complete() {
    auto has_active_sessions = _coordinator->has_active_sessions();
    auto plan_id = this->plan_id;
    sslog.debug("[Stream #{}] stream_result_future: has_active_sessions={}", plan_id, has_active_sessions);
    if (!has_active_sessions) {
        if (sslog.is_enabled(logging::log_level::debug)) {
            _mgr.show_streams();
        }
        auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(lowres_clock::now() - _start_time).count();
        auto stats = make_lw_shared<sstring>("");
        //FIXME: discarded future.
        (void)_mgr.get_progress_on_all_shards(plan_id).then([duration, stats] (auto sbytes) {
            auto tx_bw = sstring("0");
            auto rx_bw = sstring("0");
            if (std::fabs(duration) > FLT_EPSILON) {
                tx_bw = format("{:.2f}", sbytes.bytes_sent / duration / 1024);
                rx_bw = format("{:.2f}", sbytes.bytes_received  / duration / 1024);
            }
            *stats = format("tx={:d} KiB, {} KiB/s, rx={:d} KiB, {} KiB/s", sbytes.bytes_sent / 1024, tx_bw, sbytes.bytes_received / 1024, rx_bw);
        }).handle_exception([plan_id] (auto ep) {
            sslog.warn("[Stream #{}] Fail to get progress on all shards: {}", plan_id, ep);
        }).finally([this, plan_id, stats] () {
            _mgr.remove_stream(plan_id);
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
