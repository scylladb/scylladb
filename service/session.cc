/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/session.hh"
#include "utils/log.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/timer.hh>

namespace service {

static logging::logger slogger("session");


session::guard::guard(session& s)
    : _session(&s)
    , _holder(s._gate.hold()) {
}

session::guard::~guard() {
}

session_manager::session_manager() {
    create_session(default_session_id);
}

session::guard session_manager::enter_session(session_id id) {
    auto i = _sessions.find(id);
    if (i == _sessions.end()) {
        throw std::runtime_error(fmt::format("Session not found: {}", id));
    }
    auto guard = i->second->enter();
    slogger.debug("session {} entered", id);
    return guard;
}

void session_manager::create_session(session_id id) {
    auto [i, created] = _sessions.emplace(id, std::make_unique<session>(id));
    if (created) {
        slogger.debug("session {} created", id);
    } else {
        slogger.debug("session {} already exists", id);
    }
}

void session_manager::initiate_close_of_sessions_except(const std::unordered_set<session_id>& keep) {
    for (auto&& [id, session] : _sessions) {
        if (id != default_session_id && !keep.contains(id)) {
            if (!session->is_closing()) {
                _closing_sessions.push_front(*session);
            }
            session->start_closing();
        }
    }
}

future<> session_manager::drain_closing_sessions() {
    slogger.info("drain_closing_sessions: waiting for lock");
    seastar::timer<lowres_clock> lock_timer([this] {
        slogger.warn("drain_closing_sessions: still waiting for lock, available units {}",
                     _session_drain_sem.available_units());
    });
    lock_timer.arm_periodic(std::chrono::minutes(5));
    auto lock = co_await get_units(_session_drain_sem, 1);
    lock_timer.cancel();
    auto n = std::distance(_closing_sessions.begin(), _closing_sessions.end());
    slogger.info("drain_closing_sessions: acquired lock, {} sessions to drain", n);
    auto i = _closing_sessions.begin();
    while (i != _closing_sessions.end()) {
        session& s = *i;
        ++i;
        auto id = s.id();
        slogger.info("drain_closing_sessions: waiting for session {} to close, gate count {}", id, s.gate_count());
        std::optional<seastar::timer<lowres_clock>> warn_timer;
        warn_timer.emplace([&s, id] {
            slogger.warn("drain_closing_sessions: session {} still not closed, gate count {}",
                         id, s.gate_count());
        });
        warn_timer->arm_periodic(std::chrono::minutes(5));
        co_await s.close();
        warn_timer.reset();
        if (_sessions.erase(id)) {
            slogger.info("drain_closing_sessions: session {} closed", id);
        }
    }
    slogger.info("drain_closing_sessions: done");
}

} // namespace service
