/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service/session.hh"
#include "log.hh"

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
    auto lock = co_await get_units(_session_drain_sem, 1);
    auto i = _closing_sessions.begin();
    while (i != _closing_sessions.end()) {
        session& s = *i;
        ++i;
        auto id = s.id();
        slogger.debug("draining session {}", id);
        co_await s.close();
        if (_sessions.erase(id)) {
            slogger.debug("session {} closed", id);
        }
    }
}

} // namespace service
