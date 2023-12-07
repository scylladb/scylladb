/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/semaphore.hh>

#include <boost/intrusive/list.hpp>
#include <unordered_set>

namespace service {

using session_id = utils::tagged_uuid<struct session_id_tag>;

// We want it be different than default-constructed session_id to catch mistakes.
constexpr session_id default_session_id = session_id(
    utils::UUID(0x81e7fc5a8d4411ee, 0x8577325096b39f47)); // timeuuid 2023-11-27 16:46:27.182089.0 UTC

/// Session is used to track execution of work related to some greater task, identified by session_id.
/// Work can enter the session using enter(), and is considered to be part of the session
/// as long as the guard returned by enter() is alive.
///
/// Session goes over the following states monotonically:
///  1) open - accepts work via enter()
///  2) closing - rejects work via enter()
///  3) closed - rejects work via enter(), and no guards are alive anymore
///
/// Sessions are removed only after they are closed, it's impossible to have a session::guard of a session
/// which is not in the registry.
class session {
public:
    using link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
private:
    session_id _id;
    seastar::gate _gate;
    std::optional<shared_future<>> _closed;
    link_type _link;
public:
    using list_type = boost::intrusive::list<session,
            boost::intrusive::member_hook<session, session::link_type, &session::_link>,
            boost::intrusive::constant_time_size<false>>;
public:
    class guard {
        session* _session;
        seastar::gate::holder _holder;
    public:
        explicit guard(session& s);
        guard(const guard&) noexcept = default;
        guard(guard&&) noexcept = default;
        guard& operator=(guard&&) noexcept = default;
        guard& operator=(const guard&) noexcept = default;
        ~guard();

        void check() const {
            if (!valid()) {
                throw seastar::abort_requested_exception();
            }
        }

        [[nodiscard]] bool valid() const {
            return !_session->_closed;
        }
    };

    explicit session(session_id id) : _id(id) {}

    guard enter() {
        return guard(*this);
    };

    /// No new work is admitted to enter the session after this.
    /// Can be called many times.
    void start_closing() noexcept {
        if (!_closed) {
            _closed = seastar::shared_future<>(_gate.close());
        }
    }

    /// Returns true iff the session is not open.
    /// Calling enter() in this state will fail.
    bool is_closing() const {
        return bool(_closed);
    }

    session_id id() const {
        return _id;
    }

    /// Post-condition of successfully resolved future: There are no guards alive for this session, and
    /// and it's impossible to create more such guards later.
    /// Can be called concurrently.
    future<> close() {
        start_closing();
        return _closed->get_future();
    }
};

class session_manager {
    std::unordered_map<session_id, std::unique_ptr<session>> _sessions;
    session::list_type _closing_sessions;
    seastar::semaphore _session_drain_sem{1};
public:
    session_manager();

    session::guard enter_session(session_id id);

    /// Creates a session on this shard if it doesn't exist yet.
    /// If the session already exists does nothing.
    void create_session(session_id);

    /// Calls start_closing() on all sessions except those in keep.
    void initiate_close_of_sessions_except(const std::unordered_set<session_id>& keep);

    /// Post-condition: All sessions which are in closing state before the call will be in closed state after the call.
    /// Can be called concurrently.
    future<> drain_closing_sessions();
};

} // namespace service
