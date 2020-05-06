/*
 * Copyright (C) 2020 ScyllaDB
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

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/noncopyable_function.hh>
#include "seastarx.hh"

#include <map>
#include <chrono>
#include <optional>
#include <exception>

namespace utils {


// Manages sessions, which are indexed by Key.
// In this context, a session is an object that holds some state (SessionData)
// which can be manipulated under lock. Session state may be initialized or not,
// and can be scheduled to be disposed after a specified timeout.

template<typename Key, typename SessionData>
class expiring_session_manager {
private:
    class session_container;

    // session_map must be a collection that does not invalidate other iterators
    // when inserting/erasing
    using session_map = std::map<Key, lw_shared_ptr<session_container>>;
    using session_iterator = typename session_map::iterator;
    using clock_type = lowres_clock;

public:
    class session_ref;

    using disposer = noncopyable_function<future<>(SessionData&)>;
    using operation = noncopyable_function<future<>(session_ref)>;

private:
    class session_container final : public enable_lw_shared_from_this<session_container> {
    private:
        session_map& _map;
        session_iterator _it;
        semaphore _operation_semaphore = {1};
        uint64_t _generation = 0;
        timer<clock_type> _timeout;

        std::optional<SessionData> _data;

    public:
        session_container(session_map& map, session_iterator it) : _map(map), _it(it) {}

        session_container(const session_container&) = delete;
        session_container(session_container&&) = delete;
        session_container& operator=(const session_container&) = delete;
        session_container& operator=(session_container&&) = delete;

        template<typename F>
        future<> serialized(F&& f) {
            return with_semaphore(_operation_semaphore, 1, std::forward<F>(f));
        }

        void remove_from_map_if_unused() {
            if (_it != _map.end() && _operation_semaphore.available_units() == 1 && !bool(_data)) {
                _map.erase(_it);
                _it = _map.end();
            }
        }

        const Key& get_key() const { return _it->first; }
        SessionData* get_data() { return &*_data; }
        bool is_initialized() const { return bool(_data); }

        template<typename... Ts>
        void initialize_session_data(Ts&&... args) {
            _data.emplace(std::forward<Ts>(args)...);
        }

        void dispose_session_data() {
            if (_timeout.armed()) {
                _timeout.cancel();
            }
            _generation++;
            _data.reset();
        }

        void dispose_after_timeout(clock_type::duration d, disposer dispose) {
            if (!_data) {
                return;
            }
            _generation++;
            _timeout.set_callback([this, callback_generation = _generation, dispose = std::move(dispose)] () mutable {
                (void)serialized([this, callback_generation, dispose = std::move(dispose)] () mutable {
                    if (callback_generation != _generation || !_data) {
                        return make_ready_future<>();
                    }
                    return futurize_apply(std::move(dispose), std::ref(*_data)).then([this] {
                        dispose_session_data();
                    });
                }).finally([this, keep_alive = this->shared_from_this()] {
                    remove_from_map_if_unused();
                });
            });
            _timeout.rearm(clock_type::now() + d);
        }
    };

public:
    // A reference to a session.
    //
    // This proxy structure exists because some of the methods related
    // to the session_container need to be public, but are unwanted
    // in the public interface:
    //    make_lw_shared requires a public constructor,
    //    enable_lw_shared_from_this requires public inheritance.
    //
    // Because constructing session_container or obtaining a shared
    // pointer to it is undesirable outside expiring_session_manager,
    // session_ref is introduced which holds a reference to the container
    // and forwards only some of the methods.
    class session_ref {
    private:
        session_container& _cont;

    private:
        session_ref(session_container& cont) : _cont(cont) {}

    public:
        const Key& get_key() const { return _cont.get_key(); }
        SessionData* get_data() { return _cont.get_data(); }
        bool is_initialized() const { return _cont.is_initialized(); }

        // Constructs the session data object, using given arguments.
        template<typename... Ts>
        void initialize_session_data(Ts&&... args) {
            _cont.initialize_session_data(std::forward<Ts>(args)...);
        }

        // Destroys session data object associated with this session.
        // If dispose after timeout was scheduled, it is canceled.
        void dispose_session_data() {
            _cont.dispose_session_data();
        }

        // Schedules a dispose operation to be run after timeout expires.
        // Session data is disposed under session lock, therefore it does
        // not race with do_with_session.
        // If session data is disposed before timeout is reached, dispose
        // after timeout is canceled.
        // If another dispose was scheduled before, it is canceled and the
        // newer dispose operation is scheduled.
        // `Dispose` can be a future.
        void dispose_after_timeout(clock_type::duration d, disposer dispose) {
            _cont.dispose_after_timeout(d, std::move(dispose));
        }

        friend class expiring_session_manager<Key, SessionData>;
    };

private:
    session_map _sessions;

public:
    // Performs an operation on a session under lock.
    // Function f obtains a session_ref object which can be used to manipulate
    // the session data - create, destroy, modify it, or schedule disposal
    // after timeout.
    future<> do_with_session(const Key& key, operation op) {
        auto it = _sessions.find(key);
        if (it == _sessions.end()) {
            it = _sessions.emplace(key, nullptr).first;
            try {
                it->second = make_lw_shared<session_container>(_sessions, it);
            } catch (...) {
                // make_lw_shared can OOM
                _sessions.erase(it);
                throw;
            }
        }
        auto sess = it->second;

        return sess->serialized([this, sess, op = std::move(op)] () mutable {
            return futurize_invoke(std::move(op), session_ref(*sess));
        }).finally([this, sess] {
            sess->remove_from_map_if_unused();
        });
    }
};

}
