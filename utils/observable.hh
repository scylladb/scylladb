/*
 * Copyright (C) 2018 ScyllaDB
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

#include <seastar/util/noncopyable_function.hh>
#include <vector>
#include <boost/range/algorithm/replace.hpp>
#include <boost/range/algorithm/remove.hpp>

namespace utils {

// observable/observer - a publish/subscribe utility
//
// An observable is an object that can broadcast notifications
// about changes in some state. An observer listens for such notifications
// in a particular observable it is connected to. Multiple observers can
// observe a single observable.
//
// A connection between an observer and an observable is established when
// the observer is constructed (using observable::observe()); from then
// on their life cycles are separate, either can be moved or destroyed
// without affecting the other.
//
// During construction, the observer specifies how to react to a change
// in the observable's state by specifying a function to be called on
// a state change. An observable causes the function to be executed
// by calling its operator()() method.
//
// All observers are called without preemption, so an observer should have
// a small number of observers.

template <typename... Args>
class observable {
public:
    class observer;
private:
    std::vector<observer*> _observers;
public:
    class observer {
        friend class observable;
        observable* _observable;
        seastar::noncopyable_function<void (Args...)> _callback;
    private:
        void moved(observer* from) {
            if (_observable) {
                _observable->moved(from, this);
            }
        }
    public:
        observer(observable* o, seastar::noncopyable_function<void (Args...)> callback) noexcept
                : _observable(o), _callback(std::move(callback)) {
        }
        observer(observer&& o) noexcept
                : _observable(std::exchange(o._observable, nullptr))
                , _callback(std::move(o._callback)) {
            moved(&o);
        }
        observer& operator=(observer&& o) noexcept {
            if (this != &o) {
                disconnect();
                _observable = std::exchange(o._observable, nullptr);
                _callback = std::move(o._callback);
                moved(&o);
            }
            return *this;
        }
        ~observer() {
            disconnect();
        }
        // Stops observing the observable immediately, instead of
        // during destruction.
        void disconnect() {
            if (_observable) {
                _observable->destroyed(this);
            }
            _observable = nullptr;
        }
    };
    friend class observer;
private:
    void destroyed(observer* dead) {
        _observers.erase(boost::remove(_observers, dead), _observers.end());
    }
    void moved(observer* from, observer* to) {
        boost::replace(_observers, from, to);
    }
    void update_observers(observable* ob) {
        for (auto&& c : _observers) {
            c->_observable = ob;
        }
    }
public:
    observable() = default;
    observable(observable&& o) noexcept
            : _observers(std::move(o._observers)) {
        update_observers(this);
    }
    observable& operator=(observable&& o) noexcept {
        if (this != &o) {
            update_observers(nullptr);
            _observers = std::move(o._observers);
            update_observers(this);
        }
        return *this;
    }
    ~observable() {
        update_observers(nullptr);
    }
    // Send args to all connected observers
    void operator()(Args... args) const {
        std::exception_ptr e;
        for (auto&& ob : _observers) {
            try {
                ob->_callback(args...);
            } catch (...) {
                if (!e) {
                    e = std::current_exception();
                }
            }
        }
        if (e) {
            std::rethrow_exception(std::move(e));
        }
    }
    // Adds an observer to an observable
    observer observe(std::function<void (Args...)> callback) {
        observer ob(this, std::move(callback));
        _observers.push_back(&ob);
        return ob;
    }
};

// An observer<Args...> can receive notifications about changes
// in an observable<Args...>'s state.
template <typename... Args>
using observer = typename observable<Args...>::observer;

}
