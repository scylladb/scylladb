/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "seastar/core/future.hh"
#include "seastar/core/sleep.hh"
#include "seastar/core/seastar.hh"
#include <seastar/core/smp.hh>
#include "seastarx.hh"

#include "log.hh"

#include <algorithm>
#include <chrono>
#include <set>
#include <type_traits>
#include <concepts>
#include <optional>

#include <boost/range/adaptor/map.hpp>

namespace utils {

// Exception thrown by enabled error injection
class injected_error : public std::runtime_error {
public:
    injected_error(const sstring& err_name)
    : runtime_error{err_name} { }
};

extern logging::logger errinj_logger;

/**
 * Error injection class can be used to create and manage code injections
 * which trigger an error or a custom action in debug mode.
 *
 * Error injection is a place in application code, which is identified by
 * name and is tied to some single handler, that is defined in-place
 * at injection registration site, so cannot be redefined in the future.
 *
 * One needs to define another error injection in order to supply a
 * different kind of error handler.
 *
 * This class has a specialized version as no-op version with all
 * injections optimized away, controlled by the compile flag
 * SCYLLA_ENABLE_ERROR_INJECTION.
 *
 * Setting an injection requires two parameters - injection name,
 * which can be an arbitrary, human readable string, and a handler lambda, which
 * has the following signature: void().
 *
 * Some errors may involve overriding the future<> instance in order to inject
 * sleeps or waiting on condition variables, in which case inject()
 * should be also passed a reference to future<> instance to be intercepted.
 *
 * All injections are disabled by default. This is controlled by the
 * enable(name), enable_once(name), and disable(name) methods.
 *
 * Enabling or disabling an injection can be done either by calling this API
 * directly (e.g. in unit tests) or via REST interface (ref: api/api-doc/error_injection.json).
 * Enabled injection will be triggered (meaning its associated handler will be called)
 * once an injection with matching name is checked via inject().
 *
 * Enabled check is done at injection time. But if in the future it is
 * required to be checked inside the continuation the code must be updated.
 *
 * There are two predefined injections:
 *
 * 1. inject(name, duration in milliseconds, future)
 *    Sleeps for a given amount of milliseconds. This is seastar::sleep,
 *    not a reactor stall. Requires future<> reference passed to be modified.
 *    Expected use case: slowing down the process.
 *    e.g. making view update generation process extremely slow.
 *
 * 2. inject(name, deadline as time_point in the future, future)
 *    Sleeps until given deadline. This is seastar::sleep,
 *    not a reactor stall. Requires future<> reference passed to be modified.
 *    Expected use case: slowing down the process so it hits external timeouts.
 *    e.g. making view update generation process extremely slow.
 *
 * 3. inject(name, future, exception_factory_lambda)
 *    Inserts code to raise a given exception type (if enabled).
 *    Requires future<> reference passed and an exception factory returning an
 *    exception pointer, for example:
 *          inject("exc", [] () {
 *              return std::make_exception_ptr(std::runtime_error("test"));
 *          }, f);
 *    Expected use case: emulate custom errors like timeouts.
 *
 */

template <bool injection_enabled>
class error_injection {
    inline static thread_local error_injection _local;
    using handler_fun = std::function<void()>;

    // String cross-type comparator
    class str_less
    {
    public:
        using is_transparent = std::true_type;

        template<typename TypeLeft, typename TypeRight>
        bool operator()(const TypeLeft& left, const TypeRight& right) const
        {
            return left < right;
        }
    };
    // Map enabled-injection-name -> is-one-shot
    // TODO: change to unordered_set once we have heterogeneous lookups
    std::map<sstring, bool, str_less> _enabled;

    bool is_enabled(const std::string_view& injection_name) const {
        return _enabled.contains(injection_name);
    }

    bool is_one_shot(const std::string_view& injection_name) const {
        const auto it = _enabled.find(injection_name);
        if (it == _enabled.end()) {
            return false;
        }
        return it->second;
    }

public:
    // \brief Enter into error injection if it's enabled
    // \param name error injection name to check
    bool enter(const std::string_view& name) {
        if (!is_enabled(name)) {
            return false;
        }
        if (is_one_shot(name)) {
            disable(name);
        }
        return true;
    }

    void enable(const std::string_view& injection_name, bool one_shot = false) {
        _enabled.emplace(injection_name, one_shot);
        errinj_logger.debug("Enabling injection {} \"{}\"",
                one_shot? "one-shot ": "", injection_name);
    }

    void disable(const std::string_view& injection_name) {
        // TODO: plain erase once _enabled has heterogeneous lookups
        auto it = _enabled.find(injection_name);
        if (it == _enabled.end()) {
            return;
        }
        _enabled.erase(it);
    }

    void disable_all() {
        _enabled.clear();
    }

    std::vector<sstring> enabled_injections() const {
        return boost::copy_range<std::vector<sstring>>(_enabled | boost::adaptors::map_keys);
    }

    // \brief Inject a lambda call
    // \param f lambda to be run
    [[gnu::always_inline]]
    void inject(const std::string_view& name, handler_fun f) {
        if (!is_enabled(name)) {
            return;
        }
        if (is_one_shot(name)) {
            disable(name);
        }
        errinj_logger.debug("Triggering injection \"{}\"", name);
        f();
    }

    // \brief Inject a sleep for milliseconds
    [[gnu::always_inline]]
    future<> inject(const std::string_view& name,
            const std::chrono::milliseconds duration) {

        if (!is_enabled(name)) {
            return make_ready_future<>();
        }
        if (is_one_shot(name)) {
            disable(name);
        }
        errinj_logger.debug("Triggering sleep injection \"{}\" ({}ms)", name, duration.count());
        return seastar::sleep(duration);
    }

    // \brief Inject a sleep to deadline (timeout)
    template <typename Clock, typename Duration>
    [[gnu::always_inline]]
    future<> inject(const std::string_view& name, std::chrono::time_point<Clock, Duration> deadline) {

        if (!is_enabled(name)) {
            return make_ready_future<>();
        }
        if (is_one_shot(name)) {
            disable(name);
        }

        // Time left until deadline
        std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
        errinj_logger.debug("Triggering sleep injection \"{}\" ({}ms)", name, duration.count());
        return seastar::sleep<Clock>(duration);
    }

    // \brief Inject a sleep to deadline with lambda(timeout)
    // Avoid adding a sleep continuation in the chain for disabled error injection
    template <typename Clock, typename Duration, typename Func>
    [[gnu::always_inline]]
    std::result_of_t<Func()> inject(const std::string_view& name, std::chrono::time_point<Clock, Duration> deadline,
                Func&& func) {
        if (is_enabled(name)) {
            if (is_one_shot(name)) {
                disable(name);
            }
            std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
            errinj_logger.debug("Triggering sleep injection \"{}\" ({}ms)", name, duration.count());
            return seastar::sleep<Clock>(duration).then([func = std::move(func)] {
                    return func(); });
        } else {
            return func();
        }
    }

    // \brief Inject exception
    // \param exception_factory function returning an exception pointer
    template <typename Func>
    requires std::is_invocable_r_v<std::exception_ptr, Func>
    [[gnu::always_inline]]
    future<>
    inject(const std::string_view& name,
            Func&& exception_factory) {

        if (!is_enabled(name)) {
            return make_ready_future<>();
        }
        if (is_one_shot(name)) {
            disable(name);
        }
        errinj_logger.debug("Triggering exception injection \"{}\"", name);
        return make_exception_future<>(exception_factory());
    }

    future<> enable_on_all(const std::string_view& injection_name, bool one_shot = false) {
        return smp::invoke_on_all([injection_name = sstring(injection_name), one_shot] {
            auto& errinj = _local;
            errinj.enable(injection_name, one_shot);
        });
    }

    static future<> disable_on_all(const std::string_view& injection_name) {
        return smp::invoke_on_all([injection_name = sstring(injection_name)] {
            auto& errinj = _local;
            errinj.disable(injection_name);
        });
    }

    static future<> disable_on_all() {
        return smp::invoke_on_all([] {
            auto& errinj = _local;
            errinj.disable_all();
        });
    }

    static std::vector<sstring> enabled_injections_on_all() {
        // TODO: currently we always enable an injection on all shards at once,
        // so returning the list from the current shard will do.
        // In future different shards may have different enabled sets,
        // in which case we may want to extend the API.
        auto& errinj = _local;
        return errinj.enabled_injections();
    }

    static error_injection& get_local() {
        return _local;
    }
};


// no-op, should be optimized away
template <>
class error_injection<false> {
    static thread_local error_injection _local;
    using handler_fun = std::function<void()>;
public:
    bool enter(const std::string_view& name) const {
        return false;
    }

    [[gnu::always_inline]]
    void enable(const std::string_view& injection_name, const bool one_shot = false) {}

    [[gnu::always_inline]]
    void disable(const std::string_view& injection_name) {}

    [[gnu::always_inline]]
    void disable_all() { }

    [[gnu::always_inline]]
    std::vector<sstring> enabled_injections() const { return {}; };

    // Inject a lambda call
    [[gnu::always_inline]]
    void inject(const std::string_view& name, handler_fun f) { }

    // Inject sleep
    [[gnu::always_inline]]
    future<> inject(const std::string_view& name,
            const std::chrono::milliseconds duration) {
        return make_ready_future<>();
    }

    // \brief Inject a sleep to deadline (timeout)
    template <typename Clock, typename Duration>
    [[gnu::always_inline]]
    future<> inject(const std::string_view& name, std::chrono::time_point<Clock, Duration> deadline) {
        return make_ready_future<>();
    }

    // \brief Inject a sleep to deadline (timeout) with lambda
    // Avoid adding a continuation in the chain for disabled error injections
    template <typename Clock, typename Duration, typename Func>
    [[gnu::always_inline]]
    std::result_of_t<Func()> inject(const std::string_view& name, std::chrono::time_point<Clock, Duration> deadline,
                Func&& func) {
        return func();
    }

    // Inject exception
    template <typename Func>
    requires std::is_invocable_r_v<std::exception_ptr, Func>
    [[gnu::always_inline]]
    future<>
    inject(const std::string_view& name,
            Func&& exception_factory) {
        return make_ready_future<>();
    }

    [[gnu::always_inline]]
    static future<> enable_on_all(const std::string_view& injection_name, const bool one_shot = false) {
        return make_ready_future<>();
    }

    [[gnu::always_inline]]
    static future<> disable_on_all(const std::string_view& injection_name) {
        return make_ready_future<>();
    }

    [[gnu::always_inline]]
    static future<> disable_on_all() {
        return make_ready_future<>();
    }

    [[gnu::always_inline]]
    static std::vector<sstring> enabled_injections_on_all() { return {}; }

    static error_injection& get_local() {
        return _local;
    }
};

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
using error_injection_type = error_injection<true>;        // debug, dev
#else
using error_injection_type = error_injection<false>;       // release
#endif

inline error_injection_type& get_local_injector() {
    return error_injection_type::get_local();
}

} // namespace utils
