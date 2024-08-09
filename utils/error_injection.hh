/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/defer.hh>
#include "seastarx.hh"

#include "log.hh"

#include <algorithm>
#include <chrono>
#include <type_traits>
#include <optional>
#include <unordered_map>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/lexical_cast.hpp>


namespace utils {

// Exception thrown by enabled error injection
class injected_error : public std::runtime_error {
public:
    injected_error(const sstring& err_name)
    : runtime_error{err_name} { }
};

extern logging::logger errinj_logger;

using error_injection_parameters = std::unordered_map<sstring, sstring>;

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
 * The predefined injections are as follows:
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
 * 4. inject(name, future<> func(injection_handler&), share_messages)
 *    Inserts code that can wait for an event.
 *    Requires func to be a function taking an injection_handler reference and
 *    returning a future<>. Depending on the share_messages value,
 *    handlers can share events or not.
 *    Expected use case: wait for an event from tests.
 * 5. inject_parameter(name)
 *    Enables tests to inject parameters into the system, like lowering timeouts or limits, to
 *    make the tests run faster.
 *    Logically it is the same as
 *      T value{}
 *      co_await inject(name, [&value](auto& handler) { value = handler.get("value"); }
 *    The function simply returns the 'value' parameter.
 *    Expected use case: adjusting system parameters for tests.
 * 6. set_parameter(injection_name, parameter_name, parameter_value)
 *    Enable tests to observe the value of internal parameters, without having to
 *    publicly expose said parameters.
 *    This is the opposite of inject_parameter(name).
 *    Expected use case: validate that certain code path was taken, and/or the
 *    value of some parameter is the expected value, on given code-path.
 */

template <bool injection_enabled>
class error_injection {
    inline static thread_local error_injection _local;
    using handler_fun = std::function<void()>;

    /**
     * It is shared between the injection_data. It is created once when enabling an injection
     * on a given shard, and all injection_handlers, that are created separately for each firing of this injection.
     */
    struct injection_shared_data {
        size_t received_message_count{0};
        size_t shared_read_message_count{0};
        condition_variable received_message_cv;
        error_injection_parameters parameters;
        sstring injection_name;

        explicit injection_shared_data(error_injection_parameters parameters, std::string_view injection_name)
            : parameters(std::move(parameters))
            , injection_name(injection_name)
            {}

        template <typename T>
        std::optional<T> get(sstring name) const {
            const auto it = parameters.find(name);
            if (it == parameters.end()) {
                return std::nullopt;
            }
            const auto& s = it->second;
            errinj_logger.debug("Injected value [{}] for parameter [{}], injection [{}]",
                s, name, injection_name);
            if constexpr (std::is_same_v<T, std::string_view>) {
                return s;
            } else {
                return boost::lexical_cast<T>(s.data(), s.size());
            }
        }
    };

    class injection_data;
public:
    /**
     * The injection handler class is used to wait for events inside the injected code.
     * If multiple inject (with handler) are called concurrently for the same injection_name,
     * all of them will have separate handlers.
     *
     * Handlers can be of two types depending on the share_messages value passed to inject
     * (with handler):
     * 1. By default, handlers share received messages. It means that every message can be
     * received by all handlers (even if they start waiting in the future).
     * 2. When handlers do not share received messages, only one can receive a specific
     * message. Other handlers will wait for new messages.
     *
     * For a single injection, these two types of handlers are independent. A handler of one
     * type never impacts a handler of the second type.
     *
     * In most cases, using the default type is sufficient or required. The second type
     * allows waiting for new messages during every execution of the injected code.
     */
    class injection_handler: public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        lw_shared_ptr<injection_shared_data> _shared_data;
        size_t _read_messages_counter{0};
        bool _share_messages;

        explicit injection_handler(lw_shared_ptr<injection_shared_data> shared_data, bool share_messages)
            : _shared_data(std::move(shared_data)), _share_messages(share_messages) {}

    public:
        template <typename Clock, typename Duration>
        future<> wait_for_message(std::chrono::time_point<Clock, Duration> timeout) {
            if (!_shared_data) {
                on_internal_error(errinj_logger, "injection_shared_data is not initialized");
            }

            try {
                co_await _shared_data->received_message_cv.wait(timeout, [&] {
                    if (!_share_messages) {
                        bool wakes_up = _shared_data->shared_read_message_count < _shared_data->received_message_count;
                        if (wakes_up) {
                            // Increase shared_read_message_count here, so other sharing handlers don't wake up.
                            ++_shared_data->shared_read_message_count;
                        }
                        return wakes_up;
                    }

                    return _read_messages_counter < _shared_data->received_message_count;
                });
            }
            catch (const std::exception& e) {
                on_internal_error(errinj_logger, "Error injection wait_for_message timeout: " + std::string(e.what()));
            }
            ++_read_messages_counter;
        }

        // \brief Checks if there is an unreceived message.
        // If yes, returns true and marks the message as received.
        bool poll_for_message() {
            if (!_shared_data) {
                on_internal_error(errinj_logger, "injection_shared_data is not initialized");
            }

            if (_share_messages && _read_messages_counter < _shared_data->received_message_count) {
                ++_read_messages_counter;
                return true;
            }
            if (!_share_messages && _shared_data->shared_read_message_count < _shared_data->received_message_count) {
                ++_shared_data->shared_read_message_count;
                return true;
            }
            return false;
        }

        template <typename T = std::string_view>
        std::optional<T> get(std::string_view key) const {
            if (!_shared_data) {
                on_internal_error(errinj_logger, "injection_shared_data is not initialized");
            }
            return _shared_data->template get<T>(std::string(key));
        }

        friend class error_injection;
    };

private:
    using waiting_handler_fun = std::function<future<>(injection_handler&)>;

    /**
     * - there is a counter of received messages; it is shared between the injection_data,
     *   which is created once when enabling an injection on a given shard, and all injection_handlers,
     *   that are created separately for each firing of this injection.
     * - the counter is incremented when receiving a message from the REST endpoint and the condition variable is signaled.
     *
     * Handlers sharing messages:
     * - each injection_handler (separate for each firing) stores its own private counter, _read_messages_counter.
     * - that private counter is incremented whenever we wait for a message, and compared to the received counter.
     *   We sleep on the condition variable if not enough messages were received.
     *
     * Handlers not sharing messages:
     * - injection_shared_data stores a counter, shared_read_message_count, which is shared by all handlers.
     * - that shared counter is incremented whenever a handler finishes waiting for a message. While waiting for
     *   a message, a handler compares this counter to the received counter. It sleeps on the condition variable if
     *   they are equal.
     */
    struct injection_data {
        bool one_shot;
        lw_shared_ptr<injection_shared_data> shared_data;
        bi::list<injection_handler, bi::constant_time_size<false>> handlers;

        explicit injection_data(bool one_shot, error_injection_parameters parameters, std::string_view injection_name)
            : one_shot(one_shot)
            , shared_data(make_lw_shared<injection_shared_data>(std::move(parameters), injection_name)) {}

        void receive_message() {
            SCYLLA_ASSERT(shared_data);

            ++shared_data->received_message_count;
            shared_data->received_message_cv.broadcast();
        }

        bool is_one_shot() const {
            return one_shot;
        }

        bool is_ongoing_oneshot() const {
            return is_one_shot() && !handlers.empty();
        }
    };

    // Map enabled-injection-name -> is-one-shot
    std::unordered_map<std::string_view, injection_data> _enabled;

    bool is_one_shot(const std::string_view& injection_name) const {
        const auto it = _enabled.find(injection_name);
        if (it == _enabled.end()) {
            return false;
        }
        return it->second.one_shot;
    }

    injection_data const* get_data(const std::string_view& injection_name) const {
        const auto it = _enabled.find(injection_name);
        if (it == _enabled.end()) {
            return nullptr;
        }
        return &it->second;
    }

    injection_data* get_data(const std::string_view& injection_name) {
        const auto it = _enabled.find(injection_name);
        if (it == _enabled.end()) {
            return nullptr;
        }
        return &it->second;
    }

public:
    // \brief Returns true iff the injection is enabled.
    // \param name error injection name to check
    bool is_enabled(const std::string_view& injection_name) const {
        auto data = get_data(injection_name);
        return data && !data->is_ongoing_oneshot();
    }

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

    void enable(const std::string_view& injection_name, bool one_shot = false, error_injection_parameters parameters = {}) {
        auto data = injection_data{one_shot, std::move(parameters), injection_name};
        std::string_view name = data.shared_data->injection_name;
        _enabled.emplace(name, std::move(data));
        errinj_logger.debug("Enabling injection {} \"{}\"",
                one_shot? "one-shot ": "", injection_name);
    }

    void disable(const std::string_view& injection_name) {
        _enabled.erase(injection_name);
    }

    void disable_all() {
        _enabled.clear();
    }

    std::vector<sstring> enabled_injections() const {
        return boost::copy_range<std::vector<sstring>>(_enabled | boost::adaptors::filtered([] (const auto& pair) {
            return !pair.second.is_ongoing_oneshot();
        }) | boost::adaptors::map_keys);
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
        auto duration = deadline - Clock::now();
        errinj_logger.debug("Triggering sleep injection \"{}\" ({})", name, duration);
        return seastar::sleep<Clock>(duration);
    }

    // \brief Inject a sleep to deadline with lambda(timeout)
    // Avoid adding a sleep continuation in the chain for disabled error injection
    template <typename Clock, typename Duration, typename Func>
    [[gnu::always_inline]]
    std::invoke_result_t<Func> inject(const std::string_view& name, std::chrono::time_point<Clock, Duration> deadline,
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

    // \brief Inject exception
    // \param func function returning a future and taking an injection handler
    // \param share_messages if true, injection handlers share received messages
    future<> inject(const std::string_view& name, waiting_handler_fun func, bool share_messages = true) {
        auto* data = get_data(name);
        if (!data) {
            co_return;
        }

        bool one_shot = data->is_one_shot();
        if (data->is_ongoing_oneshot()) {
            // There is ongoing one-shot injection, so this one is not triggered.
            // It is not removed from _enabled to keep the data associated with the injection as long as it is needed.
            co_return;
        }

        errinj_logger.debug("Triggering injection \"{}\" with injection handler", name);
        injection_handler handler(data->shared_data, share_messages);
        data->handlers.push_back(handler);

        auto disable_one_shot = defer([this, one_shot, name = sstring(name)] {
            if (one_shot) {
                disable(name);
            }
        });

        co_await func(handler);
    }

    template <typename T = std::string_view>
    std::optional<T> inject_parameter(const std::string_view& name) {
        auto* data = get_data(name);
        if (!data) {
            return std::nullopt;
        }
        return data->shared_data->template get<T>("value");
    }

    // \brief Export the value of the parameter with the given name
    // \param injection_name the name of the error injection
    // \param parameter_name the name of the exported parameter
    // \param parameter_value the value of the exported parameter
    //
    // This method is inject_parameter() in the other direction, allows external
    // code to observe internal values.
    void set_parameter(std::string_view injection_name, sstring parameter_name, sstring parameter_value) {
        if (auto data = get_data(injection_name); data) {
            data->shared_data->parameters[std::move(parameter_name)] = std::move(parameter_value);
        }
        if (is_one_shot(injection_name)) {
            disable(injection_name);
        }
    }

    error_injection_parameters get_injection_parameters(std::string_view name) {
        if (auto data = get_data(name); data) {
            return data->shared_data->parameters;
        }
        return {};
    }

    future<> enable_on_all(const std::string_view& injection_name, bool one_shot = false, error_injection_parameters parameters = {}) {
        return smp::invoke_on_all([injection_name = sstring(injection_name), one_shot, parameters = std::move(parameters)] {
            auto& errinj = _local;
            errinj.enable(injection_name, one_shot, parameters);
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

    void receive_message(const std::string_view& injection_name) {
        if (auto* data = get_data(injection_name)) {
            data->receive_message();
        }
    }

    static future<> receive_message_on_all(const std::string_view& injection_name) {
        return smp::invoke_on_all([injection_name = sstring(injection_name)] {
            auto& errinj = _local;
            errinj.receive_message(injection_name);
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
    using waiting_handler_fun = std::function<future<>(error_injection<true>::injection_handler&)>;
public:
    bool is_enabled(const std::string_view& name) const {
        return false;
    }

    bool enter(const std::string_view& name) const {
        return false;
    }

    [[gnu::always_inline]]
    void enable(const std::string_view& injection_name, const bool one_shot = false, error_injection_parameters parameters = {}) {}

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
    std::invoke_result_t<Func> inject(const std::string_view& name, std::chrono::time_point<Clock, Duration> deadline,
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

    // \brief Inject exception
    // \param func function returning a future and taking an injection handler
    [[gnu::always_inline]]
    future<> inject(const std::string_view& name, waiting_handler_fun func, bool share_messages = true) {
        return make_ready_future<>();
    }

    template <typename T>
    [[gnu::always_inline]]
    std::optional<T> inject_parameter(const std::string_view& name) {
        return std::nullopt;
    }

    [[gnu::always_inline]]
    void set_parameter(std::string_view injection_name, sstring parameter_name, sstring parameter_value) { }

    [[gnu::always_inline]]
    error_injection_parameters get_injection_parameters(std::string_view name) {
        return {};
    }

    [[gnu::always_inline]]
    static future<> enable_on_all(const std::string_view& injection_name, const bool one_shot = false, const error_injection_parameters& parameters = {}) {
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
    static future<> receive_message_on_all(const std::string_view& injection_name) {
        return make_ready_future<>();
    }

    [[gnu::always_inline]]
    static void receive_message(const std::string_view& injection_name) {}

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
