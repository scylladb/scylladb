/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#ifndef FUTURE_HH_
#define FUTURE_HH_

#include "apply.hh"
#include "task.hh"
#include <stdexcept>
#include <memory>
#include <type_traits>
#include <assert.h>


/// \defgroup future-module Futures and Promises
///
/// \brief
/// Futures and promises are the basic tools for asynchronous
/// programming in seastar.  A future represents a result that
/// may not have been computed yet, for example a buffer that
/// is being read from the disk, or the result of a function
/// that is executed on another cpu.  A promise object allows
/// the future to be eventually resolved by assigning it a value.
///
/// \brief
/// Another way to look at futures and promises are as the reader
/// and writer sides, respectively, of a single-item, single use
/// queue.  You read from the future, and write to the promise,
/// and the system takes care that it works no matter what the
/// order of operations is.
///
/// \brief
/// The normal way of working with futures is to chain continuations
/// to them.  A continuation is a block of code (usually a lamdba)
/// that is called when the future is assigned a value (the future
/// is resolved); the continuation can then access the actual value.
///

/// \defgroup future-util Future Utilities
///
/// \brief
/// These utilities are provided to help perform operations on futures.


namespace seastar {

class thread_context;

namespace thread_impl {

thread_context* get();
void switch_in(thread_context* to);
void switch_out(thread_context* from);

}

}


/// \addtogroup future-module
/// @{

template <class... T>
class promise;

template <class... T>
class future;

/// \brief Creates a \ref future in an available, value state.
///
/// Creates a \ref future object that is already resolved.  This
/// is useful when it is determined that no I/O needs to be performed
/// to perform a computation (for example, because the data is cached
/// in some buffer).
template <typename... T, typename... A>
future<T...> make_ready_future(A&&... value);

/// \brief Creates a \ref future in an available, failed state.
///
/// Creates a \ref future object that is already resolved in a failed
/// state.  This is useful when no I/O needs to be performed to perform
/// a computation (for example, because the connection is closed and
/// we cannot read from it).
template <typename... T>
future<T...> make_exception_future(std::exception_ptr value) noexcept;

/// \cond internal
void engine_exit(std::exception_ptr eptr = {});

void report_failed_future(std::exception_ptr ex);
/// \endcond

//
// A future/promise pair maintain one logical value (a future_state).
// To minimize allocations, the value is stored in exactly one of three
// locations:
//
// - in the promise (so long as it exists, and before a .then() is called)
//
// - in the task associated with the .then() clause (after .then() is called,
//   if a value was not set)
//
// - in the future (if the promise was destroyed, or if it never existed, as
//   with make_ready_future()), before .then() is called
//
// Both promise and future maintain a pointer to the state, which is modified
// the the state moves to a new location due to events (such as .then() being
// called) or due to the promise or future being mobved around.
//

/// \cond internal
template <typename... T>
struct future_state {
    static constexpr bool move_noexcept = std::is_nothrow_move_constructible<std::tuple<T...>>::value;
    static constexpr bool copy_noexcept = std::is_nothrow_copy_constructible<std::tuple<T...>>::value;
    enum class state {
         invalid,
         future,
         result,
         exception,
    } _state = state::future;
    union any {
        any() {}
        ~any() {}
        std::tuple<T...> value;
        std::exception_ptr ex;
    } _u;
    future_state() noexcept {}
    future_state(future_state&& x) noexcept(move_noexcept)
            : _state(x._state) {
        switch (_state) {
        case state::future:
            break;
        case state::result:
            new (&_u.value) std::tuple<T...>(std::move(x._u.value));
            break;
        case state::exception:
            new (&_u.ex) std::exception_ptr(std::move(x._u.ex));
            break;
        case state::invalid:
            break;
        default:
            abort();
        }
        x._state = state::invalid;
    }
    __attribute__((always_inline))
    ~future_state() noexcept {
        switch (_state) {
        case state::invalid:
            break;
        case state::future:
            break;
        case state::result:
            _u.value.~tuple();
            break;
        case state::exception:
            _u.ex.~exception_ptr();
            break;
        default:
            abort();
        }
    }
    future_state& operator=(future_state&& x) noexcept(move_noexcept) {
        if (this != &x) {
            this->~future_state();
            new (this) future_state(std::move(x));
        }
        return *this;
    }
    bool available() const noexcept { return _state == state::result || _state == state::exception; }
    bool failed() const noexcept { return _state == state::exception; }
    void wait();
    void set(const std::tuple<T...>& value) noexcept(copy_noexcept) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) std::tuple<T...>(value);
    }
    void set(std::tuple<T...>&& value) noexcept(move_noexcept) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) std::tuple<T...>(std::move(value));
    }
    template <typename... A>
    void set(A&&... a) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) std::tuple<T...>(std::forward<A>(a)...);
    }
    void set_exception(std::exception_ptr ex) noexcept {
        assert(_state == state::future);
        _state = state::exception;
        new (&_u.ex) std::exception_ptr(ex);
    }
    std::exception_ptr get_exception() noexcept {
        assert(_state == state::exception);
        // Move ex out so future::~future() knows we've handled it
        _state = state::invalid;
        return std::move(_u.ex);
    }
    std::tuple<T...> get() {
        assert(_state != state::future);
        if (_state == state::exception) {
            _state = state::invalid;
            // Move ex out so future::~future() knows we've handled it
            std::rethrow_exception(std::move(_u.ex));
        }
        return std::move(_u.value);
    }
    using get0_return_type = std::tuple_element_t<0, std::tuple<T...>>;
    static get0_return_type get0(std::tuple<T...>&& x) {
        return std::get<0>(x);
    }
    void forward_to(promise<T...>& pr) noexcept {
        assert(_state != state::future);
        if (_state == state::exception) {
            pr.set_exception(_u.ex);
        } else {
            pr.set_value(std::move(get()));
        }
        _state = state::invalid;
    }
};

// Specialize future_state<> to overlap the state enum with the exception, as there
// is no value to hold.
//
// Assumes std::exception_ptr is really a pointer.
template <>
struct future_state<> {
    static_assert(sizeof(std::exception_ptr) == sizeof(void*), "exception_ptr not a pointer");
    static constexpr bool move_noexcept = true;
    static constexpr bool copy_noexcept = true;
    enum class state : uintptr_t {
         invalid = 0,
         future = 1,
         result = 2,
         exception_min = 3,  // or anything greater
    };
    union any {
        any() { st = state::future; }
        ~any() {}
        state st;
        std::exception_ptr ex;
    } _u;
    future_state() noexcept {}
    future_state(future_state&& x) noexcept {
        if (x._u.st < state::exception_min) {
            _u.st = x._u.st;
        } else {
            // Move ex out so future::~future() knows we've handled it
            // Moving it will reset us to invalid state
            new (&_u.ex) std::exception_ptr(std::move(x._u.ex));
        }
        x._u.st = state::invalid;
    }
    ~future_state() noexcept {
        if (_u.st >= state::exception_min) {
            _u.ex.~exception_ptr();
        }
    }
    future_state& operator=(future_state&& x) noexcept {
        if (this != &x) {
            this->~future_state();
            new (this) future_state(std::move(x));
        }
        return *this;
    }
    bool available() const noexcept { return _u.st == state::result || _u.st >= state::exception_min; }
    bool failed() const noexcept { return _u.st >= state::exception_min; }
    void set(const std::tuple<>& value) noexcept {
        assert(_u.st == state::future);
        _u.st = state::result;
    }
    void set(std::tuple<>&& value) noexcept {
        assert(_u.st == state::future);
        _u.st = state::result;
    }
    void set() {
        assert(_u.st == state::future);
        _u.st = state::result;
    }
    void set_exception(std::exception_ptr ex) noexcept {
        assert(_u.st == state::future);
        new (&_u.ex) std::exception_ptr(ex);
        assert(_u.st >= state::exception_min);
    }
    std::tuple<> get() {
        assert(_u.st != state::future);
        if (_u.st >= state::exception_min) {
            // Move ex out so future::~future() knows we've handled it
            // Moving it will reset us to invalid state
            std::rethrow_exception(std::move(_u.ex));
        }
        return {};
    }
    using get0_return_type = void;
    static get0_return_type get0(std::tuple<>&&) {
        return;
    }
    std::exception_ptr get_exception() noexcept {
        assert(_u.st >= state::exception_min);
        // Move ex out so future::~future() knows we've handled it
        // Moving it will reset us to invalid state
        return std::move(_u.ex);
    }
    void forward_to(promise<>& pr) noexcept;
};

template <typename Func, typename... T>
struct continuation final : task {
    continuation(Func&& func, future_state<T...>&& state) : _state(std::move(state)), _func(std::move(func)) {}
    continuation(Func&& func) : _func(std::move(func)) {}
    virtual void run() noexcept override {
        _func(std::move(_state));
    }
    future_state<T...> _state;
    Func _func;
};

#ifndef DEBUG
static constexpr unsigned max_inlined_continuations = 256;
#else
static constexpr unsigned max_inlined_continuations = 1;
#endif

/// \endcond

/// \brief promise - allows a future value to be made available at a later time.
///
///
template <typename... T>
class promise {
    future<T...>* _future = nullptr;
    future_state<T...> _local_state;
    future_state<T...>* _state;
    std::unique_ptr<task> _task;
    static constexpr bool move_noexcept = future_state<T...>::move_noexcept;
    static constexpr bool copy_noexcept = future_state<T...>::copy_noexcept;
public:
    /// \brief Constructs an empty \c promise.
    ///
    /// Creates promise with no associated future yet (see get_future()).
    promise() noexcept : _state(&_local_state) {}

    /// \brief Moves a \c promise object.
    promise(promise&& x) noexcept(move_noexcept) : _future(x._future), _state(x._state), _task(std::move(x._task)) {
        if (_state == &x._local_state) {
            _state = &_local_state;
            _local_state = std::move(x._local_state);
        }
        x._future = nullptr;
        x._state = nullptr;
        migrated();
    }
    promise(const promise&) = delete;
    __attribute__((always_inline))
    ~promise() noexcept {
        abandoned();
    }
    promise& operator=(promise&& x) noexcept(move_noexcept) {
        if (this != &x) {
            this->~promise();
            new (this) promise(std::move(x));
        }
        return *this;
    }
    void operator=(const promise&) = delete;

    /// \brief Gets the promise's associated future.
    ///
    /// The future and promise will be remember each other, even if either or
    /// both are moved.  When \c set_value() or \c set_exception() are called
    /// on the promise, the future will be become ready, and if a continuation
    /// was attached to the future, it will run.
    future<T...> get_future() noexcept;

    /// \brief Sets the promise's value (as tuple; by copying)
    ///
    /// Copies the tuple argument and makes it available to the associated
    /// future.  May be called either before or after \c get_future().
    void set_value(const std::tuple<T...>& result) noexcept(copy_noexcept) {
        _state->set(result);
        make_ready();
    }

    /// \brief Sets the promises value (as tuple; by moving)
    ///
    /// Moves the tuple argument and makes it available to the associated
    /// future.  May be called either before or after \c get_future().
    void set_value(std::tuple<T...>&& result) noexcept(move_noexcept) {
        _state->set(std::move(result));
        make_ready();
    }

    /// \brief Sets the promises value (variadic)
    ///
    /// Forwards the arguments and makes them available to the associated
    /// future.  May be called either before or after \c get_future().
    template <typename... A>
    void set_value(A&&... a) noexcept {
        _state->set(std::forward<A>(a)...);
        make_ready();
    }

    /// \brief Marks the promise as failed
    ///
    /// Forwards the exception argument to the future and makes it
    /// available.  May be called either before or after \c get_future().
    void set_exception(std::exception_ptr ex) noexcept {
        _state->set_exception(std::move(ex));
        make_ready();
    }

    /// \brief Marks the promise as failed
    ///
    /// Forwards the exception argument to the future and makes it
    /// available.  May be called either before or after \c get_future().
    template<typename Exception>
    void set_exception(Exception&& e) noexcept {
        set_exception(make_exception_ptr(std::forward<Exception>(e)));
    }
private:
    template <typename Func>
    void schedule(Func&& func) noexcept {
        auto tws = std::make_unique<continuation<Func, T...>>(std::move(func));
        _state = &tws->_state;
        _task = std::move(tws);
    }
    __attribute__((always_inline))
    void make_ready() noexcept;
    void migrated() noexcept;
    void abandoned() noexcept(move_noexcept);

    template <typename... U>
    friend class future;
};

/// \brief Specialization of \c promise<void>
///
/// This is an alias for \c promise<>, for generic programming purposes.
/// For example, You may have a \c promise<T> where \c T can legally be
/// \c void.
template<>
class promise<void> : public promise<> {};

/// @}

/// \addtogroup future-util
/// @{


/// \brief Check whether a type is a future
///
/// This is a type trait evaluating to \c true if the given type is a
/// future.
///
template <typename... T> struct is_future : std::false_type {};

/// \cond internal
/// \addtogroup future-util
template <typename... T> struct is_future<future<T...>> : std::true_type {};

struct ready_future_marker {};
struct ready_future_from_tuple_marker {};
struct exception_future_marker {};

extern __thread size_t future_avail_count;
/// \endcond


/// \brief Converts a type to a future type, if it isn't already.
///
/// \return Result in member type 'type'.
template <typename T>
struct futurize;

template <typename T>
struct futurize {
    /// If \c T is a future, \c T; otherwise \c future<T>
    using type = future<T>;
    /// The promise type associated with \c type.
    using promise_type = promise<T>;

    /// Apply a function to an argument list (expressed as a tuple)
    /// and return the result, as a future (if it wasn't already).
    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, std::tuple<FuncArgs...>&& args);

    /// Apply a function to an argument list
    /// and return the result, as a future (if it wasn't already).
    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, FuncArgs&&... args);
};

/// \cond internal
template <>
struct futurize<void> {
    using type = future<>;
    using promise_type = promise<>;

    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, std::tuple<FuncArgs...>&& args);

    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, FuncArgs&&... args);
};

template <typename... Args>
struct futurize<future<Args...>> {
    using type = future<Args...>;
    using promise_type = promise<Args...>;

    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, std::tuple<FuncArgs...>&& args);

    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, FuncArgs&&... args);
};
/// \endcond

// Converts a type to a future type, if it isn't already.
template <typename T>
using futurize_t = typename futurize<T>::type;

/// @}

/// \addtogroup future-module
/// @{

/// \brief A representation of a possibly not-yet-computed value.
///
/// A \c future represents a value that has not yet been computed
/// (an asynchronous computation).  It can be in one of several
/// states:
///    - unavailable: the computation has not been completed yet
///    - value: the computation has been completed successfully and a
///      value is available.
///    - failed: the computation completed with an exception.
///
/// methods in \c future allow querying the state and, most importantly,
/// scheduling a \c continuation to be executed when the future becomes
/// available.  Only one such continuation may be scheduled.
template <typename... T>
class future {
    promise<T...>* _promise;
    future_state<T...> _local_state;  // valid if !_promise
    static constexpr bool move_noexcept = future_state<T...>::move_noexcept;
    static constexpr bool copy_noexcept = future_state<T...>::copy_noexcept;
private:
    future(promise<T...>* pr) noexcept : _promise(pr) {
        _promise->_future = this;
    }
    template <typename... A>
    future(ready_future_marker, A&&... a) : _promise(nullptr) {
        _local_state.set(std::forward<A>(a)...);
    }
    template <typename... A>
    future(ready_future_from_tuple_marker, std::tuple<A...>&& data) : _promise(nullptr) {
        _local_state.set(std::move(data));
    }
    future(exception_future_marker, std::exception_ptr ex) noexcept : _promise(nullptr) {
        _local_state.set_exception(std::move(ex));
    }
    explicit future(future_state<T...>&& state) noexcept
            : _promise(nullptr), _local_state(std::move(state)) {
    }
    future_state<T...>* state() noexcept {
        return _promise ? _promise->_state : &_local_state;
    }
    template <typename Func>
    void schedule(Func&& func) noexcept {
        if (state()->available()) {
            ::schedule(std::make_unique<continuation<Func, T...>>(std::move(func), std::move(*state())));
        } else {
            _promise->schedule(std::move(func));
            _promise->_future = nullptr;
            _promise = nullptr;
        }
    }

    future_state<T...> get_available_state() {
        auto st = state();
        if (_promise) {
            _promise->_future = nullptr;
            _promise = nullptr;
        }
        return std::move(*st);
    }

    template <typename Ret, typename Func, typename Param>
    futurize_t<Ret> then(Func&& func, Param&& param) noexcept {
        using futurator = futurize<Ret>;
        using P = typename futurator::promise_type;
        if (state()->available() && (++future_avail_count % max_inlined_continuations)) {
            try {
                return futurator::apply(std::forward<Func>(func), param(get_available_state()));
            } catch (...) {
                P p;
                p.set_exception(std::current_exception());
                return p.get_future();
            }
        }
        P pr;
        auto fut = pr.get_future();
        schedule([pr = std::move(pr), func = std::forward<Func>(func), param = std::forward<Param>(param)] (auto&& state) mutable {
            try {
                futurator::apply(std::forward<Func>(func), param(std::move(state))).forward_to(std::move(pr));
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        return fut;
    }

public:
    /// \brief The data type carried by the future.
    using value_type = std::tuple<T...>;
    /// \brief The data type carried by the future.
    using promise_type = promise<T...>;
    /// \brief Moves the future into a new object.
    future(future&& x) noexcept(move_noexcept) : _promise(x._promise) {
        if (!_promise) {
            _local_state = std::move(x._local_state);
        }
        x._promise = nullptr;
        if (_promise) {
            _promise->_future = this;
        }
    }
    future(const future&) = delete;
    future& operator=(future&& x) noexcept {
        if (this != &x) {
            this->~future();
            new (this) future(std::move(x));
        }
        return *this;
    }
    void operator=(const future&) = delete;
    __attribute__((always_inline))
    ~future() {
        if (_promise) {
            _promise->_future = nullptr;
        }
        if (failed()) {
            report_failed_future(state()->get_exception());
        }
    }
    /// \brief gets the value returned by the computation
    ///
    /// Requires that the future be available.  If the value
    /// was computed successfully, it is returned (as an
    /// \c std::tuple).  Otherwise, an exception is thrown.
    ///
    /// If get() is called in a \ref seastar::thread context,
    /// then it need not be available; instead, the thread will
    /// be paused until the future becomes available.
    std::tuple<T...> get() {
        if (!state()->available()) {
            wait();
        }
        // detach from promise, so that promise::abandoned() doesn't trigger
        if (_promise) {
            _promise->_future = nullptr;
            _promise = nullptr;
        }
        return state()->get();
    }

    /// Gets the value returned by the computation.
    ///
    /// Similar to \ref get(), but instead of returning a
    /// tuple, returns the first value of the tuple.  This is
    /// useful for the common case of a \c future<T> with exactly
    /// one type parameter.
    ///
    /// Equivalent to: \c std::get<0>(f.get()).
    typename future_state<T...>::get0_return_type get0() {
        return future_state<T...>::get0(get());
    }

    /// \cond internal
    void wait() {
        auto thread = seastar::thread_impl::get();
        assert(thread);
        schedule([this, thread] (future_state<T...>&& new_state) {
            *state() = std::move(new_state);
            seastar::thread_impl::switch_in(thread);
        });
        seastar::thread_impl::switch_out(thread);
    }
    /// \endcond

    /// \brief Checks whether the future is available.
    ///
    /// \return \c true if the future has a value, or has failed.
    bool available() noexcept {
        return state()->available();
    }

    /// \brief Checks whether the future has failed.
    ///
    /// \return \c true if the future is availble and has failed.
    bool failed() noexcept {
        return state()->failed();
    }

    /// \brief Schedule a block of code to run when the future is ready.
    ///
    /// Schedules a function (often a lambda) to run when the future becomes
    /// available.  The function is called with the result of this future's
    /// computation as parameters.  The return value of the function becomes
    /// the return value of then(), itself as a future; this allows then()
    /// calls to be chained.
    ///
    /// If the future failed, the function is not called, and the exception
    /// is propagated into the return value of then().
    ///
    /// \param func - function to be called when the future becomes available,
    ///               unless it has failed.
    /// \return a \c future representing the return value of \c func, applied
    ///         to the eventual value of this future.
    template <typename Func>
    futurize_t<std::result_of_t<Func(T&&...)>> then(Func&& func) noexcept {
        return then<std::result_of_t<Func(T&&...)>>(std::forward<Func>(func), [] (future_state<T...>&& state) { return state.get(); });
    }

    /// \brief Schedule a block of code to run when the future is ready, allowing
    ///        for exception handling.
    ///
    /// Schedules a function (often a lambda) to run when the future becomes
    /// available.  The function is called with the this future as a parameter;
    /// it will be in an available state.  The return value of the function becomes
    /// the return value of then_wrapped(), itself as a future; this allows
    /// then_wrapped() calls to be chained.
    ///
    /// Unlike then(), the function will be called for both value and exceptional
    /// futures.
    ///
    /// \param func - function to be called when the future becomes available,
    /// \return a \c future representing the return value of \c func, applied
    ///         to the eventual value of this future.
    template <typename Func>
    futurize_t<std::result_of_t<Func(future<T...>)>>
    then_wrapped(Func&& func) noexcept {
        return then<std::result_of_t<Func(future<T...>)>>(std::forward<Func>(func), [] (future_state<T...>&& state) { return future(std::move(state)); });
    }

    /// \brief Satisfy some \ref promise object with this future as a result.
    ///
    /// Arranges so that when this future is resolve, it will be used to
    /// satisfy an unrelated promise.  This is similar to scheduling a
    /// continuation that moves the result of this future into the promise
    /// (using promise::set_value() or promise::set_exception(), except
    /// that it is more efficient.
    ///
    /// \param pr a promise that will be fulfilled with the results of this
    /// future.
    void forward_to(promise<T...>&& pr) noexcept {
        if (state()->available()) {
            state()->forward_to(pr);
        } else {
            _promise->_future = nullptr;
            *_promise = std::move(pr);
            _promise = nullptr;
        }
    }

    /**
     * Finally continuation for statements that require waiting for the result. I.e. you need to "finally" call
     * a function that returns a possibly unavailable future.
     * The returned future will be "waited for", any exception generated will be propagated, but the return value
     * is ignored. I.e. the original return value (the future upon which you are making this call) will be preserved.
     */
    template <typename Func>
    future<T...> finally(Func&& func) noexcept {
        return then_wrapped([func = std::forward<Func>(func)](future<T...> result) mutable {
            using futurator = futurize<std::result_of_t<Func()>>;
            return futurator::apply(std::forward<Func>(func)).then_wrapped([result = std::move(result)](auto f_res) mutable {
                try {
                    f_res.get(); // force excepion if one
                    return std::move(result);
                } catch (...) {
                    return make_exception_future<T...>(std::current_exception());
                }
            });
        });
    }

    /// \brief Terminate the program if this future fails.
    ///
    /// Terminates the entire program is this future resolves
    /// to an exception.  Use with caution.
    future<> or_terminate() noexcept {
        return then_wrapped([] (auto&& f) {
            try {
                f.get();
            } catch (...) {
                engine_exit(std::current_exception());
            }
        });
    }

    /// \brief Discards the value carried by this future.
    ///
    /// Converts the future into a no-value \c future<>, by
    /// ignorting any result.  Exceptions are propagated unchanged.
    future<> discard_result() noexcept {
        return then([] (T&&...) {});
    }

    /// \cond internal
    template <typename... U>
    friend class promise;
    template <typename... U, typename... A>
    friend future<U...> make_ready_future(A&&... value);
    template <typename... U>
    friend future<U...> make_exception_future(std::exception_ptr ex) noexcept;
    template <typename... U, typename Exception>
    friend future<U...> make_exception_future(Exception&& ex) noexcept;
    /// \endcond
};

inline
void future_state<>::forward_to(promise<>& pr) noexcept {
    assert(_u.st != state::future && _u.st != state::invalid);
    if (_u.st >= state::exception_min) {
        pr.set_exception(std::move(_u.ex));
    } else {
        pr.set_value(std::tuple<>());
    }
    _u.st = state::invalid;
}

template <typename... T>
inline
future<T...>
promise<T...>::get_future() noexcept {
    assert(!_future);
    return future<T...>(this);
}

template <typename... T>
inline
void promise<T...>::make_ready() noexcept {
    if (_task) {
        ::schedule(std::move(_task));
    }
}

template <typename... T>
inline
void promise<T...>::migrated() noexcept {
    if (_future) {
        _future->_promise = this;
    }
}

template <typename... T>
inline
void promise<T...>::abandoned() noexcept(move_noexcept) {
    if (_future) {
        assert(_state);
        assert(_state->available());
        _future->_local_state = std::move(*_state);
        _future->_promise = nullptr;
    }
}

template <typename... T, typename... A>
inline
future<T...> make_ready_future(A&&... value) {
    return future<T...>(ready_future_marker(), std::forward<A>(value)...);
}

template <typename... T>
inline
future<T...> make_exception_future(std::exception_ptr ex) noexcept {
    return future<T...>(exception_future_marker(), std::move(ex));
}

/// \brief Creates a \ref future in an available, failed state.
///
/// Creates a \ref future object that is already resolved in a failed
/// state.  This no I/O needs to be performed to perform a computation
/// (for example, because the connection is closed and we cannot read
/// from it).
template <typename... T, typename Exception>
inline
future<T...> make_exception_future(Exception&& ex) noexcept {
    return make_exception_future<T...>(std::make_exception_ptr(std::forward<Exception>(ex)));
}

/// @}

/// \cond internal

template<typename T>
template<typename Func, typename... FuncArgs>
typename futurize<T>::type futurize<T>::apply(Func&& func, std::tuple<FuncArgs...>&& args) {
    return make_ready_future<T>(::apply(std::forward<Func>(func), std::move(args)));
}

template<typename T>
template<typename Func, typename... FuncArgs>
typename futurize<T>::type futurize<T>::apply(Func&& func, FuncArgs&&... args) {
    return make_ready_future<T>(func(std::forward<FuncArgs>(args)...));
}

template<typename Func, typename... FuncArgs>
typename futurize<void>::type futurize<void>::apply(Func&& func, std::tuple<FuncArgs...>&& args) {
    ::apply(std::forward<Func>(func), std::move(args));
    return make_ready_future<>();
}

template<typename Func, typename... FuncArgs>
typename futurize<void>::type futurize<void>::apply(Func&& func, FuncArgs&&... args) {
    func(std::forward<FuncArgs>(args)...);
    return make_ready_future<>();
}

template<typename... Args>
template<typename Func, typename... FuncArgs>
typename futurize<future<Args...>>::type futurize<future<Args...>>::apply(Func&& func, std::tuple<FuncArgs...>&& args) {
    return ::apply(std::forward<Func>(func), std::move(args));
}

template<typename... Args>
template<typename Func, typename... FuncArgs>
typename futurize<future<Args...>>::type futurize<future<Args...>>::apply(Func&& func, FuncArgs&&... args) {
    return func(std::forward<FuncArgs>(args)...);
}

/// \endcond

#endif /* FUTURE_HH_ */
