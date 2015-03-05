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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef FUTURE_HH_
#define FUTURE_HH_

#include "apply.hh"
#include <stdexcept>
#include <memory>
#include <type_traits>
#include <assert.h>

template <class... T>
class promise;

template <class... T>
class future;

template <typename... T, typename... A>
future<T...> make_ready_future(A&&... value);

template <typename... T>
future<T...> make_exception_future(std::exception_ptr value) noexcept;

class task {
public:
    virtual ~task() noexcept {}
    virtual void run() noexcept = 0;
};

void schedule(std::unique_ptr<task> t);
void engine_exit(std::exception_ptr eptr = {});

template <typename Func>
class lambda_task : public task {
    Func _func;
public:
    lambda_task(const Func& func) : _func(func) {}
    lambda_task(Func&& func) : _func(std::move(func)) {}
    virtual void run() noexcept { _func(); }
};

template <typename Func>
std::unique_ptr<task>
make_task(const Func& func) {
    return std::unique_ptr<task>(new lambda_task<Func>(func));
}

template <typename Func>
std::unique_ptr<task>
make_task(Func&& func) {
    return std::unique_ptr<task>(new lambda_task<Func>(std::move(func)));
}

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
    }
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
    std::tuple<T...> get() {
        assert(_state != state::future);
        if (_state == state::exception) {
            std::rethrow_exception(_u.ex);
        }
        return std::move(_u.value);
    }
    void forward_to(promise<T...>& pr) noexcept {
        assert(_state != state::future);
        if (_state == state::exception) {
            pr.set_exception(_u.ex);
        } else {
            pr.set_value(std::move(get()));
        }
    }
};

// Specialize future_state<> to overlap the state enum with the exception, as there
// is no value to hold.
//
// Assumes std::exception_ptr is really a pointer.
template <>
struct future_state<> {
    static_assert(sizeof(std::exception_ptr) == sizeof(void*), "exception_ptr not a pointer");
    static constexpr const bool move_noexcept = true;
    static constexpr const bool copy_noexcept = true;
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
            new (&_u.ex) std::exception_ptr(std::move(x._u.ex));
        }
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
            std::rethrow_exception(_u.ex);
        }
        return {};
    }
    void forward_to(promise<>& pr) noexcept;
};

template <typename Func, typename... T>
struct future_task final : task, future_state<T...> {
    Func _func;
    future_task(Func&& func) : _func(std::move(func)) {}
    virtual void run() noexcept {
        func(*this);
    }
};

template <typename... T>
class promise {
    future<T...>* _future = nullptr;
    future_state<T...> _local_state;
    future_state<T...>* _state;
    std::unique_ptr<task> _task;
    static constexpr const bool move_noexcept = future_state<T...>::move_noexcept;
    static constexpr const bool copy_noexcept = future_state<T...>::copy_noexcept;
public:
    promise() noexcept : _state(&_local_state) {}
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
    future<T...> get_future() noexcept;
    void set_value(const std::tuple<T...>& result) noexcept(copy_noexcept) {
        _state->set(result);
        make_ready();
    }
    void set_value(std::tuple<T...>&& result) noexcept(move_noexcept) {
        _state->set(std::move(result));
        make_ready();
    }
    template <typename... A>
    void set_value(A&&... a) noexcept {
        _state->set(std::forward<A>(a)...);
        make_ready();
    }
    void set_exception(std::exception_ptr ex) noexcept {
        _state->set_exception(std::move(ex));
        make_ready();
    }
    template<typename Exception>
    void set_exception(Exception&& e) noexcept {
        set_exception(make_exception_ptr(std::forward<Exception>(e)));
    }
private:
    template <typename Func>
    void schedule(Func&& func) noexcept {
        struct task_with_state final : task {
            task_with_state(Func&& func) : _func(std::move(func)) {}
            virtual void run() noexcept override {
                _func(_state);
            }
            future_state<T...> _state;
            Func _func;
        };
        auto tws = std::make_unique<task_with_state>(std::move(func));
        _state = &tws->_state;
        _task = std::move(tws);
    }
    void make_ready() noexcept;
    void migrated() noexcept;
    void abandoned() noexcept(move_noexcept);

    template <typename... U>
    friend class future;
};

template<>
class promise<void> : public promise<> {};

template <typename... T> struct is_future : std::false_type {};
template <typename... T> struct is_future<future<T...>> : std::true_type {};

struct ready_future_marker {};
struct ready_future_from_tuple_marker {};
struct exception_future_marker {};

extern __thread size_t future_avail_count;

// Converts a type to a future type, if it isn't already.
//
// Result in member type 'type'.
template <typename T>
struct futurize;

template <typename T>
struct futurize {
    using type = future<T>;
    using promise_type = promise<T>;

    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, std::tuple<FuncArgs...>&& args);

    template<typename Func, typename... FuncArgs>
    static inline type apply(Func&& func, FuncArgs&&... args);
};

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

// Converts a type to a future type, if it isn't already.
template <typename T>
using futurize_t = typename futurize<T>::type;

template <typename... T>
class future {
    promise<T...>* _promise;
    future_state<T...> _local_state;  // valid if !_promise
    static constexpr const bool move_noexcept = future_state<T...>::move_noexcept;
    static constexpr const bool copy_noexcept = future_state<T...>::copy_noexcept;
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
        struct task_with_ready_state final : task {
            task_with_ready_state(Func&& func, future_state<T...>&& state) : _state(std::move(state)), _func(std::move(func)) {}
            virtual void run() noexcept override {
                _func(_state);
            }
            future_state<T...> _state;
            Func _func;
        };
        if (state()->available()) {
            ::schedule(std::make_unique<task_with_ready_state>(std::move(func), std::move(*state())));
        } else {
            _promise->schedule(std::move(func));
            _promise->_future = nullptr;
            _promise = nullptr;
        }
    }
public:
    using value_type = std::tuple<T...>;
    using promise_type = promise<T...>;
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
    ~future() {
        if (_promise) {
            _promise->_future = nullptr;
        }
    }
    std::tuple<T...> get() {
        return state()->get();
    }

    bool available() noexcept {
        return state()->available();
    }

    bool failed() noexcept {
        return state()->failed();
    }

    template <typename Func>
    futurize_t<std::result_of_t<Func(T&&...)>> then(Func&& func) noexcept {
        using futurator = futurize<std::result_of_t<Func(T&&...)>>;
        if (state()->available() && (++future_avail_count % 256)) {
            try {
                return futurator::apply(std::forward<Func>(func), std::move(state()->get()));
            } catch (...) {
                typename futurator::promise_type p;
                p.set_exception(std::current_exception());
                return p.get_future();
            }
        }
        typename futurator::promise_type pr;
        auto fut = pr.get_future();
        schedule([pr = std::move(pr), func = std::forward<Func>(func)] (auto& state) mutable {
            try {
                futurator::apply(std::forward<Func>(func), state.get()).forward_to(std::move(pr));
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        return fut;
    }

    void forward_to(promise<T...>&& pr) noexcept {
        if (state()->available()) {
            state()->forward_to(pr);
        } else {
            _promise->_future = nullptr;
            *_promise = std::move(pr);
            _promise = nullptr;
        }
    }

    template <typename Func>
    futurize_t<std::result_of_t<Func(future<T...>)>>
    then_wrapped(Func&& func) noexcept {
        using futurator = futurize<std::result_of_t<Func(future<T...>)>>;
        using P = typename futurator::promise_type;
        if (state()->available()) {
            try {
                return futurator::apply(std::forward<Func>(func), std::move(*this));
            } catch (...) {
                P pr;
                pr.set_exception(std::current_exception());
                return pr.get_future();
            }
        }
        P pr;
        auto next_fut = pr.get_future();
        _promise->schedule([func = std::forward<Func>(func), pr = std::move(pr)] (auto& state) mutable {
            try {
                futurator::apply(std::forward<Func>(func), future(std::move(state)))
                    .forward_to(std::move(pr));
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        _promise->_future = nullptr;
        _promise = nullptr;
        return next_fut;
    }

    /**
     * Finally continuation for statements that require waiting for the result. I.e. you need to "finally" call
     * a function that returns a possibly unavailable future.
     * The returned future will be "waited for", any exception generated will be propagated, but the return value
     * is ignored. I.e. the original return value (the future upon which you are making this call) will be preserved.
     */
    template <typename Func>
    future<T...> finally(Func&& func, std::enable_if_t<is_future<std::result_of_t<Func()>>::value, void*> = nullptr) noexcept {
        return std::move(*this).then_wrapped([func = std::forward<Func>(func)](future<T...> result) {
            return func().then_wrapped([result = std::move(result)](auto f_res) mutable {
                try {
                    f_res.get(); // force excepion if one
                    return std::move(result);
                } catch (...) {
                    return make_exception_future<T...>(std::current_exception());
                }
            });
        });
    }

    template <typename Func>
    future<T...> finally(Func&& func, std::enable_if_t<!is_future<std::result_of_t<Func()>>::value, void*> = nullptr) noexcept {
        promise<T...> pr;
        auto f = pr.get_future();
        if (state()->available()) {
            try {
                func();
            } catch (...) {
                pr.set_exception(std::current_exception());
                return f;
            }
            state()->forward_to(pr);
            return f;
        }
        _promise->schedule([pr = std::move(pr), func = std::forward<Func>(func)] (auto& state) mutable {
            try {
                func();
            } catch (...) {
                pr.set_exception(std::current_exception());
                return;
            }
            state.forward_to(pr);
        });
        _promise->_future = nullptr;
        _promise = nullptr;
        return f;
    }

    future<> or_terminate() && noexcept {
        return std::move(*this).then_wrapped([] (auto&& f) {
            try {
                f.get();
            } catch (...) {
                engine_exit(std::current_exception());
            }
        });
    }

    future<> discard_result() && noexcept {
        return then([] (T&&...) {});
    }

    template <typename... U>
    friend class promise;
    template <typename... U, typename... A>
    friend future<U...> make_ready_future(A&&... value);
    template <typename... U>
    friend future<U...> make_exception_future(std::exception_ptr ex) noexcept;
    template <typename... U, typename Exception>
    friend future<U...> make_exception_future(Exception&& ex) noexcept;
};

inline
void future_state<>::forward_to(promise<>& pr) noexcept {
    assert(_u.st != state::future && _u.st != state::invalid);
    if (_u.st >= state::exception_min) {
        pr.set_exception(_u.ex);
    } else {
        pr.set_value(std::tuple<>());
    }
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

template <typename... T, typename Exception>
inline
future<T...> make_exception_future(Exception&& ex) noexcept {
    return make_exception_future<T...>(std::make_exception_ptr(std::forward<Exception>(ex)));
}

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

#endif /* FUTURE_HH_ */
