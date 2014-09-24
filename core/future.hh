/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef FUTURE_HH_
#define FUTURE_HH_

#include "apply.hh"
#include <stdexcept>
#include <memory>
#include <assert.h>

template <class... T>
class promise;

template <class... T>
class future;

template <typename... T, typename... A>
future<T...> make_ready_future(A&&... value);

template <typename... T>
future<T...> make_exception_future(std::exception_ptr value);

class task {
public:
    virtual ~task() {}
    virtual void run() = 0;
};

void schedule(std::unique_ptr<task> t);

template <typename Func>
class lambda_task : public task {
    Func _func;
public:
    lambda_task(const Func& func) : _func(func) {}
    lambda_task(Func&& func) : _func(std::move(func)) {}
    virtual void run() { _func(); }
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
    future_state() = default;
    future_state(future_state&& x) : _state(x._state) {
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
    future_state& operator=(future_state&& x) {
        if (this != &x) {
            this->~future_state();
            new (this) future_state(std::move(x));
        }
        return *this;
    }
    bool available() const { return _state == state::result || _state == state::exception; }
    bool failed() const { return _state == state::exception; }
    void wait();
    void set(const std::tuple<T...>& value) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) std::tuple<T...>(value);
    }
    void set(std::tuple<T...>&& value) {
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
    void set_exception(std::exception_ptr ex) {
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
    void forward_to(promise<T...>& pr) {
        assert(_state != state::future);
        if (_state == state::exception) {
            pr.set_exception(_u.ex);
        } else {
            pr.set_value(std::move(get()));
        }
    }
};

template <typename Func, typename... T>
struct future_task final : task, future_state<T...> {
    Func _func;
    future_task(Func&& func) : _func(std::move(func)) {}
    virtual void run() {
        func(*this);
    }
};

template <typename... T>
class promise {
    future<T...>* _future = nullptr;
    future_state<T...> _local_state;
    future_state<T...>* _state;
    std::unique_ptr<task> _task;
public:
    promise() : _state(&_local_state) {}
    promise(promise&& x) : _future(x._future), _state(x._state), _task(std::move(x._task)) {
        if (_state == &x._local_state) {
            _state = &_local_state;
            _local_state = std::move(x._local_state);
        }
        x._future = nullptr;
        x._state = nullptr;
        migrated();
    }
    promise(const promise&) = delete;
    ~promise() {
        abandoned();
    }
    promise& operator=(promise&& x) {
        if (this != &x) {
            this->~promise();
            new (this) promise(std::move(x));
        }
        return *this;
    }
    void operator=(const promise&) = delete;
    future<T...> get_future();
    void set_value(const std::tuple<T...>& result) {
        _state->set(result);
        make_ready();
    }
    void set_value(std::tuple<T...>&& result) {
        _state->set(std::move(result));
        make_ready();
    }
    template <typename... A>
    void set_value(A&&... a) {
        _state->set(std::forward<A>(a)...);
        make_ready();
    }
    void set_exception(std::exception_ptr ex) {
        _state->set_exception(std::move(ex));
        make_ready();
    }
    template<typename Exception>
    void set_exception(Exception&& e) {
        set_exception(make_exception_ptr(std::forward<Exception>(e)));
    }
private:
    template <typename Func>
    void schedule(Func&& func) {
        struct task_with_state final : task {
            task_with_state(Func&& func) : _func(std::move(func)) {}
            virtual void run() override {
                _func(_state);
            }
            future_state<T...> _state;
            Func _func;
        };
        auto tws = std::make_unique<task_with_state>(std::move(func));
        _state = &tws->_state;
        _task = std::move(tws);
    }
    void make_ready();
    void migrated();
    void abandoned();

    template <typename... U>
    friend class future;
};

template <typename... T> struct is_future : std::false_type {};
template <typename... T> struct is_future<future<T...>> : std::true_type {};

struct ready_future_marker {};
struct exception_future_marker {};

template <typename... T>
class future {
    promise<T...>* _promise;
    future_state<T...> _local_state;  // valid if !_promise
private:
    future(promise<T...>* pr) : _promise(pr) {
        _promise->_future = this;
    }
    template <typename... A>
    future(ready_future_marker, A&&... a) : _promise(nullptr) {
        _local_state.set(std::forward<A>(a)...);
    }
    future(exception_future_marker, std::exception_ptr ex) : _promise(nullptr) {
        _local_state.set_exception(std::move(ex));
    }
    future_state<T...>* state() {
        return _promise ? _promise->_state : &_local_state;
    }
public:
    using value_type = std::tuple<T...>;
    using promise_type = promise<T...>;
    future(future&& x) : _promise(x._promise) {
        if (!_promise) {
            _local_state = std::move(x._local_state);
        }
        x._promise = nullptr;
        if (_promise) {
            _promise->_future = this;
        }
    }
    future(const future&) = delete;
    future& operator=(future&& x) {
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

    bool available() {
        return state()->available();
    }

    bool failed() {
        return state()->failed();
    }

    template <typename Func, typename Enable>
    void then(Func, Enable);

    template <typename Func>
    future<> then(Func&& func,
            std::enable_if_t<std::is_same<std::result_of_t<Func(T&&...)>, void>::value, void*> = nullptr) {
        if (state()->available()) {
            try {
                apply(std::move(func), std::move(state()->get()));
                return make_ready_future<>();
            } catch (...) {
                return make_exception_future(std::current_exception());
            }
        }
        promise<> pr;
        auto fut = pr.get_future();
        _promise->schedule([pr = std::move(pr), func = std::forward<Func>(func)] (auto& state) mutable {
            try {
                apply(std::move(func), state.get());
                pr.set_value();
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        _promise->_future = nullptr;
        _promise = nullptr;
        return fut;
    }

    void forward_to(promise<T...>&& pr) {
        if (state()->available()) {
            state()->forward_to(pr);
        } else {
            _promise->schedule([pr = std::move(pr)] (future_state<T...>& state) mutable {
                state.forward_to(pr);
            });
            _promise->_future = nullptr;
            _promise = nullptr;
        }
    }

    template <typename Func>
    std::result_of_t<Func(T&&...)>
    then(Func&& func,
            std::enable_if_t<is_future<std::result_of_t<Func(T&&...)>>::value, void*> = nullptr) {
        using P = typename std::result_of_t<Func(T&&...)>::promise_type;
        if (state()->available()) {
            try {
                return apply(std::move(func), std::move(state()->get()));
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
                auto result = state.get();
                auto next_fut = apply(std::move(func), std::move(result));
                next_fut.forward_to(std::move(pr));
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        _promise->_future = nullptr;
        _promise = nullptr;
        return next_fut;
    }

    template <typename Func>
    future<> rescue(Func&& func) {
        if (state()->available()) {
            try {
                func([&state = *state()] { return state.get(); });
                return make_ready_future();
            } catch (...) {
                return make_exception_future(std::current_exception());
            }
        }
        promise<> pr;
        auto f = pr.get_future();
        _promise->schedule([pr = std::move(pr), func = std::forward<Func>(func)] (auto& state) mutable {
            try {
                func([&state]() mutable { return state.get(); });
                pr.set_value();
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        _promise->_future = nullptr;
        _promise = nullptr;
        return f;
    }

    template <typename Func>
    future<T...> finally(Func&& func) {
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

    template <typename... U>
    friend class promise;
    template <typename... U, typename... A>
    friend future<U...> make_ready_future(A&&... value);
    template <typename... U>
    friend future<U...> make_exception_future(std::exception_ptr ex);
    template <typename... U, typename Exception>
    friend future<U...> make_exception_future(Exception&& ex);
};

template <typename... T>
inline
future<T...>
promise<T...>::get_future()
{
    assert(!_future);
    return future<T...>(this);
}

template <typename... T>
inline
void promise<T...>::make_ready() {
    if (_task) {
        ::schedule(std::move(_task));
    }
}

template <typename... T>
inline
void promise<T...>::migrated() {
    if (_future) {
        _future->_promise = this;
    }
}

template <typename... T>
inline
void promise<T...>::abandoned() {
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
future<T...> make_exception_future(std::exception_ptr ex) {
    return future<T...>(exception_future_marker(), std::move(ex));
}

template <typename... T, typename Exception>
inline
future<T...> make_exception_future(Exception&& ex) {
    return make_exception_future<T...>(make_exception_ptr(std::forward<Exception>(ex)));
}

#endif /* FUTURE_HH_ */
