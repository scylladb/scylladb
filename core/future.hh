/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef FUTURE_HH_
#define FUTURE_HH_

#include "apply.hh"
#include <stdexcept>
#include <memory>

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


template <typename... T>
struct future_state {
    promise<T...>* _promise = nullptr;
    future<T...>* _future = nullptr;
    std::unique_ptr<task> _task;
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
    ~future_state() noexcept {
        switch (_state) {
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
    bool available() const { return _state == state::result || _state == state::exception; }
    bool has_promise() const { return _promise; }
    bool has_future() const { return _future; }
    void wait();
    void make_ready();
    void set(const std::tuple<T...>& value) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) std::tuple<T...>(value);
        make_ready();
    }
    void set(std::tuple<T...>&& value) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) std::tuple<T...>(std::move(value));
        make_ready();
    }
    template <typename... A>
    void set(A&&... a) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) std::tuple<T...>(std::forward<A>(a)...);
        make_ready();
    }
    void set_exception(std::exception_ptr ex) {
        assert(_state == state::future);
        _state = state::exception;
        new (&_u.ex) std::exception_ptr(ex);
        make_ready();
    }
    std::tuple<T...> get() {
        while (_state == state::future) {
            abort();
        }
        if (_state == state::exception) {
            std::rethrow_exception(_u.ex);
        }
        return std::move(_u.value);
    }
    template <typename Func>
    void schedule(Func&& func) {
        _task = make_task(std::forward<Func>(func));
        if (available()) {
            make_ready();
        }
    }
    friend future<T...> make_ready_future<T...>(T&&... value);
};

template <typename... T>
class promise {
    future_state<T...>* _state;
public:
    promise() : _state(new future_state<T...>()) { _state->_promise = this; }
    promise(promise&& x) : _state(std::move(x._state)) { x._state = nullptr; }
    promise(const promise&) = delete;
    ~promise() {
        if (_state) {
            _state->_promise = nullptr;
            if (!_state->has_future()) {
                delete _state;
            }
        }
    }
    promise& operator=(promise&& x) {
        this->~promise();
        _state = x._state;
        x._state = nullptr;
        return *this;
    }
    void operator=(const promise&) = delete;
    future<T...> get_future();
    void set_value(const std::tuple<T...>& result) { _state->set(result); }
    void set_value(std::tuple<T...>&& result)  { _state->set(std::move(result)); }
    template <typename... A>
    void set_value(A&&... a) { _state->set(std::forward<A>(a)...); }
    void set_exception(std::exception_ptr ex) { _state->set_exception(std::move(ex)); }
};

template <typename... T> struct is_future : std::false_type {};
template <typename... T> struct is_future<future<T...>> : std::true_type {};

template <typename... T>
class future {
    future_state<T...>* _state;
private:
    future(future_state<T...>* state) : _state(state) { _state->_future = this; }
public:
    using value_type = std::tuple<T...>;
    using promise_type = promise<T...>;
    future(future&& x) : _state(x._state) { x._state = nullptr; }
    future(const future&) = delete;
    future& operator=(future&& x);
    void operator=(const future&) = delete;
    ~future() {
        if (_state) {
            _state->_future = nullptr;
            if (!_state->has_promise()) {
                delete _state;
            }
        }
    }
    std::tuple<T...> get() {
        return _state->get();
    }

    template <typename Func, typename Enable>
    void then(Func, Enable);

    template <typename Func>
    future<> then(Func&& func,
            std::enable_if_t<std::is_same<std::result_of_t<Func(T&&...)>, void>::value, void*> = nullptr) {
        auto state = _state;
        if (state->available()) {
            try {
                apply(std::move(func), std::move(_state->get()));
                return make_ready_future<>();
            } catch (...) {
                return make_exception_future(std::current_exception());
            }
        }
        promise<> pr;
        auto fut = pr.get_future();
        state->schedule([fut = std::move(*this), pr = std::move(pr), func = std::forward<Func>(func)] () mutable {
            try {
                apply(std::move(func), fut.get());
                pr.set_value();
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        return fut;
    }

    template <typename Func>
    std::result_of_t<Func(T&&...)>
    then(Func&& func,
            std::enable_if_t<is_future<std::result_of_t<Func(T&&...)>>::value, void*> = nullptr) {
        auto state = _state;
        using P = typename std::result_of_t<Func(T&&...)>::promise_type;
        if (state->available()) {
            try {
                return apply(std::move(func), std::move(_state->get()));
            } catch (...) {
                P pr;
                pr.set_exception(std::current_exception());
                return pr.get_future();
            }
        }
        P pr;
        auto next_fut = pr.get_future();
        state->schedule([fut = std::move(*this), func = std::forward<Func>(func), pr = std::move(pr)] () mutable {
            try {
                auto result = fut.get();
                auto next_fut = apply(std::move(func), std::move(result));
                next_fut.then([pr = std::move(pr)] (auto... next) mutable {
                    pr.set_value(std::move(next)...);
                });
            } catch (...) {
                pr.set_exception(std::current_exception());
            }
        });
        return next_fut;
    }

    template <typename Func>
    void rescue(Func&& func) {
        auto state = _state;
        state->schedule([fut = std::move(*this), func = std::forward<Func>(func)] () mutable {
            func([fut = std::move(fut)] () mutable { fut.get(); });
        });
    }

    template <typename... A>
    static future do_make_ready_future(A&&... value) {
        auto s = std::make_unique<future_state<T...>>();
        s->set(std::forward<A>(value)...);
        return future(s.release());
    }

    static future do_make_exception_future(std::exception_ptr ex) {
        auto s = std::make_unique<future_state<T...>>();
        s->set_exception(std::move(ex));
        return future(s.release());
    }

    friend class promise<T...>;
};

template <typename... T>
inline
future<T...>
promise<T...>::get_future()
{
    assert(!_state->_future);
    return future<T...>(_state);
}

template <typename... T>
inline
void future_state<T...>::make_ready() {
    if (_task) {
        ::schedule(std::move(_task));
    }
}

template <typename... T, typename... A>
inline
future<T...> make_ready_future(A&&... value) {
    return future<T...>::do_make_ready_future(std::forward<A>(value)...);
}

template <typename... T>
inline
future<T...> make_exception_future(std::exception_ptr ex) {
    return future<T...>::do_make_exception_future(std::move(ex));
}

#endif /* FUTURE_HH_ */
