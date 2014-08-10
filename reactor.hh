/*
 * reactor.hh
 *
 *  Created on: Aug 1, 2014
 *      Author: avi
 */

#ifndef REACTOR_HH_
#define REACTOR_HH_

#include <memory>
#include <libaio.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unordered_map>
#include <netinet/ip.h>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <iostream>

class socket_address;
class reactor;
class pollable_fd;

struct ipv4_addr {
    uint8_t host[4];
    uint16_t port;
};

socket_address make_ipv4_address(ipv4_addr addr);

class socket_address {
private:
    union {
        ::sockaddr_storage sas;
        ::sockaddr sa;
        ::sockaddr_in in;
    } u;
    friend socket_address make_ipv4_address(ipv4_addr addr);
    friend class reactor;
};

template <class T>
class promise;

template <class T>
class future;

class task {
public:
    virtual ~task() {}
    virtual void run() = 0;
};

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


template <typename T>
struct future_state {
    promise<T>* _promise = nullptr;
    future<T>* _future = nullptr;
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
        T value;
        std::exception_ptr ex;
    } _u;
    ~future_state() noexcept {
        switch (_state) {
        case state::future:
            break;
        case state::result:
            _u.value.~T();
            break;
        case state::exception:
            _u.ex.~exception_ptr();
            break;
        default:
            abort();
        }
    }
    bool has_promise() const { return _promise; }
    bool has_future() const { return _future; }
    void wait();
    void set(const T& value) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) T(value);
        if (_task) {
            _task->run();
        }
    }
    void set(T&& value) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) T(std::move(value));
        if (_task) {
            _task->run();
        }
    }
    template <typename... A>
    void set(A... a) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) T(std::forward(a)...);
        std::cout << "checking task at " << &_task << "\n";
        if (_task) {
            _task->run();
        }
    }
    void set_exception(std::exception_ptr ex) {
        assert(_state == state::future);
        _state = state::exception;
        new (&_u.ex) std::exception(ex);
        if (_task) {
            _task->run();
        }
    }
    T get() {
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
        std::cout << "scheduling task at " << &_task << "\n";
        _task = make_task(std::forward<Func>(func));
    }
};

template <typename T>
class promise {
    future_state<T>* _state;
public:
    promise() : _state(new future_state<T>()) { _state->_promise = this; }
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
    promise& operator=(promise&&);
    void operator=(const T&) = delete;
    future<T> get_future();
    void set_value(const T& result) { _state->set(result); }
    void set_value(T&& result)  { _state->set(std::move(result)); }
};

template <typename T>
class future {
    future_state<T>* _state;
private:
    future(future_state<T>* state) : _state(state) { _state->_future = this; }
public:
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
    T get() {
        return _state->get();
    }
    template <typename Func>
    void then(Func&& func) {
        auto state = _state;
        state->schedule([fut = std::move(*this), func = std::forward<Func>(func)] () mutable {
            std::cout << "running task\n";
            func(std::move(fut));
        });
    }
    friend class promise<T>;
};

template <typename T>
inline
future<T>
promise<T>::get_future()
{
    assert(!_state->_future);
    return future<T>(_state);
}

using accept_result = std::tuple<std::unique_ptr<pollable_fd>, socket_address>;

struct listen_options {
    bool reuse_address = false;
};

class reactor {
public:
    int _epollfd;
    io_context_t _io_context;
private:
    void epoll_add_in(pollable_fd& fd, std::unique_ptr<task> t);
    void epoll_add_out(pollable_fd& fd, std::unique_ptr<task> t);
    void abort_on_error(int ret);
public:
    reactor();
    reactor(const reactor&) = delete;
    void operator=(const reactor&) = delete;
    ~reactor();

    std::unique_ptr<pollable_fd> listen(socket_address sa, listen_options opts = {});

    future<accept_result> accept(pollable_fd& listen_fd);

    future<size_t> read_some(pollable_fd& fd, void* buffer, size_t size);

    void run();

    friend class pollable_fd;
};

class pollable_fd {
protected:
    explicit pollable_fd(int fd) : fd(fd) {}
    pollable_fd(const pollable_fd&) = delete;
    void operator=(const pollable_fd&) = delete;
    int fd;
    int events = 0;
    std::unique_ptr<task> pollin;
    std::unique_ptr<task> pollout;
    friend class reactor;
};

inline
future<accept_result>
reactor::accept(pollable_fd& listenfd) {
    promise<accept_result> pr;
    future<accept_result> fut = pr.get_future();
    epoll_add_in(listenfd, make_task([pr = std::move(pr), lfd = listenfd.fd] () mutable {
        socket_address sa;
        socklen_t sl = sizeof(&sa.u.sas);
        int fd = ::accept4(lfd, &sa.u.sa, &sl, SOCK_NONBLOCK | SOCK_CLOEXEC);
        assert(fd != -1);
        pr.set_value(accept_result{std::unique_ptr<pollable_fd>(new pollable_fd(fd)), sa});
    }));
    return fut;
}

inline
future<size_t>
reactor::read_some(pollable_fd& fd, void* buffer, size_t len) {
    promise<size_t> pr;
    auto fut = pr.get_future();
    epoll_add_in(fd, make_task([pr = std::move(pr), rfd = fd.fd, buffer, len] () mutable {
        ssize_t r = ::recv(rfd, buffer, len, 0);
        assert(r != -1);
        pr.set_value(r);
    }));
    return fut;
}


#endif /* REACTOR_HH_ */
