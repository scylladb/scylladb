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
#include <unistd.h>
#include <vector>
#include <algorithm>

class socket_address;
class reactor;
class pollable_fd;
class pollable_fd_state;

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
    bool available() const { return _state == state::result || _state == state::exception; }
    bool has_promise() const { return _promise; }
    bool has_future() const { return _future; }
    void wait();
    void make_ready();
    void set(const T& value) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) T(value);
        make_ready();
    }
    void set(T&& value) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) T(std::move(value));
        make_ready();
    }
    template <typename... A>
    void set(A... a) {
        assert(_state == state::future);
        _state = state::result;
        new (&_u.value) T(std::forward(a)...);
        make_ready();
    }
    void set_exception(std::exception_ptr ex) {
        assert(_state == state::future);
        _state = state::exception;
        new (&_u.ex) std::exception_ptr(ex);
        make_ready();
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
        _task = make_task(std::forward<Func>(func));
        if (available()) {
            make_ready();
        }
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
    void operator=(const promise&) = delete;
    future<T> get_future();
    void set_value(const T& result) { _state->set(result); }
    void set_value(T&& result)  { _state->set(std::move(result)); }
};

template <typename T> struct is_future : std::false_type {};
template <typename T> struct is_future<future<T>> : std::true_type {};

template <typename T>
class future {
    future_state<T>* _state;
private:
    future(future_state<T>* state) : _state(state) { _state->_future = this; }
public:
    using value_type = T;
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

    template <typename Func, typename Enable>
    void then(Func, Enable);

    template <typename Func>
    void then(Func&& func,
            std::enable_if_t<std::is_same<std::result_of_t<Func(T&&)>, void>::value, void*> = nullptr) {
        auto state = _state;
        if (state->available()) {
            return func(std::move(_state->get()));
        }
        state->schedule([fut = std::move(*this), func = std::forward<Func>(func)] () mutable {
            func(fut.get());
        });
    }

    template <typename Func>
    std::result_of_t<Func(T&&)>
    then(Func&& func,
            std::enable_if_t<is_future<std::result_of_t<Func(T&&)>>::value, void*> = nullptr) {
        auto state = _state;
        if (state->available()) {
            return func(std::move(_state->get()));
        }
        using U = typename std::result_of_t<Func(T&&)>::value_type;
        promise<U> pr;
        auto next_fut = pr.get_future();
        state->schedule([fut = std::move(*this), func = std::forward<Func>(func), pr = std::move(pr)] () mutable {
            func(fut.get()).then([pr = std::move(pr)] (U next) mutable {
                pr.set_value(std::move(next));
            });
        });
        return next_fut;
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

using accept_result = std::tuple<pollable_fd, socket_address>;

struct listen_options {
    bool reuse_address = false;
};

class reactor {
public:
    int _epollfd;
    io_context_t _io_context;
    std::vector<std::unique_ptr<task>> _pending_tasks;
private:
    void epoll_add_in(pollable_fd_state& fd, std::unique_ptr<task> t);
    void epoll_add_out(pollable_fd_state& fd, std::unique_ptr<task> t);
    void forget(pollable_fd_state& fd);
    void abort_on_error(int ret);
public:
    reactor();
    reactor(const reactor&) = delete;
    void operator=(const reactor&) = delete;
    ~reactor();

    pollable_fd listen(socket_address sa, listen_options opts = {});

    future<accept_result> accept(pollable_fd_state& listen_fd);

    future<size_t> read_some(pollable_fd_state& fd, void* buffer, size_t size);
    future<size_t> read_some(pollable_fd_state& fd, const std::vector<iovec>& iov);

    future<size_t> write_some(pollable_fd_state& fd, const void* buffer, size_t size);

    future<size_t> write_all(pollable_fd_state& fd, const void* buffer, size_t size);

    void run();

    void add_task(std::unique_ptr<task>&& t) { _pending_tasks.push_back(std::move(t)); }
private:
    void write_all_part(pollable_fd_state& fd, const void* buffer, size_t size,
            promise<size_t> result, size_t completed);

    friend class pollable_fd;
    friend class pollable_fd_state;
};

extern reactor the_reactor;

class pollable_fd_state {
public:
    ~pollable_fd_state() { the_reactor.forget(*this); ::close(fd); }
    explicit pollable_fd_state(int fd) : fd(fd) {}
    pollable_fd_state(const pollable_fd_state&) = delete;
    void operator=(const pollable_fd_state&) = delete;
    int fd;
    int events = 0;
    std::unique_ptr<task> pollin;
    std::unique_ptr<task> pollout;
    friend class reactor;
    friend class pollable_fd;
};

class pollable_fd {
    std::unique_ptr<pollable_fd_state> _s;
    pollable_fd(int fd) : _s(std::make_unique<pollable_fd_state>(fd)) {}
public:
    pollable_fd(pollable_fd&&) = default;
    pollable_fd& operator=(pollable_fd&&) = default;
    future<size_t> read_some(char* buffer, size_t size) { return the_reactor.read_some(*_s, buffer, size); }
    future<size_t> read_some(uint8_t* buffer, size_t size) { return the_reactor.read_some(*_s, buffer, size); }
    future<size_t> read_some(const std::vector<iovec>& iov) { return the_reactor.read_some(*_s, iov); }
    future<size_t> write_all(const char* buffer, size_t size) { return the_reactor.write_all(*_s, buffer, size); }
    future<size_t> write_all(const uint8_t* buffer, size_t size) { return the_reactor.write_all(*_s, buffer, size); }
    future<accept_result> accept() { return the_reactor.accept(*_s); }
    friend class reactor;
};

// A temporary_buffer either points inside a larger buffer, or, if the requested size
// is too large, or if the larger buffer is scattered, contains its own storage.
//
// A temporary_buffer must be consumed before the next operation on the underlying
// input_stream_buffer is initiated.
template <typename CharType>
class temporary_buffer {
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    std::unique_ptr<CharType[]> _own_buffer;
    CharType* _buffer;
    size_t _size;
public:
    explicit temporary_buffer(size_t size) : _own_buffer(new CharType[size]), _buffer(_own_buffer.get()), _size(size) {}
    explicit temporary_buffer(CharType* borrow, size_t size) : _own_buffer(), _buffer(borrow), _size(size) {}
    temporary_buffer() = delete;
    temporary_buffer(const temporary_buffer&) = delete;
    temporary_buffer(temporary_buffer&& x) : _own_buffer(std::move(x._own_buffer)), _buffer(x._buffer), _size(x._size) {
        x._buffer = nullptr;
        x._size = 0;
    }
    void operator=(const temporary_buffer&) = delete;
    temporary_buffer& operator=(temporary_buffer&& x) {
        _own_buffer = std::move(x._own_buffer);
        _buffer = x._buffer;
        _size = x._size;
        x._buffer = nullptr;
        x._size = 0;
        return *this;
    }
    const CharType* get() const { return _buffer; }
    CharType* get_write() { return _buffer; }
    size_t size() const { return _size; }
    const CharType* begin() { return _buffer; }
    const CharType* end() { return _buffer + _size; }
    bool owning() const { return bool(_own_buffer); }
    temporary_buffer prefix(size_t size) && {
        auto ret = std::move(*this);
        ret._size = size;
        return ret;
    }
    CharType operator[](size_t pos) const {
        return _buffer[pos];
    }
};

template <typename CharType>
class input_stream_buffer {
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    pollable_fd& _fd;
    std::unique_ptr<CharType[]> _buf;
    size_t _size;
    size_t _begin = 0;
    size_t _end = 0;
    bool _eof = false;
private:
    using tmp_buf = temporary_buffer<CharType>;
    size_t available() const { return _end - _begin; }
    size_t possibly_available() const { return _size - _begin; }
    tmp_buf allocate(size_t n) {
        if (n <= possibly_available()) {
            return tmp_buf(_buf.get() + _begin, n);
        } else {
            return tmp_buf(n);
        }
    }
    void advance(size_t n) {
        _begin += n;
        if (_begin == _end) {
            _begin = _end = 0;
        }
    }
public:
    using char_type = CharType;
    input_stream_buffer(pollable_fd& fd, size_t size) : _fd(fd), _buf(new char_type[size]), _size(size) {}
    future<temporary_buffer<CharType>> read_exactly(size_t n);
    future<temporary_buffer<CharType>> read_until(size_t limit, CharType eol);
    future<temporary_buffer<CharType>> read_until(size_t limit, const CharType* eol, size_t eol_len);
private:
    void read_exactly_part(size_t n, promise<tmp_buf> pr, tmp_buf buf, size_t completed);
    void read_until_part(size_t n, CharType eol, promise<tmp_buf> pr, tmp_buf buf, size_t completed);
};

template <typename CharType>
class output_stream_buffer {
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    pollable_fd& _fd;
    std::unique_ptr<CharType[]> _buf;
    size_t _size;
    size_t _begin = 0;
    size_t _end = 0;
private:
    size_t available() const { return _end - _begin; }
    size_t possibly_available() const { return _size - _begin; }
public:
    using char_type = CharType;
    output_stream_buffer(pollable_fd& fd, size_t size) : _fd(fd), _buf(new char_type[size]), _size(size) {}
    future<size_t> write(const char_type* buf, size_t n);
    future<bool> flush();
private:
};


inline
future<accept_result>
reactor::accept(pollable_fd_state& listenfd) {
    promise<accept_result> pr;
    future<accept_result> fut = pr.get_future();
    epoll_add_in(listenfd, make_task([this, pr = std::move(pr), lfd = listenfd.fd] () mutable {
        socket_address sa;
        socklen_t sl = sizeof(&sa.u.sas);
        int fd = ::accept4(lfd, &sa.u.sa, &sl, SOCK_NONBLOCK | SOCK_CLOEXEC);
        assert(fd != -1);
        pr.set_value(accept_result{pollable_fd(fd), sa});
    }));
    return fut;
}

inline
future<size_t>
reactor::read_some(pollable_fd_state& fd, void* buffer, size_t len) {
    promise<size_t> pr;
    auto fut = pr.get_future();
    epoll_add_in(fd, make_task([pr = std::move(pr), rfd = fd.fd, buffer, len] () mutable {
        ssize_t r = ::recv(rfd, buffer, len, 0);
        assert(r != -1);
        pr.set_value(r);
    }));
    return fut;
}

inline
future<size_t>
reactor::read_some(pollable_fd_state& fd, const std::vector<iovec>& iov) {
    promise<size_t> pr;
    auto fut = pr.get_future();
    epoll_add_in(fd, make_task([pr = std::move(pr), rfd = fd.fd, iov = iov] () mutable {
        ::msghdr mh = {};
        mh.msg_iov = &iov[0];
        mh.msg_iovlen = iov.size();
        ssize_t r = ::recvmsg(rfd, &mh, 0);
        assert(r != -1);
        pr.set_value(r);
    }));
    return fut;
}

inline
future<size_t>
reactor::write_some(pollable_fd_state& fd, const void* buffer, size_t len) {
    promise<size_t> pr;
    auto fut = pr.get_future();
    epoll_add_out(fd, make_task([pr = std::move(pr), sfd = fd.fd, buffer, len] () mutable {
        ssize_t r = ::send(sfd, buffer, len, 0);
        assert(r != -1);
        pr.set_value(r);
    }));
    return fut;
}

inline
void
reactor::write_all_part(pollable_fd_state& fd, const void* buffer, size_t len,
        promise<size_t> result, size_t completed) {
    if (completed == len) {
        result.set_value(completed);
    } else {
        write_some(fd, static_cast<const char*>(buffer) + completed, len - completed).then(
                [&fd, buffer, len, result = std::move(result), completed, this] (size_t part) mutable {
            write_all_part(fd, buffer, len, std::move(result), completed + part);
        });
    }
}

inline
future<size_t>
reactor::write_all(pollable_fd_state& fd, const void* buffer, size_t len) {
    assert(len);
    promise<size_t> pr;
    auto fut = pr.get_future();
    write_all_part(fd, buffer, len, std::move(pr), 0);
    return fut;
}

template <typename CharType>
void input_stream_buffer<CharType>::read_exactly_part(size_t n, promise<tmp_buf> pr, tmp_buf out, size_t completed) {
    if (available()) {
        auto now = std::min(n - completed, available());
        if (out.owned()) {
            std::copy(_buf.get() + _begin, _buf.get() + _begin + now, out.get() + completed);
        }
        advance(now);
        completed += now;
    }
    if (completed == n) {
        pr.set_value(out);
        return;
    }

    // _buf is now empty
    if (out.owned()) {
        _fd.read_some(out.get() + completed, n - completed).then(
                [this, pr = std::move(pr), out = std::move(out), completed, n] (size_t now) mutable {
            completed += now;
            read_exactly_part(n, std::move(pr), std::move(out), completed);
        });
    } else {
        _fd.read_some(_buf.get(), _size).then(
                [this, pr = std::move(pr), out = std::move(out), completed, n] (size_t now) mutable {
            _end = now;
            read_exactly_part(n, std::move(pr), std::move(out), completed);
        });
    }
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream_buffer<CharType>::read_exactly(size_t n) {
    promise<tmp_buf> pr;
    auto fut = pr.get_future();
    auto buf = allocate(n);
    read_exactly_part(n, std::move(pr), buf, 0);
    return fut;
}


template <typename CharType>
void input_stream_buffer<CharType>::read_until_part(size_t limit, CharType eol, promise<tmp_buf> pr, tmp_buf out,
        size_t completed) {
    auto to_search = std::min(limit - completed, available());
    auto i = std::find(_buf.get() + _begin, _buf.get() + _begin + to_search, eol);
    auto nr_found = i - (_buf.get() + _begin);
    if (i != _buf.get() + _begin + to_search
            || completed + nr_found == limit
            || (i == _buf.get() + _end && _eof)) {
        if (i != _buf.get() + _begin + to_search && completed + nr_found < limit) {
            assert(*i == eol);
            ++i; // include eol in result
            ++nr_found;
        }
        if (out.owning()) {
            std::copy(_buf.get() + _begin, i, out.get_write() + completed);
        }
        advance(nr_found);
        completed += nr_found;
        pr.set_value(std::move(out).prefix(completed));
    } else {
        if (!out.owning() && _end == _size) {
            // wrapping around, must allocate
            auto new_out = tmp_buf(limit);
            out = std::move(new_out);
        }
        if (out.owning()) {
            std::copy(_buf.get() + _begin, _buf.get() + _end, out.get_write() + completed);
            completed += _end - _begin;
            _begin = _end = 0;
        }
        _fd.read_some(_buf.get() + _end, _size - _end).then(
                [this, limit, eol, pr = std::move(pr), out = std::move(out), completed] (size_t n) mutable {
            _end += n;
            _eof = n == 0;
            read_until_part(limit, eol, std::move(pr), std::move(out), completed);
        });
    }
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream_buffer<CharType>::read_until(size_t limit, CharType eol) {
    promise<tmp_buf> pr;
    auto fut = pr.get_future();
    read_until_part(limit, eol, std::move(pr), allocate(possibly_available()), 0);
    return fut;
}

#include <iostream>
#include "sstring.hh"

template <typename CharType>
future<size_t>
output_stream_buffer<CharType>::write(const char_type* buf, size_t n) {
    promise<size_t> pr;
    auto fut = pr.get_future();
    if (n >= _size) {
        flush().then([pr = std::move(pr), this, buf, n] (bool done) mutable {
            _fd.write_all(buf, n).then([pr = std::move(pr)] (size_t done) mutable {
                pr.set_value(done);
            });
        });
        return fut;
    }
    auto now = std::min(n, _size - _end);
    std::copy(buf, buf + now, _buf.get() + _end);
    _end += now;
    if (now == n) {
        pr.set_value(n);
    } else {
        _fd.write_all(_buf.get(), _end).then(
                [pr = std::move(pr), this, now, n, buf] (size_t w) mutable {
            std::copy(buf + now, buf + n, _buf.get());
            _end = n - now;
            pr.set_value(n);
        });
    }
    return fut;
}

template <typename CharType>
future<bool>
output_stream_buffer<CharType>::flush() {
    promise<bool> pr;
    auto fut = pr.get_future();
    if (!_end) {
        pr.set_value(true);
    } else {
        _fd.write_all(_buf.get(), _end).then(
                [this, pr = std::move(pr)] (size_t done) mutable {
            _end = 0;
            pr.set_value(true);
        });
    }
    return fut;
}


template <typename T>
void future_state<T>::make_ready() {
    if (_task) {
        the_reactor.add_task(std::move(_task));
    }
}



#if 0
future<temporary_buffer<CharType>> read_until(size_t limit, const CharType* eol, size_t eol_len);
#endif

#endif /* REACTOR_HH_ */
