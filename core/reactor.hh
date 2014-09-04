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
#include <list>
#include <thread>
#include <system_error>
#include <chrono>
#include <boost/lockfree/queue.hpp>
#include <boost/optional.hpp>
#include "future.hh"
#include "posix.hh"
#include "apply.hh"
#include "sstring.hh"
#include "timer-set.hh"

class socket_address;
class reactor;
class pollable_fd;
class pollable_fd_state;
class file;

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

struct free_deleter {
    void operator()(void* p) { ::free(p); }
};

template <typename CharType>
inline
std::unique_ptr<CharType[], free_deleter> allocate_aligned_buffer(size_t size, size_t align) {
    static_assert(sizeof(CharType) == 1, "must allocate byte type");
    void* ret;
    auto r = posix_memalign(&ret, align, size);
    assert(r == 0);
    return std::unique_ptr<CharType[], free_deleter>(reinterpret_cast<CharType*>(ret));
}

struct listen_options {
    bool reuse_address = false;
};

class semaphore {
private:
    size_t _count;
    std::list<std::pair<promise<>, size_t>> _wait_list;
public:
    semaphore(size_t count = 1) : _count(count) {}
    future<> wait(size_t nr = 1) {
        if (_count >= nr && _wait_list.empty()) {
            _count -= nr;
            return make_ready_future<>();
        }
        promise<> pr;
        auto fut = pr.get_future();
        _wait_list.push_back({ std::move(pr), nr });
        return fut;
    }
    void signal(size_t nr = 1) {
        _count += nr;
        while (!_wait_list.empty() && _wait_list.front().second <= _count) {
            auto& x = _wait_list.front();
            _count -= x.second;
            x.first.set_value();
            _wait_list.pop_front();
        }
    }
    bool try_wait(size_t nr = 1) {
        if (_count >= nr && _wait_list.empty()) {
            _count -= nr;
            return true;
        } else {
            return false;
        }
    }
    size_t current() const { return _count; }
};

using clock_type = std::chrono::high_resolution_clock;

class timer {
    boost::intrusive::list_member_hook<> _link;
    promise<> _pr;
    clock_type::time_point _expiry;
    bool _armed = false;
public:
    ~timer();
    future<> expired();
    void arm(clock_type::time_point until);
    void arm(clock_type::duration delta);
    void suspend();
    clock_type::time_point get_timeout();
    friend class reactor;
    friend class timer_set<timer, &timer::_link, clock_type>;
};

class pollable_fd_state {
public:
    struct speculation {
        int events = 0;
        explicit speculation(int epoll_events_guessed = 0) : events(epoll_events_guessed) {}
    };
    ~pollable_fd_state();
    explicit pollable_fd_state(file_desc fd, speculation speculate = speculation())
        : fd(std::move(fd)), events_known(speculate.events) {}
    pollable_fd_state(const pollable_fd_state&) = delete;
    void operator=(const pollable_fd_state&) = delete;
    void speculate_epoll(int events) { events_known |= events; }
    file_desc fd;
    int events_requested = 0; // wanted by pollin/pollout promises
    int events_epoll = 0;     // installed in epoll
    int events_known = 0;     // returned from epoll
    promise<> pollin;
    promise<> pollout;
    friend class reactor;
    friend class pollable_fd;
};

class pollable_fd {
public:
    using speculation = pollable_fd_state::speculation;
    std::unique_ptr<pollable_fd_state> _s;
    pollable_fd(file_desc fd, speculation speculate = speculation())
        : _s(std::make_unique<pollable_fd_state>(std::move(fd), speculate)) {}
public:
    pollable_fd(pollable_fd&&) = default;
    pollable_fd& operator=(pollable_fd&&) = default;
    future<size_t> read_some(char* buffer, size_t size);
    future<size_t> read_some(uint8_t* buffer, size_t size);
    future<size_t> read_some(const std::vector<iovec>& iov);
    future<size_t> write_all(const char* buffer, size_t size);
    future<size_t> write_all(const uint8_t* buffer, size_t size);
    future<pollable_fd, socket_address> accept();
protected:
    int get_fd() const { return _s->fd.get(); }
    file_desc& get_file_desc() const { return _s->fd; }
    friend class reactor;
    friend class readable_eventfd;
};

class readable_eventfd {
    pollable_fd _fd;
public:
    explicit readable_eventfd(size_t initial = 0) : _fd(try_create_eventfd(initial)) {}
    future<size_t> wait();
    int get_write_fd() { return _fd.get_fd(); }
private:
    static file_desc try_create_eventfd(size_t initial);
};

class writeable_eventfd {
    file_desc _fd;
public:
    explicit writeable_eventfd(size_t initial = 0) : _fd(try_create_eventfd(initial)) {}
    void signal(size_t nr);
    int get_read_fd() { return _fd.get(); }
private:
    static file_desc try_create_eventfd(size_t initial);
};

class thread_pool {
    static constexpr size_t queue_length = 128;
    struct work_item;
    boost::lockfree::queue<work_item*> _pending;
    boost::lockfree::queue<work_item*> _completed;
    writeable_eventfd _start_eventfd;
    readable_eventfd _complete_eventfd;
    semaphore _queue_has_room = { queue_length };
    std::thread _worker_thread;
    struct work_item {
        virtual ~work_item() {}
        virtual void process() = 0;
        virtual void complete() = 0;
    };
    template <typename T, typename Func>
    struct work_item_returning : work_item {
        promise<T> _promise;
        boost::optional<T> _result;
        Func _func;
        work_item_returning(Func&& func) : _func(std::move(func)) {}
        virtual void process() override { _result = _func(); }
        virtual void complete() override { _promise.set_value(std::move(*_result)); }
        future<T> get_future() { return _promise.get_future(); }
    };
public:
    thread_pool();
    template <typename T, typename Func>
    future<T> submit(Func func) {
        auto wi = new work_item_returning<T, Func>(std::move(func));
        auto fut = wi->get_future();
        submit_item(wi);
        return fut;
    }
private:
    void work();
    void complete();
    void submit_item(work_item* wi);
};

class reactor {
    static constexpr size_t max_aio = 128;
    promise<> _start_promise;
    uint64_t _timers_completed;
    clock_type::time_point _next_timeout = clock_type::time_point::max();
    timer_set<timer, &timer::_link, clock_type> _timers;
    pollable_fd _timerfd = file_desc::timerfd_create(CLOCK_REALTIME, TFD_CLOEXEC | TFD_NONBLOCK);
public:
    file_desc _epollfd;
    readable_eventfd _io_eventfd;
    io_context_t _io_context;
    semaphore _io_context_available;
    std::vector<std::unique_ptr<task>> _pending_tasks;
    thread_pool _thread_pool;
private:
    future<> get_epoll_future(pollable_fd_state& fd, promise<> pollable_fd_state::* pr, int event);
    void complete_epoll_event(pollable_fd_state& fd, promise<> pollable_fd_state::* pr, int events, int event);
    future<> readable(pollable_fd_state& fd);
    future<> writeable(pollable_fd_state& fd);
    void forget(pollable_fd_state& fd);
    void abort_on_error(int ret);
    void complete_timers();
public:
    reactor();
    reactor(const reactor&) = delete;
    void operator=(const reactor&) = delete;

    pollable_fd listen(socket_address sa, listen_options opts = {});

    future<pollable_fd, socket_address> accept(pollable_fd_state& listen_fd);

    future<size_t> read_some(pollable_fd_state& fd, void* buffer, size_t size);
    future<size_t> read_some(pollable_fd_state& fd, const std::vector<iovec>& iov);

    future<size_t> write_some(pollable_fd_state& fd, const void* buffer, size_t size);

    future<size_t> write_all(pollable_fd_state& fd, const void* buffer, size_t size);

    future<file> open_file_dma(sstring name);
    future<> flush(file& f);

    void run();
    future<> start() { return _start_promise.get_future(); }

    void add_task(std::unique_ptr<task>&& t) { _pending_tasks.push_back(std::move(t)); }
private:
    future<size_t> write_all_part(pollable_fd_state& fd, const void* buffer, size_t size, size_t completed);

    future<size_t> read_dma(file& f, uint64_t pos, void* buffer, size_t len);
    future<size_t> read_dma(file& f, uint64_t pos, std::vector<iovec> iov);
    future<size_t> write_dma(file& f, uint64_t pos, const void* buffer, size_t len);
    future<size_t> write_dma(file& f, uint64_t pos, std::vector<iovec> iov);

    template <typename Func>
    future<io_event> submit_io(Func prepare_io);

    void process_io(size_t count);

    void add_timer(timer* tmr);
    void del_timer(timer* tmr);

    friend class pollable_fd;
    friend class pollable_fd_state;
    friend class file;
    friend class thread_pool;
    friend class readable_eventfd;
    friend class timer;
};

extern reactor the_reactor;

inline
pollable_fd_state::~pollable_fd_state() {
    the_reactor.forget(*this);
}

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
    future<temporary_buffer<CharType>> read_exactly_part(size_t n, tmp_buf buf, size_t completed);
    future<temporary_buffer<CharType>> read_until_part(size_t n, CharType eol, tmp_buf buf, size_t completed);
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

class file {
    int _fd;
private:
    explicit file(int fd) : _fd(fd) {}
public:
    ~file() {
        if (_fd != -1) {
            ::close(_fd);
        }
    }
    file(file&& x) : _fd(x._fd) { x._fd = -1; }
    template <typename CharType>
    future<size_t> dma_read(uint64_t pos, CharType* buffer, size_t len) {
        return the_reactor.read_dma(*this, pos, buffer, len);
    }

    future<size_t> dma_read(uint64_t pos, std::vector<iovec> iov) {
        return the_reactor.read_dma(*this, pos, std::move(iov));
    }

    template <typename CharType>
    future<size_t> dma_write(uint64_t pos, const CharType* buffer, size_t len) {
        return the_reactor.write_dma(*this, pos, buffer, len);
    }

    future<size_t> dma_write(uint64_t pos, std::vector<iovec> iov) {
        return the_reactor.write_dma(*this, pos, std::move(iov));
    }

    future<> flush() {
        return the_reactor.flush(*this);
    }

    friend class reactor;
};

inline
size_t iovec_len(const std::vector<iovec>& iov)
{
    size_t ret = 0;
    for (auto&& e : iov) {
        ret += e.iov_len;
    }
    return ret;
}

inline
future<pollable_fd, socket_address>
reactor::accept(pollable_fd_state& listenfd) {
    return readable(listenfd).then([this, &listenfd] () mutable {
        socket_address sa;
        socklen_t sl = sizeof(&sa.u.sas);
        file_desc fd = listenfd.fd.accept(sa.u.sa, sl, SOCK_NONBLOCK | SOCK_CLOEXEC);
        pollable_fd pfd(std::move(fd), pollable_fd::speculation(EPOLLOUT));
        return make_ready_future<pollable_fd, socket_address>(std::move(pfd), std::move(sa));
    });
}

inline
future<size_t>
reactor::read_some(pollable_fd_state& fd, void* buffer, size_t len) {
    return readable(fd).then([this, &fd, buffer, len] () mutable {
        auto r = fd.fd.read(buffer, len);
        if (!r) {
            return read_some(fd, buffer, len);
        }
        if (size_t(*r) == len) {
            fd.speculate_epoll(EPOLLIN);
        }
        return make_ready_future<size_t>(*r);
    });
}

inline
future<size_t>
reactor::read_some(pollable_fd_state& fd, const std::vector<iovec>& iov) {
    return readable(fd).then([this, &fd, iov = iov] () mutable {
        ::msghdr mh = {};
        mh.msg_iov = &iov[0];
        mh.msg_iovlen = iov.size();
        auto r = fd.fd.recvmsg(&mh, 0);
        if (!r) {
            return read_some(fd, iov);
        }
        if (size_t(*r) == iovec_len(iov)) {
            fd.speculate_epoll(EPOLLIN);
        }
        return make_ready_future<size_t>(*r);
    });
}

inline
future<size_t>
reactor::write_some(pollable_fd_state& fd, const void* buffer, size_t len) {
    return writeable(fd).then([this, &fd, buffer, len] () mutable {
        auto r = fd.fd.send(buffer, len, 0);
        if (!r) {
            return write_some(fd, buffer, len);
        }
        if (size_t(*r) == len) {
            fd.speculate_epoll(EPOLLOUT);
        }
        return make_ready_future<size_t>(*r);
    });
}

inline
future<size_t>
reactor::write_all_part(pollable_fd_state& fd, const void* buffer, size_t len, size_t completed) {
    if (completed == len) {
        return make_ready_future<size_t>(completed);
    } else {
        return write_some(fd, static_cast<const char*>(buffer) + completed, len - completed).then(
                [&fd, buffer, len, completed, this] (size_t part) mutable {
            return write_all_part(fd, buffer, len, completed + part);
        });
    }
}

inline
future<size_t>
reactor::write_all(pollable_fd_state& fd, const void* buffer, size_t len) {
    assert(len);
    return write_all_part(fd, buffer, len, 0);
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream_buffer<CharType>::read_exactly_part(size_t n, tmp_buf out, size_t completed) {
    if (available()) {
        auto now = std::min(n - completed, available());
        if (out.owned()) {
            std::copy(_buf.get() + _begin, _buf.get() + _begin + now, out.get() + completed);
        }
        advance(now);
        completed += now;
    }
    if (completed == n) {
        return make_ready_future<tmp_buf>(out);
    }

    // _buf is now empty
    if (out.owned()) {
        return _fd.read_some(out.get() + completed, n - completed).then(
                [this, out = std::move(out), completed, n] (size_t now) mutable {
            completed += now;
            return read_exactly_part(n, std::move(out), completed);
        });
    } else {
        _fd.read_some(_buf.get(), _size).then(
                [this, out = std::move(out), completed, n] (size_t now) mutable {
            _end = now;
            return read_exactly_part(n, std::move(out), completed);
        });
    }
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream_buffer<CharType>::read_exactly(size_t n) {
    auto buf = allocate(n);
    return read_exactly_part(n, buf, 0);
}


template <typename CharType>
future<temporary_buffer<CharType>>
input_stream_buffer<CharType>::read_until_part(size_t limit, CharType eol, tmp_buf out,
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
        return make_ready_future<tmp_buf>(std::move(out).prefix(completed));
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
        return _fd.read_some(_buf.get() + _end, _size - _end).then(
                [this, limit, eol, out = std::move(out), completed] (size_t n) mutable {
            _end += n;
            _eof = n == 0;
            return read_until_part(limit, eol, std::move(out), completed);
        });
    }
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream_buffer<CharType>::read_until(size_t limit, CharType eol) {
    return read_until_part(limit, eol, allocate(possibly_available()), 0);
}

#include <iostream>
#include "sstring.hh"

template <typename CharType>
future<size_t>
output_stream_buffer<CharType>::write(const char_type* buf, size_t n) {
    if (n >= _size) {
        return flush().then([this, buf, n] (bool done) mutable {
            return _fd.write_all(buf, n);
        });
    }
    auto now = std::min(n, _size - _end);
    std::copy(buf, buf + now, _buf.get() + _end);
    _end += now;
    if (now == n) {
        return make_ready_future<size_t>(n);
    } else {
        return _fd.write_all(_buf.get(), _end).then(
                [this, now, n, buf] (size_t w) mutable {
            std::copy(buf + now, buf + n, _buf.get());
            _end = n - now;
            return make_ready_future<size_t>(n);
        });
    }
}

template <typename CharType>
future<bool>
output_stream_buffer<CharType>::flush() {
    if (!_end) {
        return make_ready_future<bool>(true);
    }
    return _fd.write_all(_buf.get(), _end).then(
            [this] (size_t done) mutable {
        _end = 0;
        return make_ready_future<bool>(true);
    });
}

inline
future<size_t> pollable_fd::read_some(char* buffer, size_t size) {
    return the_reactor.read_some(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::read_some(uint8_t* buffer, size_t size) {
    return the_reactor.read_some(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::read_some(const std::vector<iovec>& iov) {
    return the_reactor.read_some(*_s, iov);
}

inline
future<size_t> pollable_fd::write_all(const char* buffer, size_t size) {
    return the_reactor.write_all(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::write_all(const uint8_t* buffer, size_t size) {
    return the_reactor.write_all(*_s, buffer, size);
}

inline
future<pollable_fd, socket_address> pollable_fd::accept() {
    return the_reactor.accept(*_s);
}

inline
future<> timer::expired() {
    return _pr.get_future().then([this] {
        _pr = promise<>();  // prepare for next call
        return make_ready_future<>();
    });
}

inline
timer::~timer() {
    if (_armed) {
        the_reactor.del_timer(this);
    }
}

inline
void timer::arm(clock_type::time_point until) {
    assert(!_armed);
    _armed = true;
    _expiry = until;
    the_reactor.add_timer(this);
}

inline
void timer::arm(clock_type::duration delta) {
    return arm(clock_type::now() + delta);
}

inline
void timer::suspend() {
    assert(_armed);
    _armed = false;
    the_reactor.del_timer(this);
}

inline
clock_type::time_point timer::get_timeout() {
    return _expiry;
}

#if 0
future<temporary_buffer<CharType>> read_until(size_t limit, const CharType* eol, size_t eol_len);
#endif

#endif /* REACTOR_HH_ */
