/*
 * Copyright 2014 Cloudius Systems
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
#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <boost/optional.hpp>
#include <boost/program_options.hpp>
#include "util/eclipse.hh"
#include "future.hh"
#include "posix.hh"
#include "apply.hh"
#include "sstring.hh"
#include "timer-set.hh"
#include "deleter.hh"
#include "temporary_buffer.hh"
#include "circular_buffer.hh"

class socket_address;
class reactor;
class pollable_fd;
class pollable_fd_state;
class file;

template <typename CharType>
class input_stream;

template <typename CharType>
class output_stream;

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
public:
    ::sockaddr& as_posix_sockaddr() { return u.sa; }
    ::sockaddr_in& as_posix_sockaddr_in() { return u.in; }
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
    using callback_t = std::function<void()>;
    boost::intrusive::list_member_hook<> _link;
    callback_t _callback;
    clock_type::time_point _expiry;
    boost::optional<clock_type::duration> _period;
    bool _armed = false;
    bool _queued = false;
public:
    ~timer();
    future<> expired();
    void set_callback(callback_t&& callback);
    void arm(clock_type::time_point until, boost::optional<clock_type::duration> period = {});
    void arm(clock_type::duration delta);
    void arm_periodic(clock_type::duration delta);
    bool armed() const { return _armed; }
    bool cancel();
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
    friend class writeable_eventfd;
};

class connected_socket_impl {
public:
    virtual ~connected_socket_impl() {}
    virtual input_stream<char> input() = 0;
    virtual output_stream<char> output() = 0;
};

class connected_socket {
    std::unique_ptr<connected_socket_impl> _csi;
public:
    explicit connected_socket(std::unique_ptr<connected_socket_impl> csi)
        : _csi(std::move(csi)) {}
    input_stream<char> input();
    output_stream<char> output();
};

class server_socket_impl {
public:
    virtual ~server_socket_impl() {}
    virtual future<connected_socket, socket_address> accept() = 0;
};

class posix_server_socket_impl : public server_socket_impl {
    pollable_fd _lfd;
public:
    explicit posix_server_socket_impl(pollable_fd lfd) : _lfd(std::move(lfd)) {}
    virtual future<connected_socket, socket_address> accept();
};

class server_socket {
    std::unique_ptr<server_socket_impl> _ssi;
public:
    explicit server_socket(std::unique_ptr<server_socket_impl> ssi)
        : _ssi(std::move(ssi)) {}
    future<connected_socket, socket_address> accept() {
        return _ssi->accept();
    }
};

class network_stack {
public:
    virtual ~network_stack() {}
    virtual server_socket listen(socket_address sa, listen_options opts) = 0;
};

class network_stack_registry {
public:
    using options = boost::program_options::variables_map;
private:
    static std::unordered_map<sstring,
            std::function<std::unique_ptr<network_stack> (options opts)>>& _map() {
        static std::unordered_map<sstring,
                std::function<std::unique_ptr<network_stack> (options opts)>> map;
        return map;
    }
    static sstring& _default() {
        static sstring def;
        return def;
    }
public:
    static boost::program_options::options_description& options_description() {
        static boost::program_options::options_description opts;
        return opts;
    }
    static void register_stack(sstring name,
            boost::program_options::options_description opts,
            std::function<std::unique_ptr<network_stack> (options opts)> create,
            bool make_default = false);
    static sstring default_stack();
    static std::vector<sstring> list();
    static std::unique_ptr<network_stack> create(options opts);
    static std::unique_ptr<network_stack> create(sstring name, options opts);
};

class network_stack_registrator {
public:
    using options = boost::program_options::variables_map;
    explicit network_stack_registrator(sstring name,
            boost::program_options::options_description opts,
            std::function<std::unique_ptr<network_stack> (options opts)> factory,
            bool make_default = false) {
        network_stack_registry::register_stack(name, opts, factory, make_default);
    }
};

class posix_network_stack : public network_stack {
public:
    posix_network_stack(boost::program_options::variables_map opts) {}
    virtual server_socket listen(socket_address sa, listen_options opts) override;
    static std::unique_ptr<network_stack> create(boost::program_options::variables_map opts) {
        return std::unique_ptr<network_stack>(new posix_network_stack(opts));
    }
};

class writeable_eventfd;

class readable_eventfd {
    pollable_fd _fd;
public:
    explicit readable_eventfd(size_t initial = 0) : _fd(try_create_eventfd(initial)) {}
    readable_eventfd(readable_eventfd&&) = default;
    writeable_eventfd write_side();
    future<size_t> wait();
    int get_write_fd() { return _fd.get_fd(); }
private:
    explicit readable_eventfd(file_desc&& fd) : _fd(std::move(fd)) {}
    static file_desc try_create_eventfd(size_t initial);

    friend class writeable_eventfd;
};

class writeable_eventfd {
    file_desc _fd;
public:
    explicit writeable_eventfd(size_t initial = 0) : _fd(try_create_eventfd(initial)) {}
    writeable_eventfd(writeable_eventfd&&) = default;
    readable_eventfd read_side();
    void signal(size_t nr);
    int get_read_fd() { return _fd.get(); }
private:
    explicit writeable_eventfd(file_desc&& fd) : _fd(std::move(fd)) {}
    static file_desc try_create_eventfd(size_t initial);

    friend class readable_eventfd;
};

class thread_pool;
class smp;

class inter_thread_work_queue {
    static constexpr size_t queue_length = 128;
    struct work_item;
    boost::lockfree::queue<work_item*> _pending;
    boost::lockfree::queue<work_item*> _completed;
    writeable_eventfd _start_eventfd;
    readable_eventfd _complete_eventfd;
    semaphore _queue_has_room = { queue_length };
    struct work_item {
        virtual ~work_item() {}
        virtual void process() = 0;
        virtual void complete() = 0;
    };
    template <typename Func>
    struct work_item_void : public work_item {
        promise<> _promise;
        Func _func;
        work_item_void(Func&& func) : _func(std::move(func)) {}
        virtual void process() override { _func(); }
        virtual void complete() override { _promise.set_value(); }
        future<> get_future() { return _promise.get_future(); }
    };
    template <typename T, typename Func>
    struct work_item_returning : public work_item_void<Func> {
        promise<T> _promise;
        boost::optional<T> _result;
        work_item_returning(Func&& func) : work_item_void<Func>(std::move(func)) {}
        virtual void process() override { _result = this->_func(); }
        virtual void complete() override { _promise.set_value(std::move(*_result)); }
        future<T> get_future() { return _promise.get_future(); }
    };
public:
    inter_thread_work_queue();
    template <typename T, typename Func>
    future<T> submit(Func func) {
        auto wi = new work_item_returning<T, Func>(std::move(func));
        auto fut = wi->get_future();
        submit_item(wi);
        return fut;
    }
    template <typename Func>
    future<> submit(Func func) {
        auto wi = new work_item_void<Func>(std::move(func));
        auto fut = wi->get_future();
        submit_item(wi);
        return fut;
    }
private:
    void work();
    void complete();
    void submit_item(work_item* wi);

    friend class thread_pool;
    friend class smp;
};

class thread_pool {
    inter_thread_work_queue inter_thread_wq;
    std::thread _worker_thread;
    std::atomic<bool> _stopped = { false };
public:
    thread_pool() : _worker_thread([this] { work(); }) {}
    ~thread_pool();
    template <typename T, typename Func>
    future<T> submit(Func func) {return inter_thread_wq.submit<T>(std::move(func));}
private:
    void work();
};

class reactor {
    static constexpr size_t max_aio = 128;
    promise<> _start_promise;
    uint64_t _timers_completed;
    timer_set<timer, &timer::_link, clock_type> _timers;
    pollable_fd _timerfd = file_desc::timerfd_create(CLOCK_REALTIME, TFD_CLOEXEC | TFD_NONBLOCK);
    struct signal_handler {
        signal_handler(int signo);
        promise<> _promise;
        pollable_fd _signalfd;
        signalfd_siginfo _siginfo;
    };
    std::unordered_map<int, signal_handler> _signal_handlers;
    bool _stopped = false;
    bool _handle_sigint = true;
    std::unique_ptr<network_stack> _network_stack;
public:
    file_desc _epollfd;
    readable_eventfd _io_eventfd;
    io_context_t _io_context;
    semaphore _io_context_available;
    circular_buffer<std::unique_ptr<task>> _pending_tasks;
    thread_pool _thread_pool;
    unsigned _id = 0;
private:
    future<> get_epoll_future(pollable_fd_state& fd, promise<> pollable_fd_state::* pr, int event);
    void complete_epoll_event(pollable_fd_state& fd, promise<> pollable_fd_state::* pr, int events, int event);
    future<> readable(pollable_fd_state& fd);
    future<> writeable(pollable_fd_state& fd);
    void forget(pollable_fd_state& fd);
    void abort_on_error(int ret);
    void complete_timers();
public:
    static boost::program_options::options_description get_options_description();
    reactor();
    reactor(const reactor&) = delete;
    void operator=(const reactor&) = delete;

    void configure(boost::program_options::variables_map config);

    server_socket listen(socket_address sa, listen_options opts = {});

    pollable_fd posix_listen(socket_address sa, listen_options opts = {});

    future<pollable_fd, socket_address> accept(pollable_fd_state& listen_fd);

    future<size_t> read_some(pollable_fd_state& fd, void* buffer, size_t size);
    future<size_t> read_some(pollable_fd_state& fd, const std::vector<iovec>& iov);

    future<size_t> write_some(pollable_fd_state& fd, const void* buffer, size_t size);

    future<size_t> write_all(pollable_fd_state& fd, const void* buffer, size_t size);

    future<file> open_file_dma(sstring name);
    future<> flush(file& f);

    void run();
    future<> when_started() { return _start_promise.get_future(); }
    future<> receive_signal(int signo);

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
    friend class readable_eventfd;
    friend class timer;
};

extern thread_local reactor engine;

class smp {
	static std::vector<std::thread> _threads;
	static inter_thread_work_queue** _qs;
public:
	static boost::program_options::options_description get_options_description();
	static void configure(boost::program_options::variables_map vm);
	static void join_all();

	template <typename T, typename Func>
	static future<T> submit_to(unsigned t, Func func) {
	    return t == engine._id ? make_ready_future<T>(func()) :
	            _qs[t][engine._id].submit<T>(std::move(func));}
	template <typename Func>
	static future<> submit_to(unsigned t, Func func) {
	    return t == engine._id ? func(),  make_ready_future<>():
	            _qs[t][engine._id].submit<>(std::move(func));}
private:
	static void listen_all(inter_thread_work_queue* qs);
	static void listen_one(inter_thread_work_queue& q, std::unique_ptr<readable_eventfd>&& rfd, std::unique_ptr<writeable_eventfd>&& wfd);
public:
	static unsigned count;
};

inline
pollable_fd_state::~pollable_fd_state() {
    engine.forget(*this);
}

class data_source_impl {
public:
    virtual ~data_source_impl() {}
    virtual future<temporary_buffer<char>> get() = 0;
};

class data_source {
    std::unique_ptr<data_source_impl> _dsi;
public:
    explicit data_source(std::unique_ptr<data_source_impl> dsi) : _dsi(std::move(dsi)) {}
    data_source(data_source&& x) = default;
    future<temporary_buffer<char>> get() { return _dsi->get(); }
};

class data_sink_impl {
public:
    virtual ~data_sink_impl() {}
    virtual future<> put(std::vector<temporary_buffer<char>> data) = 0;
    virtual future<> put(temporary_buffer<char> data) {
        std::vector<temporary_buffer<char>> v;
        v.reserve(1);
        v.push_back(std::move(data));
        return put(std::move(v));
    }
};

class data_sink {
    std::unique_ptr<data_sink_impl> _dsi;
public:
    explicit data_sink(std::unique_ptr<data_sink_impl> dsi) : _dsi(std::move(dsi)) {}
    data_sink(data_sink&& x) = default;
    future<> put(std::vector<temporary_buffer<char>> data) {
        return _dsi->put(std::move(data));
    }
    future<> put(temporary_buffer<char> data) {
        return _dsi->put(std::move(data));
    }
};

class posix_data_source_impl final : public data_source_impl {
    pollable_fd& _fd;
    temporary_buffer<char> _buf;
    size_t _buf_size;
public:
    explicit posix_data_source_impl(pollable_fd& fd, size_t buf_size = 8192)
        : _fd(fd), _buf(buf_size), _buf_size(buf_size) {}
    virtual future<temporary_buffer<char>> get() override;
};

class posix_data_sink_impl : public data_sink_impl {
    pollable_fd& _fd;
    std::vector<temporary_buffer<char>> _data;
private:
    future<> do_write(size_t idx);
public:
    explicit posix_data_sink_impl(pollable_fd& fd) : _fd(fd) {}
    future<> put(std::vector<temporary_buffer<char>> data) override;
};

data_source posix_data_source(pollable_fd& fd);
data_sink posix_data_sink(pollable_fd& fd);

template <typename CharType>
class input_stream {
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    data_source _fd;
    temporary_buffer<CharType> _buf;
    bool _eof = false;
private:
    using tmp_buf = temporary_buffer<CharType>;
    size_t available() const { return _buf.size(); }
public:
    // Consumer concept, for consume() method:
    struct ConsumerConcept {
        // call done(tmp_buf) to signal end of processing. tmp_buf parameter to
        // done is unconsumed data
        template <typename Done>
        void operator()(tmp_buf data, Done done);
    };
    using char_type = CharType;
    explicit input_stream(data_source fd, size_t buf_size = 8192) : _fd(std::move(fd)), _buf(0) {}
    future<temporary_buffer<CharType>> read_exactly(size_t n);
    template <typename Consumer>
    future<> consume(Consumer& c);
private:
    future<temporary_buffer<CharType>> read_exactly_part(size_t n, tmp_buf buf, size_t completed);
};

template <typename CharType>
class output_stream {
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    data_sink _fd;
    temporary_buffer<CharType> _buf;
    size_t _size;
    size_t _begin = 0;
    size_t _end = 0;
private:
    size_t available() const { return _end - _begin; }
    size_t possibly_available() const { return _size - _begin; }
public:
    using char_type = CharType;
    output_stream(data_sink fd, size_t size)
        : _fd(std::move(fd)), _buf(size), _size(size) {}
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
        return engine.read_dma(*this, pos, buffer, len);
    }

    future<size_t> dma_read(uint64_t pos, std::vector<iovec> iov) {
        return engine.read_dma(*this, pos, std::move(iov));
    }

    template <typename CharType>
    future<size_t> dma_write(uint64_t pos, const CharType* buffer, size_t len) {
        return engine.write_dma(*this, pos, buffer, len);
    }

    future<size_t> dma_write(uint64_t pos, std::vector<iovec> iov) {
        return engine.write_dma(*this, pos, std::move(iov));
    }

    future<> flush() {
        return engine.flush(*this);
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
input_stream<CharType>::read_exactly_part(size_t n, tmp_buf out, size_t completed) {
    if (available()) {
        auto now = std::min(n - completed, available());
        std::copy(_buf.get(), _buf.get() + now, out.get() + completed);
        _buf.trim_front(now);
        completed += now;
    }
    if (completed == n) {
        return make_ready_future<tmp_buf>(out);
    }

    // _buf is now empty
    return _fd.get().then([this, n, out = std::move(out), completed] (char* data, size_t m, std::unique_ptr<deleter> d) {
        _buf = tmp_buf(data, m, std::move(d));
        return read_exactly_part(n, std::move(out), completed);
    });
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream<CharType>::read_exactly(size_t n) {
    if (_buf.size() == n) {
        // easy case: steal buffer, return to caller
        return make_ready_future<temporary_buffer<CharType>>(std::move(_buf));
    } else if (_buf.size() > n) {
        // buffer large enough, share it with caller
        auto front = _buf.share(0, n);
        _buf.trim_front(n);
        return make_ready_future<temporary_buffer<CharType>>(front);
    } else if (_buf.size() == 0) {
        // buffer is empty: grab one and retry
        return _fd.get().then([this, n] (char* data, size_t m, std::unique_ptr<deleter> d) {
            _buf = tmp_buf(data, m, std::move(d));
            return read_exactly(n);
        });
    } else {
        // buffer too small: start copy/read loop
        temporary_buffer<CharType> b(n);
        return read_exactly_part(n, std::move(b), 0);
    }
}

template <typename CharType>
template <typename Consumer>
future<>
input_stream<CharType>::consume(Consumer& consumer) {
    if (_buf.empty() && !_eof) {
        return _fd.get().then([this, &consumer] (tmp_buf buf) {
            _buf = std::move(buf);
            _eof = _buf.empty();
            return consume(consumer);
        });
    } else {
        auto tmp = std::move(_buf);
        bool done = tmp.empty();
        consumer(std::move(tmp), [this, &done] (tmp_buf unconsumed) {
            done = true;
            if (!unconsumed.empty()) {
                _buf = std::move(unconsumed);
            }
        });
        if (!done) {
            return consume(consumer);
        } else {
            return make_ready_future<>();
        }
    }
}

#include <iostream>
#include "sstring.hh"

template <typename CharType>
future<size_t>
output_stream<CharType>::write(const char_type* buf, size_t n) {
    if (n >= _size) {
        temporary_buffer<char> tmp(n);
        std::copy(buf, buf + n, tmp.get_write());
        return flush().then([this, tmp = std::move(tmp)] (bool done) mutable {
            return _fd.put(std::move(tmp));
        }).then([n] {
            return make_ready_future<size_t>(n);
        });
    }
    auto now = std::min(n, _size - _end);
    std::copy(buf, buf + now, _buf.get_write() + _end);
    _end += now;
    if (now == n) {
        return make_ready_future<size_t>(n);
    } else {
        temporary_buffer<CharType> next(_size);
        std::copy(buf + now, buf + n, next.get_write());
        _end = n - now;
        std::swap(next, _buf);
        return _fd.put(std::move(next)).then(
                [this, n] () mutable {
            return make_ready_future<size_t>(n);
        });
    }
}

template <typename CharType>
future<bool>
output_stream<CharType>::flush() {
    if (!_end) {
        return make_ready_future<bool>(true);
    }
    _buf.trim(_end);
    temporary_buffer<CharType> next(_size);
    std::swap(_buf, next);
    return _fd.put(std::move(next)).then([this] {
        _end = 0;
        return make_ready_future<bool>(true);
    });
}

inline
future<size_t> pollable_fd::read_some(char* buffer, size_t size) {
    return engine.read_some(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::read_some(uint8_t* buffer, size_t size) {
    return engine.read_some(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::read_some(const std::vector<iovec>& iov) {
    return engine.read_some(*_s, iov);
}

inline
future<size_t> pollable_fd::write_all(const char* buffer, size_t size) {
    return engine.write_all(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::write_all(const uint8_t* buffer, size_t size) {
    return engine.write_all(*_s, buffer, size);
}

inline
future<pollable_fd, socket_address> pollable_fd::accept() {
    return engine.accept(*_s);
}

inline
timer::~timer() {
    if (_queued) {
        engine.del_timer(this);
    }
}

inline
void timer::set_callback(callback_t&& callback) {
    _callback = std::move(callback);
}

inline
void timer::arm(clock_type::time_point until, boost::optional<clock_type::duration> period) {
    assert(!_armed);
    _period = period;
    _armed = true;
    _expiry = until;
    engine.add_timer(this);
    _queued = true;
}

inline
void timer::arm(clock_type::duration delta) {
    return arm(clock_type::now() + delta);
}

inline
void timer::arm_periodic(clock_type::duration delta) {
    arm(clock_type::now() + delta, {delta});
}

inline
bool timer::cancel() {
    if (!_armed) {
        return false;
    }
    _armed = false;
    if (_queued) {
        engine.del_timer(this);
        _queued = false;
    }
    return true;
}

inline
clock_type::time_point timer::get_timeout() {
    return _expiry;
}

inline
input_stream<char>
connected_socket::input() {
    return _csi->input();
}

inline
output_stream<char>
connected_socket::output() {
    return _csi->output();
}

#endif /* REACTOR_HH_ */
