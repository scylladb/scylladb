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
#include <experimental/optional>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/optional.hpp>
#include <boost/program_options.hpp>
#include "util/eclipse.hh"
#include "future.hh"
#include "posix.hh"
#include "apply.hh"
#include "sstring.hh"
#include "timer-set.hh"
#include "deleter.hh"
#include "net/api.hh"
#include "temporary_buffer.hh"
#include "circular_buffer.hh"
#include "file.hh"

class reactor;
class pollable_fd;
class pollable_fd_state;

template <typename CharType>
class input_stream;

template <typename CharType>
class output_stream;

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
    bool _expired = false;
public:
    ~timer();
    future<> expired();
    void set_callback(callback_t&& callback);
    void arm(clock_type::time_point until, boost::optional<clock_type::duration> period = {});
    void rearm(clock_type::time_point until, boost::optional<clock_type::duration> period = {});
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
    future<size_t> sendmsg(struct msghdr *msg);
    future<size_t> recvmsg(struct msghdr *msg);
    future<size_t> sendto(socket_address addr, const void* buf, size_t len);
    file_desc& get_file_desc() const { return _s->fd; }
protected:
    int get_fd() const { return _s->fd.get(); }
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


namespace std {

template <>
struct hash<::sockaddr_in> {
    size_t operator()(::sockaddr_in a) const {
        return a.sin_port ^ a.sin_addr.s_addr;
    }
};

}

bool operator==(const ::sockaddr_in a, const ::sockaddr_in b);

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
    virtual net::udp_channel make_udp_channel(ipv4_addr addr = {}) = 0;
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

class syscall_work_queue {
    static constexpr size_t queue_length = 128;
    struct work_item;
    using lf_queue = boost::lockfree::spsc_queue<work_item*,
                            boost::lockfree::capacity<queue_length>>;
    lf_queue _pending;
    lf_queue _completed;
    writeable_eventfd _start_eventfd;
    readable_eventfd _complete_eventfd;
    semaphore _queue_has_room = { queue_length };
    struct work_item {
        virtual ~work_item() {}
        virtual void process() = 0;
        virtual void complete() = 0;
    };
    template <typename T, typename Func>
    struct work_item_returning :  work_item {
        Func _func;
        promise<T> _promise;
        boost::optional<T> _result;
        work_item_returning(Func&& func) : _func(std::move(func)) {}
        virtual void process() override { _result = this->_func(); }
        virtual void complete() override { _promise.set_value(std::move(*_result)); }
        future<T> get_future() { return _promise.get_future(); }
    };
public:
    syscall_work_queue();
    template <typename T, typename Func>
    future<T> submit(Func func) {
        auto wi = new work_item_returning<T, Func>(std::move(func));
        auto fut = wi->get_future();
        submit_item(wi);
        return fut;
    }
    void start() { complete(); }
private:
    void work();
    void complete();
    void submit_item(work_item* wi);

    friend class thread_pool;
};

class smp_message_queue {
    static constexpr size_t queue_length = 128;
    struct work_item;
    using lf_queue = boost::lockfree::spsc_queue<work_item*,
                            boost::lockfree::capacity<queue_length>>;
    lf_queue _pending;
    lf_queue _completed;
    writeable_eventfd _start_eventfd;
    readable_eventfd _complete_eventfd;
    writeable_eventfd _complete_eventfd_write;
    readable_eventfd _start_eventfd_read;
    semaphore _queue_has_room = { queue_length };
    struct work_item {
        virtual ~work_item() {}
        virtual future<> process() = 0;
        virtual void complete() = 0;
    };
    template <typename Func, typename Future>
    struct async_work_item : work_item {
        smp_message_queue& _q;
        Func _func;
        using value_type = typename Future::value_type;
        std::experimental::optional<value_type> _result;
        std::exception_ptr _ex; // if !_result
        typename Future::promise_type _promise; // used on local side
        async_work_item(smp_message_queue& q, Func&& func) : _q(q), _func(std::move(func)) {}
        virtual future<> process() override {
            return this->_func().rescue([this] (auto&& get_result) {
                try {
                    _result = get_result();
                } catch (...) {
                    _ex = std::current_exception();
                }
            });
        }
        virtual void complete() override {
            if (_result) {
                _promise.set_value(std::move(*_result));
            } else {
                // FIXME: _ex was allocated on another cpu
                _promise.set_exception(std::move(_ex));
            }
        }
        Future get_future() { return _promise.get_future(); }
    };
public:
    smp_message_queue();
    template <typename Func>
    std::result_of_t<Func()> submit(Func func) {
        using future = std::result_of_t<Func()>;
        auto wi = new async_work_item<Func, future>(*this, std::move(func));
        auto fut = wi->get_future();
        submit_item(wi);
        return fut;
    }
    void start() { complete(); }
    void listen();
private:
    void work();
    void complete();
    void submit_item(work_item* wi);
    void respond(work_item* wi);

    friend class smp;
};

class thread_pool {
    syscall_work_queue inter_thread_wq;
    posix_thread _worker_thread;
    std::atomic<bool> _stopped = { false };
public:
    thread_pool() : _worker_thread([this] { work(); }) { inter_thread_wq.start(); }
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
    int _return = 0;
    timer_set<timer, &timer::_link, clock_type>::timer_list_t _expired_timers;
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

    template <typename Func>
    future<io_event> submit_io(Func prepare_io);

    int run();
    void exit(int ret);
    future<> when_started() { return _start_promise.get_future(); }
    future<> receive_signal(int signo);

    void add_task(std::unique_ptr<task>&& t) { _pending_tasks.push_back(std::move(t)); }

    network_stack& net() { return *_network_stack; }
    unsigned cpu_id() const { return _id; }
private:
    future<size_t> write_all_part(pollable_fd_state& fd, const void* buffer, size_t size, size_t completed);

    void process_io(size_t count);

    void add_timer(timer* tmr);
    void del_timer(timer* tmr);

    void stop();
    friend class pollable_fd;
    friend class pollable_fd_state;
    friend class file;
    friend class readable_eventfd;
    friend class timer;
};

extern thread_local reactor engine;

class smp {
	static std::vector<posix_thread> _threads;
	static smp_message_queue** _qs;
	static std::thread::id _tmain;

	template <typename Func>
	using returns_future = is_future<std::result_of_t<Func()>>;
public:
	static boost::program_options::options_description get_options_description();
	static void configure(boost::program_options::variables_map vm);
	static void join_all();
	static bool main_thread() { return std::this_thread::get_id() == _tmain; }

	template <typename Func>
	static std::result_of_t<Func()> submit_to(unsigned t, Func func,
	        std::enable_if_t<returns_future<Func>::value, void*> = nullptr) {
	    if (t == engine._id) {
	        return func();
	    } else {
	        return _qs[t][engine._id].submit(std::move(func));
	    }
	}
	template <typename Func>
	static future<> submit_to(unsigned t, Func func,
	        std::enable_if_t<!returns_future<Func>::value, void*> = nullptr) {
	    return submit_to(t, [func = std::move(func)] () mutable {
	       func();
	       return make_ready_future<>();
	    });
        }
private:
	static void listen_all(smp_message_queue* qs);
	static void start_all_queues();
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
    virtual future<> close() = 0;
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
    future<> close() { return _dsi->close(); }
};

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
    bool eof() { return _eof; }
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
    future<> write(const char_type* buf, size_t n);
    future<> write(const char_type* buf);
    future<> write(const sstring& s);
    future<> flush();
    future<> close() { return _fd.close(); }
private:
};

template<typename CharType>
inline
future<> output_stream<CharType>::write(const char_type* buf) {
    return write(buf, strlen(buf));
}

template<typename CharType>
inline
future<> output_stream<CharType>::write(const sstring& s) {
    return write(s.c_str(), s.size());
}

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
size_t iovec_len(const iovec* begin, size_t len)
{
    size_t ret = 0;
    auto end = begin + len;
    while (begin != end) {
        ret += begin++->iov_len;
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
        auto r = fd.fd.send(buffer, len, MSG_NOSIGNAL);
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
future<>
output_stream<CharType>::write(const char_type* buf, size_t n) {
    if (n >= _size) {
        temporary_buffer<char> tmp(n);
        std::copy(buf, buf + n, tmp.get_write());
        return flush().then([this, tmp = std::move(tmp)] () mutable {
            return _fd.put(std::move(tmp));
        });
    }
    auto now = std::min(n, _size - _end);
    std::copy(buf, buf + now, _buf.get_write() + _end);
    _end += now;
    if (now == n) {
        return make_ready_future<>();
    } else {
        temporary_buffer<CharType> next(_size);
        std::copy(buf + now, buf + n, next.get_write());
        _end = n - now;
        std::swap(next, _buf);
        return _fd.put(std::move(next));
    }
}

template <typename CharType>
future<>
output_stream<CharType>::flush() {
    if (!_end) {
        return make_ready_future<>();
    }
    _buf.trim(_end);
    temporary_buffer<CharType> next(_size);
    std::swap(_buf, next);
    return _fd.put(std::move(next)).then([this] {
        _end = 0;
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
future<size_t> pollable_fd::recvmsg(struct msghdr *msg) {
    return engine.readable(*_s).then([this, msg] {
        auto r = get_file_desc().recvmsg(msg, 0);
        if (!r) {
            return recvmsg(msg);
        }
        // We always speculate here to optimize for throughput in a workload
        // with multiple outstanding requests. This way the caller can consume
        // all messages without resorting to epoll. However this adds extra
        // recvmsg() call when we hit the empty queue condition, so it may
        // hurt request-response workload in which the queue is empty when we
        // initially enter recvmsg(). If that turns out to be a problem, we can
        // improve speculation by using recvmmsg().
        _s->speculate_epoll(EPOLLIN);
        return make_ready_future<size_t>(*r);
    });
};

inline
future<size_t> pollable_fd::sendmsg(struct msghdr* msg) {
    return engine.writeable(*_s).then([this, msg] () mutable {
        auto r = get_file_desc().sendmsg(msg, 0);
        if (!r) {
            return sendmsg(msg);
        }
        // For UDP this will always speculate. We can't know if there's room
        // or not, but most of the time there should be so the cost of mis-
        // speculation is amortized.
        if (size_t(*r) == iovec_len(msg->msg_iov, msg->msg_iovlen)) {
            _s->speculate_epoll(EPOLLOUT);
        }
        return make_ready_future<size_t>(*r);
    });
}

inline
future<size_t> pollable_fd::sendto(socket_address addr, const void* buf, size_t len) {
    return engine.writeable(*_s).then([this, buf, len, addr] () mutable {
        auto r = get_file_desc().sendto(addr, buf, len, 0);
        if (!r) {
            return sendto(std::move(addr), buf, len);
        }
        // See the comment about speculation in sendmsg().
        if (size_t(*r) == len) {
            _s->speculate_epoll(EPOLLOUT);
        }
        return make_ready_future<size_t>(*r);
    });
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
    _expired = false;
    _expiry = until;
    engine.add_timer(this);
    _queued = true;
}

inline
void timer::rearm(clock_type::time_point until, boost::optional<clock_type::duration> period) {
    if (_armed) {
        cancel();
    }
    arm(until, period);
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
