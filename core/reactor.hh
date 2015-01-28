/*
 * Copyright 2014 Cloudius Systems
 */

#ifndef REACTOR_HH_
#define REACTOR_HH_

#include <memory>
#include <type_traits>
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
#include <queue>
#include <algorithm>
#include <thread>
#include <system_error>
#include <chrono>
#include <ratio>
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
#include "semaphore.hh"
#include "core/scattered_message.hh"

#ifdef HAVE_OSV
#include <osv/newpoll.hh>
#endif

namespace scollectd { class registration; }

class reactor;
class pollable_fd;
class pollable_fd_state;
class lowres_clock;

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

using clock_type = std::chrono::high_resolution_clock;

template <typename Clock = std::chrono::high_resolution_clock>
class timer {
public:
    typedef typename Clock::time_point time_point;
    typedef typename Clock::duration duration;
    typedef Clock clock;
private:
    using callback_t = std::function<void()>;
    boost::intrusive::list_member_hook<> _link;
    callback_t _callback;
    time_point _expiry;
    boost::optional<duration> _period;
    bool _armed = false;
    bool _queued = false;
    bool _expired = false;
public:
    timer() = default;
    explicit timer(callback_t&& callback);
    ~timer();
    future<> expired();
    void set_callback(callback_t&& callback);
    void arm(time_point until, boost::optional<duration> period = {});
    void rearm(time_point until, boost::optional<duration> period = {});
    void arm(duration delta);
    void arm_periodic(duration delta);
    bool armed() const { return _armed; }
    bool cancel();
    time_point get_timeout();
    friend class reactor;
    friend class timer_set<timer, &timer::_link>;
};

class lowres_clock {
public:
    typedef int64_t rep;
    // The lowres_clock's resolution is 10ms. However, to make it is easier to
    // do calcuations with std::chrono::milliseconds, we make the clock's
    // period to 1ms instead of 10ms.
    typedef std::ratio<1, 1000> period;
    typedef std::chrono::duration<rep, period> duration;
    typedef std::chrono::time_point<lowres_clock, duration> time_point;
    lowres_clock();
    static time_point now() {
        auto nr = _now.load(std::memory_order_relaxed);
        return time_point(duration(nr));
    }
private:
    static void update();
    // _now is updated by cpu0 and read by other cpus. Make _now on its own
    // cache line to avoid false sharing.
    static std::atomic<rep> _now [[gnu::aligned(64)]];
    // High resolution timer to drive this low resolution clock
    static timer<> _timer [[gnu::aligned(64)]];
    // High resolution timer expires every 10 milliseconds
    static constexpr std::chrono::milliseconds _granularity{10};
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

inline
size_t iovec_len(const std::vector<iovec>& iov)
{
    size_t ret = 0;
    for (auto&& e : iov) {
        ret += e.iov_len;
    }
    return ret;
}

class pollable_fd {
public:
    using speculation = pollable_fd_state::speculation;
    pollable_fd(file_desc fd, speculation speculate = speculation())
        : _s(std::make_unique<pollable_fd_state>(std::move(fd), speculate)) {}
public:
    pollable_fd(pollable_fd&&) = default;
    pollable_fd& operator=(pollable_fd&&) = default;
    future<size_t> read_some(char* buffer, size_t size);
    future<size_t> read_some(uint8_t* buffer, size_t size);
    future<size_t> read_some(const std::vector<iovec>& iov);
    future<> write_all(const char* buffer, size_t size);
    future<> write_all(const uint8_t* buffer, size_t size);
    future<size_t> write_some(net::packet& p);
    future<> write_all(net::packet& p);
    future<> readable();
    future<> writeable();
    future<pollable_fd, socket_address> accept();
    future<size_t> sendmsg(struct msghdr *msg);
    future<size_t> recvmsg(struct msghdr *msg);
    future<size_t> sendto(socket_address addr, const void* buf, size_t len);
    file_desc& get_file_desc() const { return _s->fd; }
    void close() { _s.reset(); }
protected:
    int get_fd() const { return _s->fd.get(); }
    friend class reactor;
    friend class readable_eventfd;
    friend class writeable_eventfd;
private:
    std::unique_ptr<pollable_fd_state> _s;
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
    virtual future<connected_socket> connect(socket_address sa) = 0;
    virtual net::udp_channel make_udp_channel(ipv4_addr addr = {}) = 0;
    virtual future<> initialize() {
        return make_ready_future();
    }
    virtual bool has_per_core_namespace() = 0;
};

class network_stack_registry {
public:
    using options = boost::program_options::variables_map;
private:
    static std::unordered_map<sstring,
            std::function<future<std::unique_ptr<network_stack>> (options opts)>>& _map() {
        static std::unordered_map<sstring,
                std::function<future<std::unique_ptr<network_stack>> (options opts)>> map;
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
            std::function<future<std::unique_ptr<network_stack>> (options opts)> create,
            bool make_default = false);
    static sstring default_stack();
    static std::vector<sstring> list();
    static future<std::unique_ptr<network_stack>> create(options opts);
    static future<std::unique_ptr<network_stack>> create(sstring name, options opts);
};

class network_stack_registrator {
public:
    using options = boost::program_options::variables_map;
    explicit network_stack_registrator(sstring name,
            boost::program_options::options_description opts,
            std::function<future<std::unique_ptr<network_stack>> (options opts)> factory,
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

// The reactor_notifier interface is a simplified version of Linux's eventfd
// interface (with semaphore behavior off, and signal() always signaling 1).
//
// A call to signal() causes an ongoing wait() to invoke its continuation.
// If no wait() is ongoing, the next wait() will continue immediately.
class reactor_notifier {
public:
    virtual future<> wait() = 0;
    virtual void signal() = 0;
    virtual ~reactor_notifier() {}
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
private:
    void work();
    void complete();
    void submit_item(work_item* wi);

    friend class thread_pool;
};

class smp_message_queue {
    static constexpr size_t queue_length = 128;
    static constexpr size_t batch_size = 16;
    static constexpr size_t prefetch_cnt = 2;
    struct work_item;
    using lf_queue = boost::lockfree::spsc_queue<work_item*,
                            boost::lockfree::capacity<queue_length>>;
    lf_queue _pending;
    lf_queue _completed;
    struct alignas(64) {
        size_t _sent = 0;
        size_t _compl = 0;
        size_t _last_snt_batch = 0;
        size_t _last_cmpl_batch = 0;
        size_t _current_queue_length = 0;
    };
    struct alignas(64) {
        size_t _received = 0;
        size_t _last_rcv_batch = 0;
    };
    std::vector<scollectd::registration> _collectd_regs;
    struct work_item {
        virtual ~work_item() {}
        virtual future<> process() = 0;
        virtual void complete() = 0;
    };
    template <typename Func, typename Future>
    struct async_work_item : work_item {
        Func _func;
        using value_type = typename Future::value_type;
        std::experimental::optional<value_type> _result;
        std::exception_ptr _ex; // if !_result
        typename Future::promise_type _promise; // used on local side
        async_work_item(Func&& func) : _func(std::move(func)) {}
        virtual future<> process() override {
            try {
                return this->_func().rescue([this] (auto&& get_result) {
                    try {
                        _result = get_result();
                    } catch (...) {
                        _ex = std::current_exception();
                    }
                });
            } catch (...) {
                _ex = std::current_exception();
                return make_ready_future();
            }
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
    union tx_side {
        tx_side() {}
        ~tx_side() {}
        void init() { new (&a) aa; }
        struct aa {
            std::deque<work_item*> pending_fifo;
        } a;
    } _tx;
    std::vector<work_item*> _completed_fifo;
public:
    smp_message_queue();
    template <typename Func>
    std::result_of_t<Func()> submit(Func func) {
        using future = std::result_of_t<Func()>;
        auto wi = new async_work_item<Func, future>(std::move(func));
        auto fut = wi->get_future();
        submit_item(wi);
        return fut;
    }
    void start(unsigned cpuid);
    template<size_t PrefetchCnt, typename Func>
    size_t process_queue(lf_queue& q, Func process);
    size_t process_incoming();
    size_t process_completions();
private:
    void work();
    void submit_item(work_item* wi);
    void respond(work_item* wi);
    void move_pending();
    void flush_request_batch();
    void flush_response_batch();

    friend class smp;
};

class thread_pool {
#ifndef HAVE_OSV
    // FIXME: implement using reactor_notifier abstraction we used for SMP
    syscall_work_queue inter_thread_wq;
    posix_thread _worker_thread;
    std::atomic<bool> _stopped = { false };
    pthread_t _notify;
public:
    thread_pool();
    ~thread_pool();
    template <typename T, typename Func>
    future<T> submit(Func func) {return inter_thread_wq.submit<T>(std::move(func));}
#else
public:
    template <typename T, typename Func>
    future<T> submit(Func func) { std::cout << "thread_pool not yet implemented on osv\n"; abort(); }
#endif
private:
    void work();
};

// The "reactor_backend" interface provides a method of waiting for various
// basic events on one thread. We have one implementation based on epoll and
// file-descriptors (reactor_backend_epoll) and one implementation based on
// OSv-specific file-descriptor-less mechanisms (reactor_backend_osv).
class reactor_backend {
public:
    virtual ~reactor_backend() {};
    // wait_and_process() waits for some events to become available, and
    // processes one or more of them. If block==false, it doesn't wait,
    // and just processes events that have already happened, if any.
    // After the optional wait, just before processing the events, the
    // pre_process() function is called.
    virtual bool wait_and_process() = 0;
    // Methods that allow polling on file descriptors. This will only work on
    // reactor_backend_epoll. Other reactor_backend will probably abort if
    // they are called (which is fine if no file descriptors are waited on):
    virtual future<> readable(pollable_fd_state& fd) = 0;
    virtual future<> writeable(pollable_fd_state& fd) = 0;
    virtual void forget(pollable_fd_state& fd) = 0;
    // Methods that allow polling on a reactor_notifier. This is currently
    // used only for reactor_backend_osv, but in the future it should really
    // replace the above functions.
    virtual future<> notified(reactor_notifier *n) = 0;
    // Methods for allowing sending notifications events between threads.
    virtual std::unique_ptr<reactor_notifier> make_reactor_notifier() = 0;
};

// reactor backend using file-descriptor & epoll, suitable for running on
// Linux. Can wait on multiple file descriptors, and converts other events
// (such as timers, signals, inter-thread notifications) into file descriptors
// using mechanisms like timerfd, signalfd and eventfd respectively.
class reactor_backend_epoll : public reactor_backend {
private:
    file_desc _epollfd;
    future<> get_epoll_future(pollable_fd_state& fd,
            promise<> pollable_fd_state::* pr, int event);
    void complete_epoll_event(pollable_fd_state& fd,
            promise<> pollable_fd_state::* pr, int events, int event);
public:
    reactor_backend_epoll();
    virtual ~reactor_backend_epoll() override { }
    virtual bool wait_and_process() override;
    virtual future<> readable(pollable_fd_state& fd) override;
    virtual future<> writeable(pollable_fd_state& fd) override;
    virtual void forget(pollable_fd_state& fd) override;
    virtual future<> notified(reactor_notifier *n) override;
    virtual std::unique_ptr<reactor_notifier> make_reactor_notifier() override;
};

#ifdef HAVE_OSV
// reactor_backend using OSv-specific features, without any file descriptors.
// This implementation cannot currently wait on file descriptors, but unlike
// reactor_backend_epoll it doesn't need file descriptors for waiting on a
// timer, for example, so file descriptors are not necessary.
class reactor_notifier_osv;
class reactor_backend_osv : public reactor_backend {
private:
    osv::newpoll::poller _poller;
    future<> get_poller_future(reactor_notifier_osv *n);
    promise<> _timer_promise;
public:
    reactor_backend_osv();
    virtual ~reactor_backend_osv() override { }
    virtual void wait_and_process() override;
    virtual future<> readable(pollable_fd_state& fd) override;
    virtual future<> writeable(pollable_fd_state& fd) override;
    virtual void forget(pollable_fd_state& fd) override;
    virtual future<> notified(reactor_notifier *n) override;
    virtual std::unique_ptr<reactor_notifier> make_reactor_notifier() override;
    friend class reactor_notifier_osv;
};
#endif /* HAVE_OSV */

class reactor {
private:
    struct pollfn {
        virtual ~pollfn() {}
        virtual bool poll_and_check_more_work() = 0;
    };

public:
    class poller {
        std::unique_ptr<pollfn> _pollfn;
        class registration_task;
        class deregistration_task;
        registration_task* _registration_task;
    public:
        template <typename Func> // signature: bool ()
        explicit poller(Func&& poll_and_check_more_work)
                : _pollfn(make_pollfn(std::forward<Func>(poll_and_check_more_work))) {
            do_register();
        }
        ~poller();
        poller(poller&& x);
        poller& operator=(poller&& x);
        void do_register();
        friend class reactor;
    };

private:
    // FIXME: make _backend a unique_ptr<reactor_backend>, not a compile-time #ifdef.
#ifdef HAVE_OSV
    reactor_backend_osv _backend;
#else
    reactor_backend_epoll _backend;
#endif
    std::vector<pollfn*> _pollers;
    static constexpr size_t max_aio = 128;
    promise<> _exit_promise;
    future<> _exit_future;
    unsigned _id = 0;
    bool _stopped = false;
    bool _handle_sigint = true;
    promise<std::unique_ptr<network_stack>> _network_stack_ready_promise;
    int _return = 0;
    timer_t _timer;
    promise<> _start_promise;
    semaphore _cpu_started;
    uint64_t _tasks_processed = 0;
    timer_set<timer<>, &timer<>::_link> _timers;
    timer_set<timer<>, &timer<>::_link>::timer_list_t _expired_timers;
    timer_set<timer<lowres_clock>, &timer<lowres_clock>::_link> _lowres_timers;
    timer_set<timer<lowres_clock>, &timer<lowres_clock>::_link>::timer_list_t _expired_lowres_timers;
    io_context_t _io_context;
    semaphore _io_context_available;
    circular_buffer<std::unique_ptr<task>> _pending_tasks;
    circular_buffer<std::unique_ptr<task>> _at_destroy_tasks;
    size_t _task_quota;
    std::unique_ptr<network_stack> _network_stack;
    // _lowres_clock will only be created on cpu 0
    std::unique_ptr<lowres_clock> _lowres_clock;
    lowres_clock::time_point _lowres_next_timeout;
    promise<> _lowres_timer_promise;
    promise<> _timer_promise;
    std::experimental::optional<poller> _epoll_poller;
    const bool _reuseport;
    circular_buffer<double> _loads;
    double _load = 0;
private:
    void abort_on_error(int ret);
    template <typename T, typename E>
    void complete_timers(T&, E&, std::function<future<> ()>, std::function<void ()>);

    /**
     * Returns TRUE if all pollers allow blocking.
     *
     * @return FALSE if at least one of the blockers requires a non-blocking
     *         execution.
     */
    bool poll_once();
    template <typename Func> // signature: bool ()
    static std::unique_ptr<pollfn> make_pollfn(Func&& func);

    struct signal_handler {
        signal_handler(int signo, std::function<void ()>&& handler);
        std::function<void ()> _handler;
    };
    std::atomic<uint64_t> _pending_signals;
    std::unordered_map<int, signal_handler> _signal_handlers;
    bool poll_signal();
    friend void sigaction(int signo, siginfo_t* siginfo, void* ignore);

    thread_pool _thread_pool;

    void run_tasks(circular_buffer<std::unique_ptr<task>>& tasks, size_t task_quota);
    bool posix_reuseport_detect();
public:
    static boost::program_options::options_description get_options_description();
    reactor();
    reactor(const reactor&) = delete;
    ~reactor() {
        auto eraser = [](auto& list) {
            while (!list.empty()) {
                auto timer = *list.begin();
                timer.cancel();
            }
        };
        eraser(_expired_timers);
        eraser(_expired_lowres_timers);
    }
    void operator=(const reactor&) = delete;

    void configure(boost::program_options::variables_map config);

    server_socket listen(socket_address sa, listen_options opts = {});

    future<connected_socket> connect(socket_address sa);

    pollable_fd posix_listen(socket_address sa, listen_options opts = {});

    bool posix_reuseport_available() const { return _reuseport; }

    future<pollable_fd> posix_connect(socket_address sa);

    future<pollable_fd, socket_address> accept(pollable_fd_state& listen_fd);

    future<size_t> read_some(pollable_fd_state& fd, void* buffer, size_t size);
    future<size_t> read_some(pollable_fd_state& fd, const std::vector<iovec>& iov);

    future<size_t> write_some(pollable_fd_state& fd, const void* buffer, size_t size);

    future<> write_all(pollable_fd_state& fd, const void* buffer, size_t size);

    future<file> open_file_dma(sstring name);
    future<file> open_directory(sstring name);

    template <typename Func>
    future<io_event> submit_io(Func prepare_io);

    void handle_signal(int signo, std::function<void ()>&& handler);

    int run();
    void exit(int ret);
    future<> when_started() { return _start_promise.get_future(); }

    template <typename Func>
    void at_exit(Func&& func) {
        _exit_future = _exit_future.then(std::forward<Func>(func));
    }

    template <typename Func>
    void at_destroy(Func&& func) {
        _at_destroy_tasks.push_back(make_task(std::forward<Func>(func)));
    }

    void add_task(std::unique_ptr<task>&& t) { _pending_tasks.push_back(std::move(t)); }

    network_stack& net() { return *_network_stack; }
    unsigned cpu_id() const { return _id; }

    void start_epoll() {
        if (!_epoll_poller) {
            _epoll_poller = poller([this] {
                return wait_and_process();
            });
        }
    }
private:
    /**
     * Add a new "poller" - a non-blocking function returning a boolean, that
     * will be called every iteration of a main loop.
     * If it returns FALSE then reactor's main loop is forbidden to block in the
     * current iteration.
     *
     * @param fn a new "poller" function to register
     */
    void register_poller(pollfn* p);
    void unregister_poller(pollfn* p);
    void replace_poller(pollfn* old, pollfn* neww);
    struct collectd_registrations;
    collectd_registrations register_collectd_metrics();
    future<> write_all_part(pollable_fd_state& fd, const void* buffer, size_t size, size_t completed);

    bool process_io();

    void add_timer(timer<>*);
    void del_timer(timer<>*);
    void add_timer(timer<lowres_clock>*);
    void del_timer(timer<lowres_clock>*);

    future<> run_exit_tasks();
    void stop();
    friend class pollable_fd;
    friend class pollable_fd_state;
    friend class posix_file_impl;
    friend class blockdev_file_impl;
    friend class readable_eventfd;
    friend class timer<>;
    friend class timer<lowres_clock>;
    friend class smp;
    friend class smp_message_queue;
    friend class poller;
public:
    bool wait_and_process() {
        return _backend.wait_and_process();
    }

    future<> readable(pollable_fd_state& fd) {
        return _backend.readable(fd);
    }
    future<> writeable(pollable_fd_state& fd) {
        return _backend.writeable(fd);
    }
    void forget(pollable_fd_state& fd) {
        _backend.forget(fd);
    }
    future<> notified(reactor_notifier *n) {
        return _backend.notified(n);
    }
    void enable_timer(clock_type::time_point when);
    future<> timers_completed() {
        return _timer_promise.get_future();
    }
    future<> lowres_timers_completed() {
        return _lowres_timer_promise.get_future();
    }
    std::unique_ptr<reactor_notifier> make_reactor_notifier() {
        return _backend.make_reactor_notifier();
    }
};

template <typename Func> // signature: bool ()
inline
std::unique_ptr<reactor::pollfn>
reactor::make_pollfn(Func&& func) {
    struct the_pollfn : pollfn {
        the_pollfn(Func&& func) : func(std::forward<Func>(func)) {}
        Func func;
        virtual bool poll_and_check_more_work() override {
            return func();
        }
    };
    return std::make_unique<the_pollfn>(std::forward<Func>(func));
}

extern __thread reactor* local_engine;
extern __thread size_t task_quota;

inline reactor& engine() {
    return *local_engine;
}

class smp {
#if HAVE_DPDK
    using thread_adaptor = std::function<void ()>;
#else
    using thread_adaptor = posix_thread;
#endif
    static std::vector<thread_adaptor> _threads;
    static smp_message_queue** _qs;
    static std::thread::id _tmain;

    template <typename Func>
    using returns_future = is_future<std::result_of_t<Func()>>;
    template <typename Func>
    using returns_void = std::is_same<std::result_of_t<Func()>, void>;
public:
    static boost::program_options::options_description get_options_description();
    static void configure(boost::program_options::variables_map vm);
    static void join_all();
    static bool main_thread() { return std::this_thread::get_id() == _tmain; }

    template <typename Func>
    static std::result_of_t<Func()> submit_to(unsigned t, Func func,
            std::enable_if_t<returns_future<Func>::value, void*> = nullptr) {
        if (t == engine().cpu_id()) {
            return func();
        } else {
            return _qs[t][engine().cpu_id()].submit(std::move(func));
        }
    }
    template <typename Func>
    static future<std::result_of_t<Func()>> submit_to(unsigned t, Func func,
            std::enable_if_t<!returns_future<Func>::value && !returns_void<Func>::value, void*> = nullptr) {
        return submit_to(t, [func = std::move(func)] () mutable {
           return make_ready_future<std::result_of_t<Func()>>(func());
        });
    }
    template <typename Func>
    static future<> submit_to(unsigned t, Func func,
            std::enable_if_t<!returns_future<Func>::value && returns_void<Func>::value, void*> = nullptr) {
        return submit_to(t, [func = std::move(func)] () mutable {
            func();
            return make_ready_future<>();
        });
    }
    static bool poll_queues() {
        size_t got = 0;
        for (unsigned i = 0; i < count; i++) {
            if (engine().cpu_id() != i) {
                auto& rxq = _qs[engine().cpu_id()][i];
                rxq.flush_response_batch();
                got += rxq.process_incoming();
                auto& txq = _qs[i][engine()._id];
                txq.flush_request_batch();
                got += txq.process_completions();
            }
        }
        return got != 0;
    }
private:
    static void start_all_queues();
    static void pin(unsigned cpu_id);
    static void allocate_reactor();
public:
    static unsigned count;
};

inline
pollable_fd_state::~pollable_fd_state() {
    engine().forget(*this);
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
    virtual future<> put(net::packet data) = 0;
    virtual future<> put(std::vector<temporary_buffer<char>> data) {
        net::packet p;
        p.reserve(data.size());
        for (auto& buf : data) {
            p = net::packet(std::move(p), net::fragment{buf.get_write(), buf.size()}, buf.release());
        }
        return put(std::move(p));
    }
    virtual future<> put(temporary_buffer<char> buf) {
        return put(net::packet(net::fragment{buf.get_write(), buf.size()}, buf.release()));
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
    future<> put(net::packet p) {
        return _dsi->put(std::move(p));
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

// Facilitates data buffering before it's handed over to data_sink.
//
// When trim_to_size is true it's guaranteed that data sink will not receive
// chunks larger than the configured size, which could be the case when a
// single write call is made with data larger than the configured size.
//
// The data sink will not receive empty chunks.
//
template <typename CharType>
class output_stream {
    static_assert(sizeof(CharType) == 1, "must buffer stream of bytes");
    data_sink _fd;
    temporary_buffer<CharType> _buf;
    size_t _size;
    size_t _begin = 0;
    size_t _end = 0;
    bool _trim_to_size;
private:
    size_t available() const { return _end - _begin; }
    size_t possibly_available() const { return _size - _begin; }
    future<> split_and_put(temporary_buffer<CharType> buf);
public:
    using char_type = CharType;
    output_stream(data_sink fd, size_t size, bool trim_to_size = false)
        : _fd(std::move(fd)), _size(size), _trim_to_size(trim_to_size) {}
    future<> write(const char_type* buf, size_t n);
    future<> write(const char_type* buf);
    future<> write(const sstring& s);
    future<> write(net::packet p);
    future<> write(scattered_message<char_type> msg);
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

template<typename CharType>
future<> output_stream<CharType>::write(scattered_message<CharType> msg) {
    return write(std::move(msg).release());
}

template<typename CharType>
future<> output_stream<CharType>::write(net::packet p) {
    static_assert(std::is_same<CharType, char>::value, "packet works on char");

    if (p.len() == 0) {
        return make_ready_future<>();
    }

    assert(!_end && "Mixing buffered writes and zero-copy writes not supported yet");

    if (!_trim_to_size || p.len() <= _size) {
        // TODO: aggregate buffers for later coalescing. Currently we flush right
        // after appending the message anyway, so it doesn't matter.
        return _fd.put(std::move(p));
    }

    auto head = p.share(0, _size);
    p.trim_front(_size);
    return _fd.put(std::move(head)).then([this, p = std::move(p)] () mutable {
        return write(std::move(p));
    });
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
future<>
reactor::write_all_part(pollable_fd_state& fd, const void* buffer, size_t len, size_t completed) {
    if (completed == len) {
        return make_ready_future<>();
    } else {
        return write_some(fd, static_cast<const char*>(buffer) + completed, len - completed).then(
                [&fd, buffer, len, completed, this] (size_t part) mutable {
            return write_all_part(fd, buffer, len, completed + part);
        });
    }
}

inline
future<>
reactor::write_all(pollable_fd_state& fd, const void* buffer, size_t len) {
    assert(len);
    return write_all_part(fd, buffer, len, 0);
}

template <typename T, typename E>
void reactor::complete_timers(T& timers, E& expired_timers,
                              std::function<future<> ()> completed_fn,
                              std::function<void ()> enable_fn) {
    completed_fn().then([this, &timers, &expired_timers, completed_fn,
            enable_fn = std::move(enable_fn)] () mutable {
        expired_timers = timers.expire(timers.now());
        for (auto& t : expired_timers) {
            t._expired = true;
        }
        while (!expired_timers.empty()) {
            auto t = &*expired_timers.begin();
            expired_timers.pop_front();
            t->_queued = false;
            if (t->_armed) {
                t->_armed = false;
                if (t->_period) {
                    t->arm_periodic(*t->_period);
                }
                t->_callback();
            }
        }
        enable_fn();
        complete_timers(timers, expired_timers, std::move(completed_fn), std::move(enable_fn));
    });
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream<CharType>::read_exactly_part(size_t n, tmp_buf out, size_t completed) {
    if (available()) {
        auto now = std::min(n - completed, available());
        std::copy(_buf.get(), _buf.get() + now, out.get_write() + completed);
        _buf.trim_front(now);
        completed += now;
    }
    if (completed == n) {
        return make_ready_future<tmp_buf>(std::move(out));
    }

    // _buf is now empty
    return _fd.get().then([this, n, out = std::move(out), completed] (auto buf) mutable {
        if (buf.size() == 0) {
            return make_ready_future<tmp_buf>(std::move(buf));
        }
        _buf = std::move(buf);
        return this->read_exactly_part(n, std::move(out), completed);
    });
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream<CharType>::read_exactly(size_t n) {
    if (_buf.size() == n) {
        // easy case: steal buffer, return to caller
        return make_ready_future<tmp_buf>(std::move(_buf));
    } else if (_buf.size() > n) {
        // buffer large enough, share it with caller
        auto front = _buf.share(0, n);
        _buf.trim_front(n);
        return make_ready_future<tmp_buf>(std::move(front));
    } else if (_buf.size() == 0) {
        // buffer is empty: grab one and retry
        return _fd.get().then([this, n] (auto buf) mutable {
            if (buf.size() == 0) {
                return make_ready_future<tmp_buf>(std::move(buf));
            }
            _buf = std::move(buf);
            return this->read_exactly(n);
        });
    } else {
        // buffer too small: start copy/read loop
        tmp_buf b(n);
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

// Writes @buf in chunks of _size length. The last chunk is buffered if smaller.
template <typename CharType>
future<>
output_stream<CharType>::split_and_put(temporary_buffer<CharType> buf) {
    assert(_end == 0);

    if (buf.size() < _size) {
        if (!_buf) {
            _buf = temporary_buffer<char>(_size);
        }
        std::copy(buf.get(), buf.get() + buf.size(), _buf.get_write());
        _end = buf.size();
        return make_ready_future<>();
    }

    auto chunk = buf.share(0, _size);
    buf.trim_front(_size);
    return _fd.put(std::move(chunk)).then([this, buf = std::move(buf)] () mutable {
        return split_and_put(std::move(buf));
    });
}

template <typename CharType>
future<>
output_stream<CharType>::write(const char_type* buf, size_t n) {
    auto bulk_threshold = _end ? (2 * _size - _end) : _size;
    if (n >= bulk_threshold) {
        if (_end) {
            auto now = _size - _end;
            std::copy(buf, buf + now, _buf.get_write() + _end);
            _end = _size;
            temporary_buffer<char> tmp(n - now);
            std::copy(buf + now, buf + n, tmp.get_write());
            return flush().then([this, tmp = std::move(tmp)]() mutable {
                if (_trim_to_size) {
                    return split_and_put(std::move(tmp));
                } else {
                    return _fd.put(std::move(tmp));
                }
            });
        } else {
            temporary_buffer<char> tmp(n);
            std::copy(buf, buf + n, tmp.get_write());
            if (_trim_to_size) {
                return split_and_put(std::move(tmp));
            } else {
                return _fd.put(std::move(tmp));
            }
        }
    }

    if (!_buf) {
        _buf = temporary_buffer<char>(_size);
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
    _end = 0;
    return _fd.put(std::move(_buf));
}

inline
future<size_t> pollable_fd::read_some(char* buffer, size_t size) {
    return engine().read_some(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::read_some(uint8_t* buffer, size_t size) {
    return engine().read_some(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::read_some(const std::vector<iovec>& iov) {
    return engine().read_some(*_s, iov);
}

inline
future<> pollable_fd::write_all(const char* buffer, size_t size) {
    return engine().write_all(*_s, buffer, size);
}

inline
future<> pollable_fd::write_all(const uint8_t* buffer, size_t size) {
    return engine().write_all(*_s, buffer, size);
}

inline
future<size_t> pollable_fd::write_some(net::packet& p) {
    return engine().writeable(*_s).then([this, &p] () mutable {
        static_assert(offsetof(iovec, iov_base) == offsetof(net::fragment, base) &&
            sizeof(iovec::iov_base) == sizeof(net::fragment::base) &&
            offsetof(iovec, iov_len) == offsetof(net::fragment, size) &&
            sizeof(iovec::iov_len) == sizeof(net::fragment::size) &&
            alignof(iovec) == alignof(net::fragment) &&
            sizeof(iovec) == sizeof(net::fragment)
            , "net::fragment and iovec should be equivalent");

        iovec* iov = reinterpret_cast<iovec*>(p.fragment_array());
        auto r = get_file_desc().writev(iov, p.nr_frags());
        if (!r) {
            return write_some(p);
        }
        if (size_t(*r) == p.len()) {
            _s->speculate_epoll(EPOLLOUT);
        }
        return make_ready_future<size_t>(*r);
    });
}

inline
future<> pollable_fd::write_all(net::packet& p) {
    return write_some(p).then([this, &p] (size_t size) {
        if (p.len() == size) {
            return make_ready_future<>();
        }
        p.trim_front(size);
        return write_all(p);
    });
}

inline
future<> pollable_fd::readable() {
    return engine().readable(*_s);
}

inline
future<> pollable_fd::writeable() {
    return engine().writeable(*_s);
}

inline
future<pollable_fd, socket_address> pollable_fd::accept() {
    return engine().accept(*_s);
}

inline
future<size_t> pollable_fd::recvmsg(struct msghdr *msg) {
    return engine().readable(*_s).then([this, msg] {
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
    return engine().writeable(*_s).then([this, msg] () mutable {
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
    return engine().writeable(*_s).then([this, buf, len, addr] () mutable {
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

template <typename Clock>
inline
timer<Clock>::timer(callback_t&& callback) : _callback(std::move(callback)) {
}

template <typename Clock>
inline
timer<Clock>::~timer() {
    if (_queued) {
        engine().del_timer(this);
    }
}

template <typename Clock>
inline
void timer<Clock>::set_callback(callback_t&& callback) {
    _callback = std::move(callback);
}

template <typename Clock>
inline
void timer<Clock>::arm(time_point until, boost::optional<duration> period) {
    assert(!_armed);
    _period = period;
    _armed = true;
    _expired = false;
    _expiry = until;
    engine().add_timer(this);
    _queued = true;
}

template <typename Clock>
inline
void timer<Clock>::rearm(time_point until, boost::optional<duration> period) {
    if (_armed) {
        cancel();
    }
    arm(until, period);
}

template <typename Clock>
inline
void timer<Clock>::arm(duration delta) {
    return arm(Clock::now() + delta);
}

template <typename Clock>
inline
void timer<Clock>::arm_periodic(duration delta) {
    arm(Clock::now() + delta, {delta});
}

template <typename Clock>
inline
bool timer<Clock>::cancel() {
    if (!_armed) {
        return false;
    }
    _armed = false;
    if (_queued) {
        engine().del_timer(this);
        _queued = false;
    }
    return true;
}

template <typename Clock>
inline
typename timer<Clock>::time_point timer<Clock>::get_timeout() {
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
