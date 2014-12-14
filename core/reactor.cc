/*
 * Copyright 2014 Cloudius Systems
 */

#include "reactor.hh"
#include "memory.hh"
#include "core/posix.hh"
#include "net/packet.hh"
#include "resource.hh"
#include "print.hh"
#include "scollectd.hh"
#include "util/conversions.hh"
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <boost/thread/barrier.hpp>

#ifdef HAVE_OSV
#include <osv/newpoll.hh>
#endif

using namespace net;

timespec to_timespec(clock_type::time_point t) {
    using ns = std::chrono::nanoseconds;
    auto n = std::chrono::duration_cast<ns>(t.time_since_epoch()).count();
    return { n / 1'000'000'000, n % 1'000'000'000 };
}

template <typename T>
struct syscall_result {
    T result;
    int error;
    void throw_if_error() {
        if (long(result) == -1) {
            throw std::system_error(error, std::system_category());
        }
    }
};

template <typename T>
syscall_result<T>
wrap_syscall(T result) {
    syscall_result<T> sr;
    sr.result = result;
    sr.error = errno;
    return sr;
}

reactor_backend_epoll::reactor_backend_epoll()
    : _epollfd(file_desc::epoll_create(EPOLL_CLOEXEC)) {
}

reactor::reactor()
    : _backend()
    , _exit_future(_exit_promise.get_future())
    ,  _idle(false)
    , _cpu_started(0)
    , _io_eventfd()
    , _io_context(0)
    , _io_context_available(max_aio) {
    auto r = ::io_setup(max_aio, &_io_context);
    assert(r >= 0);
    memory::set_reclaim_hook([this] (std::function<void ()> reclaim_fn) {
        // push it in the front of the queue so we reclaim memory quickly
        _pending_tasks.push_front(make_task([fn = std::move(reclaim_fn)] {
            fn();
        }));
    });
}

void reactor::configure(boost::program_options::variables_map vm) {
    auto network_stack_ready = vm.count("network-stack")
        ? network_stack_registry::create(sstring(vm["network-stack"].as<std::string>()), vm)
        : network_stack_registry::create(vm);
    network_stack_ready.then([this] (std::unique_ptr<network_stack> stack) {
        _network_stack_ready_promise.set_value(std::move(stack));
    });

    _handle_sigint = !vm.count("no-handle-interrupt");
    _task_quota = vm["task-quota"].as<int>();
    _poll = vm.count("poll");
}

future<> reactor_backend_epoll::get_epoll_future(pollable_fd_state& pfd,
        promise<> pollable_fd_state::*pr, int event) {
    if (pfd.events_known & event) {
        pfd.events_known &= ~event;
        return make_ready_future();
    }
    pfd.events_requested |= event;
    if (!(pfd.events_epoll & event)) {
        auto ctl = pfd.events_epoll ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        pfd.events_epoll |= event;
        ::epoll_event eevt;
        eevt.events = pfd.events_epoll;
        eevt.data.ptr = &pfd;
        int r = ::epoll_ctl(_epollfd.get(), ctl, pfd.fd.get(), &eevt);
        assert(r == 0);
    }
    pfd.*pr = promise<>();
    return (pfd.*pr).get_future();
}

future<> reactor_backend_epoll::readable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollin, EPOLLIN);
}

future<> reactor_backend_epoll::writeable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollout, EPOLLOUT);
}

void reactor_backend_epoll::forget(pollable_fd_state& fd) {
    if (fd.events_epoll) {
        ::epoll_ctl(_epollfd.get(), EPOLL_CTL_DEL, fd.fd.get(), nullptr);
    }
}

future<> reactor_backend_epoll::notified(reactor_notifier *n) {
    // Currently reactor_backend_epoll doesn't need to support notifiers,
    // because we add to it file descriptors instead. But this can be fixed
    // later.
    std::cout << "reactor_backend_epoll does not yet support notifiers!\n";
    abort();
}


pollable_fd
reactor::posix_listen(socket_address sa, listen_options opts) {
    file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (opts.reuse_address) {
        fd.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1);
    }

    fd.bind(sa.u.sa, sizeof(sa.u.sas));
    fd.listen(100);
    return pollable_fd(std::move(fd));
}

server_socket
reactor::listen(socket_address sa, listen_options opt) {
    return _network_stack->listen(sa, opt);
}

void reactor_backend_epoll::complete_epoll_event(pollable_fd_state& pfd, promise<> pollable_fd_state::*pr,
        int events, int event) {
    if (pfd.events_requested & events & event) {
        pfd.events_requested &= ~event;
        pfd.events_known &= ~event;
        (pfd.*pr).set_value();
        pfd.*pr = promise<>();
    }
}

template <typename Func>
future<io_event>
reactor::submit_io(Func prepare_io) {
    return _io_context_available.wait(1).then([this, prepare_io = std::move(prepare_io)] () mutable {
        auto pr = std::make_unique<promise<io_event>>();
        iocb io;
        prepare_io(io);
        io.data = pr.get();
        io_set_eventfd(&io, _io_eventfd.get_write_fd());
        iocb* p = &io;
        auto r = ::io_submit(_io_context, 1, &p);
        throw_kernel_error(r);
        return pr.release()->get_future();
    });
}

void reactor::process_io(size_t count)
{
    io_event ev[max_aio];
    auto n = ::io_getevents(_io_context, count, count, ev, NULL);
    assert(n >= 0 && size_t(n) == count);
    for (size_t i = 0; i < size_t(n); ++i) {
        auto pr = reinterpret_cast<promise<io_event>*>(ev[i].data);
        pr->set_value(ev[i]);
        delete pr;
    }
    _io_context_available.signal(n);
    _io_eventfd.wait().then([this] (size_t count) {
        process_io(count);
    });
}

future<size_t>
posix_file_impl::write_dma(uint64_t pos, const void* buffer, size_t len) {
    return engine.submit_io([this, pos, buffer, len] (iocb& io) {
        io_prep_pwrite(&io, _fd, const_cast<void*>(buffer), len, pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
posix_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov) {
    return engine.submit_io([this, pos, iov = std::move(iov)] (iocb& io) {
        io_prep_pwritev(&io, _fd, iov.data(), iov.size(), pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
posix_file_impl::read_dma(uint64_t pos, void* buffer, size_t len) {
    return engine.submit_io([this, pos, buffer, len] (iocb& io) {
        io_prep_pread(&io, _fd, buffer, len, pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
posix_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov) {
    return engine.submit_io([this, pos, iov = std::move(iov)] (iocb& io) {
        io_prep_preadv(&io, _fd, iov.data(), iov.size(), pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<file>
reactor::open_file_dma(sstring name) {
    return _thread_pool.submit<syscall_result<int>>([name] {
        return wrap_syscall<int>(::open(name.c_str(), O_DIRECT | O_CLOEXEC | O_CREAT | O_RDWR, S_IRWXU));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<file>(file(sr.result));
    });
}

future<>
posix_file_impl::flush(void) {
    return engine._thread_pool.submit<syscall_result<int>>([this] {
        return wrap_syscall<int>(::fsync(_fd));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

future<struct stat>
posix_file_impl::stat(void) {
    return engine._thread_pool.submit<struct stat>([this] {
        struct stat st;
        auto ret = ::fstat(_fd, &st);
        throw_system_error_on(ret == -1);
        return (st);
    });
}

future<>
posix_file_impl::discard(uint64_t offset, uint64_t length) {
    return engine._thread_pool.submit<syscall_result<int>>([this, offset, length] () mutable {
        return wrap_syscall<int>(::fallocate(_fd, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE,
            offset, length));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

future<>
blockdev_file_impl::discard(uint64_t offset, uint64_t length) {
    return engine._thread_pool.submit<syscall_result<int>>([this, offset, length] () mutable {
        uint64_t range[2] { offset, length };
        return wrap_syscall<int>(::ioctl(_fd, BLKDISCARD, &range));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

future<size_t>
posix_file_impl::size(void) {
    return engine._thread_pool.submit<size_t>([this] {
        struct stat st;
        auto ret = ::fstat(_fd, &st);
        throw_system_error_on(ret == -1);
        return st.st_size;
    });
}

future<size_t>
blockdev_file_impl::size(void) {
    return engine._thread_pool.submit<size_t>([this] {
        size_t size;
        auto ret = ::ioctl(_fd, BLKGETSIZE64, &size);
        throw_system_error_on(ret == -1);
        return size;
    });
}

void reactor_backend_epoll::enable_timer(clock_type::time_point when)
{
    itimerspec its;
    its.it_interval = {};
    its.it_value = to_timespec(when);
    _timerfd.get_file_desc().timerfd_settime(TFD_TIMER_ABSTIME, its);
}

void reactor::add_timer(timer* tmr) {
    if (_timers.insert(*tmr)) {
        enable_timer(_timers.get_next_timeout());
    }
}

void reactor::del_timer(timer* tmr) {
    if (tmr->_expired) {
        _expired_timers.erase(_expired_timers.iterator_to(*tmr));
        tmr->_expired = false;
    } else {
        _timers.remove(*tmr);
    }
}

future<> reactor_backend_epoll::timers_completed() {
    // TODO: The following idiom converts a future<size_t>, to a future<>.
    // Is there a more efficient way? Or maybe need a variant of read_some()
    // that returns future<>? It would also be nice to have a variant of
    // read_some<> that reads the data into temporary storage (inside the
    // future), so we don't need the "_timers_completed" variable here.
    return _timerfd.read_some(
                reinterpret_cast<char*>(&_timers_completed),
                sizeof(_timers_completed))
            .then([] (size_t ignore) { return make_ready_future<>(); });
}

void reactor::complete_timers() {
    timers_completed().then(
            [this] () {
        _expired_timers = _timers.expire(clock_type::now());
        for (auto& t : _expired_timers) {
            t._expired = true;
        }
        while (!_expired_timers.empty()) {
            auto t = &*_expired_timers.begin();
            _expired_timers.pop_front();
            t->_queued = false;
            if (t->_armed) {
                t->_armed = false;
                if (t->_period) {
                    t->arm_periodic(*t->_period);
                }
                t->_callback();
            }
        }
        if (!_timers.empty()) {
            enable_timer(_timers.get_next_timeout());
        }
        complete_timers();
    });
}

future<> reactor::run_exit_tasks() {
    _exit_promise.set_value();
    return std::move(_exit_future);
}

void reactor::stop() {
    assert(engine._id == 0);
    run_exit_tasks().then([this] {
        auto sem = new semaphore(0);
        for (unsigned i = 1; i < smp::count; i++) {
            smp::submit_to<>(i, []() {
                return engine.run_exit_tasks().then([] {
                        engine._stopped = true;
                });
            }).then([sem, i]() {
                sem->signal();
            });
        }
        sem->wait(smp::count - 1).then([sem, this](){
            _stopped = true;
            delete sem;
        });
    });
}

void reactor::exit(int ret) {
    smp::submit_to(0, [this, ret] { _return = ret; stop(); });
}

struct reactor::collectd_registrations {
    std::vector<scollectd::registration> regs;
};

reactor::collectd_registrations
reactor::register_collectd_metrics() {
    std::vector<scollectd::registration> regs = {
            // queue_length     value:GAUGE:0:U
            // Absolute value of num tasks in queue.
            scollectd::add_polled_metric(scollectd::type_instance_id("reactor"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "tasks-pending")
                    , scollectd::make_typed(scollectd::data_type::GAUGE
                            , std::bind(&decltype(_pending_tasks)::size, &_pending_tasks))
            ),
            // total_operations value:DERIVE:0:U
            scollectd::add_polled_metric(scollectd::type_instance_id("reactor"
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "tasks-processed")
                    , scollectd::make_typed(scollectd::data_type::DERIVE, _tasks_processed)
            ),
            // queue_length     value:GAUGE:0:U
            // Absolute value of num timers in queue.
            scollectd::add_polled_metric(scollectd::type_instance_id("reactor"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "timers-pending")
                    , scollectd::make_typed(scollectd::data_type::GAUGE
                            , std::bind(&decltype(_timers)::size, &_timers))
            ),
            scollectd::add_polled_metric(
                scollectd::type_instance_id("memory",
                    scollectd::per_cpu_plugin_instance,
                    "total_operations", "malloc"),
                scollectd::make_typed(scollectd::data_type::DERIVE,
                        [] { return memory::stats().mallocs(); })
            ),
            scollectd::add_polled_metric(
                scollectd::type_instance_id("memory",
                    scollectd::per_cpu_plugin_instance,
                    "total_operations", "free"),
                scollectd::make_typed(scollectd::data_type::DERIVE,
                        [] { return memory::stats().frees(); })
            ),
            scollectd::add_polled_metric(
                scollectd::type_instance_id("memory",
                    scollectd::per_cpu_plugin_instance,
                    "objects", "malloc"),
                scollectd::make_typed(scollectd::data_type::GAUGE,
                        [] { return memory::stats().live_objects(); })
            ),
    };
    return { regs };
}

int reactor::run() {
    auto collectd_metrics = register_collectd_metrics();
#ifndef HAVE_OSV
    _io_eventfd.wait().then([this] (size_t count) {
        process_io(count);
    });
#endif
    if (_handle_sigint && _id == 0) {
        receive_signal(SIGINT).then([this] { stop(); });
    }
    _cpu_started.wait(smp::count).then([this] {
            _start_promise.set_value();
    });
    _network_stack_ready_promise.get_future().then([this] (std::unique_ptr<network_stack> stack) {
        _network_stack = std::move(stack);
        return _network_stack->initialize();
    }).then([this] {
        for (unsigned c = 0; c < smp::count; c++) {
            smp::submit_to(c, [] {
                    engine._cpu_started.signal();
            });
        }
    });

    // Register smp queues poller
    std::experimental::optional<poller> smp_poller;
    if (smp::count > 1) {
        smp_poller = poller(smp::poll_queues);
    }

    std::function<void ()> update_idle = [this] {
        if (_pending_tasks.empty()) {
            _idle.store(false, std::memory_order_relaxed);
        }
        // else, _idle already false, no need to set it again and dirty
        // cache line.
    };

    complete_timers();
    while (true) {
        task_quota = _task_quota;
        while (!_pending_tasks.empty() && task_quota) {
            --task_quota;
            auto tsk = std::move(_pending_tasks.front());
            _pending_tasks.pop_front();
            tsk->run();
            tsk.reset();
            ++_tasks_processed;
        }
        if (_stopped) {
            if (_id == 0) {
                smp::join_all();
            }
            break;
        }

        if (!poll_once() && !_poll) {
            if (_pending_tasks.empty()) {
                _idle.store(true, std::memory_order_seq_cst);

                if (poll_once()) {
                    _idle.store(false, std::memory_order_relaxed);
                } else {
                    assert(_pending_tasks.empty());
                }
            }
        }

        wait_and_process(_idle.load(std::memory_order_relaxed), update_idle);
    }
    return _return;
}

bool
reactor::poll_once() {
    bool work = false;
    for (auto c : _pollers) {
        work |= c->_poll_and_check_more_work();
    }

    return work;
}

void reactor::register_poller(poller* p) {
    _pollers.push_back(p);
}

void reactor::unregister_poller(poller* p) {
    _pollers.erase(std::find(_pollers.begin(), _pollers.end(), p));
}

reactor::poller::poller(std::function<bool ()> poll_and_check_more_work)
        : _poll_and_check_more_work(poll_and_check_more_work) {
    engine.register_poller(this);
}

reactor::poller::~poller() {
    if (_poll_and_check_more_work) {
        engine.unregister_poller(this);
    }
}

reactor::poller::poller(poller&& x) {
    if (x._poll_and_check_more_work) {
        engine.unregister_poller(&x);
        _poll_and_check_more_work = std::move(x._poll_and_check_more_work);
        engine.register_poller(this);
    }
}

reactor::poller&
reactor::poller::operator=(poller&& x) {
    if (this != &x) {
        this->~poller();
        new (this) poller(std::move(x));
    }
    return *this;
}

void
reactor_backend_epoll::wait_and_process(bool block, std::function<void()>& pre_process) {
    std::array<epoll_event, 128> eevt;
    int nr = ::epoll_wait(_epollfd.get(), eevt.data(), eevt.size(),
            block ? -1 : 0);
    pre_process();
    if (nr == -1 && errno == EINTR) {
        return; // gdb can cause this
    }
    assert(nr != -1);
    for (int i = 0; i < nr; ++i) {
        auto& evt = eevt[i];
        auto pfd = reinterpret_cast<pollable_fd_state*>(evt.data.ptr);
        auto events = evt.events & (EPOLLIN | EPOLLOUT);
        // FIXME: it is enough to check that pfd's task is not in _pending_tasks here
        if (block) {
            pfd->events_known |= events;
        }
        auto events_to_remove = events & ~pfd->events_requested;
        complete_epoll_event(*pfd, &pollable_fd_state::pollin, events, EPOLLIN);
        complete_epoll_event(*pfd, &pollable_fd_state::pollout, events, EPOLLOUT);
        if (events_to_remove) {
            pfd->events_epoll &= ~events_to_remove;
            evt.events = pfd->events_epoll;
            auto op = evt.events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
            ::epoll_ctl(_epollfd.get(), op, pfd->fd.get(), &evt);
        }
    }
}

future<>
reactor_backend_epoll::receive_signal(int signo) {
    auto i = _signal_handlers.emplace(signo, signo).first;
    signal_handler& sh = i->second;
    return sh._signalfd.read_some(reinterpret_cast<char*>(&sh._siginfo), sizeof(sh._siginfo)).then([&sh] (size_t ignore) {
        sh._promise.set_value();
        sh._promise = promise<>();
        return make_ready_future<>();
    });
}

reactor_backend_epoll::signal_handler::signal_handler(int signo)
    : _signalfd(file_desc::signalfd(make_sigset_mask(signo), SFD_CLOEXEC | SFD_NONBLOCK)) {
    auto mask = make_sigset_mask(signo);
    auto r = ::sigprocmask(SIG_BLOCK, &mask, NULL);
    throw_system_error_on(r == -1);
}

syscall_work_queue::syscall_work_queue()
    : _pending()
    , _completed()
    , _start_eventfd(0)
    , _complete_eventfd(0) {
}

void syscall_work_queue::submit_item(syscall_work_queue::work_item* item) {
    _queue_has_room.wait().then([this, item] {
        _pending.push(item);
        _start_eventfd.signal(1);
    });
}

void syscall_work_queue::complete() {
    _complete_eventfd.wait().then([this] (size_t count) {
        auto nr = _completed.consume_all([this] (work_item* wi) {
            wi->complete();
            delete wi;
        });
        _queue_has_room.signal(nr);
        complete();
    });
}

smp_message_queue::smp_message_queue()
    : _pending()
    , _completed()
    , _start_event(engine.make_reactor_notifier())
    , _complete_event(engine.make_reactor_notifier())
{
}

void smp_message_queue::submit_kick() {
    if (!_complete_peer->_poll && _pending_peer->idle()) {
        _start_event->signal();
    }
}

void smp_message_queue::complete_kick() {
    if (!_pending_peer->_poll && _complete_peer->idle()) {
        _complete_event->signal();
    }
}

void smp_message_queue::move_pending() {
    auto queue_room = queue_length - _current_queue_length;
    auto nr = std::min(queue_room, _tx.a.pending_fifo.size());
    if (!nr) {
        return;
    }
    auto begin = _tx.a.pending_fifo.begin();
    auto end = begin + nr;
    _pending.push(begin, end);
    _tx.a.pending_fifo.erase(begin, end);
    _current_queue_length += nr;
    submit_kick();
}

void smp_message_queue::submit_item(smp_message_queue::work_item* item) {
    _tx.a.pending_fifo.push_back(item);
    if (_tx.a.pending_fifo.size() >= batch_size) {
        move_pending();
    }
}

void smp_message_queue::respond(work_item* item) {
    _completed_fifo.push_back(item);
    if (_completed_fifo.size() >= batch_size) {
        flush_response_batch();
    }
}

void smp_message_queue::flush_response_batch() {
    _completed.push(_completed_fifo.begin(), _completed_fifo.end());
    _completed_fifo.clear();
    complete_kick();
}

size_t smp_message_queue::process_completions() {
    // copy batch to local memory in order to minimize
    // time in which cross-cpu data is accessed
    work_item* items[queue_length];
    auto nr = _completed.pop(items);
    for (unsigned i = 0; i < nr; ++i) {
        items[i]->complete();
        delete items[i];
    }

    _current_queue_length -= nr;

    return nr;
}

void smp_message_queue::flush_request_batch() {
    move_pending();
}

void smp_message_queue::complete() {
    _complete_event->wait().then([this] {
        process_completions();
        complete();
    });
}

size_t smp_message_queue::process_incoming() {
    work_item* items[queue_length];
    auto nr = _pending.pop(items);
    for (unsigned i = 0; i < nr; ++i) {
        auto wi = items[i];
        wi->process().then([this, wi] {
            respond(wi);
        });
    }
    return nr;
}

void smp_message_queue::start() {
    _tx.init();
    _complete_peer = &engine;
    complete();
}

void smp_message_queue::listen() {
    _start_event->wait().then([this] () mutable {
        process_incoming();
        listen();
    });
}

/* not yet implemented for OSv. TODO: do the notification like we do class smp. */
#ifndef HAVE_OSV
void thread_pool::work() {
    sigset_t mask;
    sigfillset(&mask);
    auto r = ::sigprocmask(SIG_BLOCK, &mask, NULL);
    throw_system_error_on(r == -1);
    while (true) {
        uint64_t count;
        auto r = ::read(inter_thread_wq._start_eventfd.get_read_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
        if (_stopped.load(std::memory_order_relaxed)) {
            break;
        }
        auto nr = inter_thread_wq._pending.consume_all([this] (syscall_work_queue::work_item* wi) {
            wi->process();
            inter_thread_wq._completed.push(wi);
        });
        count = nr;
        r = ::write(inter_thread_wq._complete_eventfd.get_write_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
    }
}

thread_pool::~thread_pool() {
    _stopped.store(true, std::memory_order_relaxed);
    inter_thread_wq._start_eventfd.signal(1);
    _worker_thread.join();
}
#endif

readable_eventfd writeable_eventfd::read_side() {
    return readable_eventfd(_fd.dup());
}

file_desc writeable_eventfd::try_create_eventfd(size_t initial) {
    assert(size_t(int(initial)) == initial);
    return file_desc::eventfd(initial, EFD_CLOEXEC);
}

void writeable_eventfd::signal(size_t count) {
    uint64_t c = count;
    auto r = _fd.write(&c, sizeof(c));
    assert(r == sizeof(c));
}

writeable_eventfd readable_eventfd::write_side() {
    return writeable_eventfd(_fd.get_file_desc().dup());
}

file_desc readable_eventfd::try_create_eventfd(size_t initial) {
    assert(size_t(int(initial)) == initial);
    return file_desc::eventfd(initial, EFD_CLOEXEC | EFD_NONBLOCK);
}

future<size_t> readable_eventfd::wait() {
    return engine.readable(*_fd._s).then([this] {
        uint64_t count;
        int r = ::read(_fd.get_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
        return make_ready_future<size_t>(count);
    });
}

void schedule(std::unique_ptr<task> t) {
    engine.add_task(std::move(t));
}

bool operator==(const ::sockaddr_in a, const ::sockaddr_in b) {
    return (a.sin_addr.s_addr == b.sin_addr.s_addr) && (a.sin_port == b.sin_port);
}

void network_stack_registry::register_stack(sstring name,
        boost::program_options::options_description opts,
        std::function<future<std::unique_ptr<network_stack>> (options opts)> create, bool make_default) {
    _map()[name] = std::move(create);
    options_description().add(opts);
    if (make_default) {
        _default() = name;
    }
}

sstring network_stack_registry::default_stack() {
    return _default();
}

std::vector<sstring> network_stack_registry::list() {
    std::vector<sstring> ret;
    for (auto&& ns : _map()) {
        ret.push_back(ns.first);
    }
    return ret;
}

future<std::unique_ptr<network_stack>>
network_stack_registry::create(options opts) {
    return create(_default(), opts);
}

future<std::unique_ptr<network_stack>>
network_stack_registry::create(sstring name, options opts) {
    return _map()[name](opts);
}

boost::program_options::options_description
reactor::get_options_description() {
    namespace bpo = boost::program_options;
    bpo::options_description opts("Core options");
    auto net_stack_names = network_stack_registry::list();
    opts.add_options()
        ("network-stack", bpo::value<std::string>(),
                sprint("select network stack (valid values: %s)",
                        format_separated(net_stack_names.begin(), net_stack_names.end(), ", ")).c_str())
        ("no-handle-interrupt", "ignore SIGINT (for gdb)")
        ("task-quota", bpo::value<int>()->default_value(200), "Max number of tasks executed between polls and in loops")
        ("poll", "never sleep, poll for the next event")
        ;
    opts.add(network_stack_registry::options_description());
    return opts;
}

boost::program_options::options_description
smp::get_options_description()
{
    namespace bpo = boost::program_options;
    bpo::options_description opts("SMP options");
    auto cpus = resource::nr_processing_units();
    opts.add_options()
        ("smp,c", bpo::value<unsigned>()->default_value(cpus), "number of threads")
        ("memory,m", bpo::value<std::string>(), "memory to use, in bytes (ex: 4G) (default: all)")
        ("reserve-memory", bpo::value<std::string>()->default_value("512M"), "memory reserved to OS")
        ("hugepages", bpo::value<std::string>(), "path to accessible hugetlbfs mount (typically /dev/hugepages/something)")
        ;
    return opts;
}

std::vector<posix_thread> smp::_threads;
smp_message_queue** smp::_qs;
std::thread::id smp::_tmain;
unsigned smp::count = 1;

void smp::listen_all(smp_message_queue* qs)
{
    for (unsigned i = 0; i < smp::count; i++) {
        qs[i]._pending_peer = &engine;
        qs[i].listen();
    }
}

void smp::start_all_queues()
{
    for (unsigned c = 0; c < count; c++) {
        _qs[c][engine.cpu_id()].start();
    }
    listen_all(_qs[engine.cpu_id()]);
}

void smp::configure(boost::program_options::variables_map configuration)
{
    smp::count = 1;
    smp::_tmain = std::this_thread::get_id();
    smp::count = configuration["smp"].as<unsigned>();
    resource::configuration rc;
    if (configuration.count("memory")) {
        rc.total_memory = parse_memory_size(configuration["memory"].as<std::string>());
    }
    if (configuration.count("reserve-memory")) {
        rc.reserve_memory = parse_memory_size(configuration["reserve-memory"].as<std::string>());
    }
    std::experimental::optional<std::string> hugepages_path;
    if (configuration.count("hugepages")) {
        hugepages_path = configuration["hugepages"].as<std::string>();
    }
    rc.cpus = smp::count;
    std::vector<resource::cpu> allocations = resource::allocate(rc);
    pin_this_thread(allocations[0].cpu_id);
    memory::configure(allocations[0].mem, hugepages_path);
    smp::_qs = new smp_message_queue* [smp::count];
    for(unsigned i = 0; i < smp::count; i++) {
        smp::_qs[i] = new smp_message_queue[smp::count];
    }

    // Better to put it into the smp class, but at smp construction time
    // correct smp::count is not known.
    static boost::barrier inited(smp::count);

    for (unsigned i = 1; i < smp::count; i++) {
        auto allocation = allocations[i];
        _threads.emplace_back([configuration, hugepages_path, i, allocation] {
            pin_this_thread(allocation.cpu_id);
            memory::configure(allocation.mem, hugepages_path);
            sigset_t mask;
            sigfillset(&mask);
            auto r = ::sigprocmask(SIG_BLOCK, &mask, NULL);
            throw_system_error_on(r == -1);
            engine._id = i;
            start_all_queues();
            inited.wait();
            engine.configure(configuration);
            engine.run();
        });
    }
    start_all_queues();
    inited.wait();
    engine.configure(configuration);
}

void smp::join_all()
{
    for (auto&& t: smp::_threads) {
        t.join();
    }
}

__thread size_t future_avail_count = 0;
__thread size_t task_quota = 0;

thread_local reactor engine;


class reactor_notifier_epoll : public reactor_notifier {
    writeable_eventfd _write;
    readable_eventfd _read;
public:
    reactor_notifier_epoll()
        : _write()
        , _read(_write.read_side()) {
    }
    virtual future<> wait() override {
        // convert _read.wait(), a future<size_t>, to a future<>:
        return _read.wait().then([this] (size_t ignore) {
            return make_ready_future<>();
        });
    }
    virtual void signal() override {
        _write.signal(1);
    }
};

std::unique_ptr<reactor_notifier>
reactor_backend_epoll::make_reactor_notifier() {
    return std::make_unique<reactor_notifier_epoll>();
}

#ifdef HAVE_OSV
class reactor_notifier_osv :
        public reactor_notifier, private osv::newpoll::pollable {
    promise<> _pr;
    // TODO: pollable should probably remember its poller, so we shouldn't
    // need to keep another copy of this pointer
    osv::newpoll::poller *_poller = nullptr;
    bool _needed = false;
public:
    virtual future<> wait() override {
        return engine.notified(this);
    }
    virtual void signal() override {
        wake();
    }
    virtual void on_wake() override {
        _pr.set_value();
        _pr = promise<>();
        // We try to avoid del()/add() ping-pongs: After an one occurance of
        // the event, we don't del() but rather set needed=false. We guess
        // the future's continuation (scheduler by _pr.set_value() above)
        // will make the pollable needed again. Only if we reach this callback
        // a second time, and needed is still false, do we finally del().
        if (!_needed) {
            _poller->del(this);
            _poller = nullptr;

        }
        _needed = false;
    }

    void enable(osv::newpoll::poller &poller) {
        _needed = true;
        if (_poller == &poller) {
            return;
        }
        assert(!_poller); // don't put same pollable on multiple pollers!
        _poller = &poller;
        _poller->add(this);
    }

    virtual ~reactor_notifier_osv() {
        if (_poller) {
            _poller->del(this);
        }
    }

    friend class reactor_backend_osv;
};

std::unique_ptr<reactor_notifier>
reactor_backend_osv::make_reactor_notifier() {
    return std::make_unique<reactor_notifier_osv>();
}
#endif


#ifdef HAVE_OSV
reactor_backend_osv::reactor_backend_osv() {
}

void
reactor_backend_osv::wait_and_process(bool block, std::function<void()>& pre_process) {
    if (block) {
        _poller.wait();
    }
    pre_process();
    _poller.process();
    // osv::poller::process runs pollable's callbacks, but does not currently
    // have a timer expiration callback - instead if gives us an expired()
    // function we need to check:
    if (_poller.expired()) {
        _timer_promise.set_value();
        _timer_promise = promise<>();
    }
}

future<>
reactor_backend_osv::notified(reactor_notifier *notifier) {
    // reactor_backend_osv::make_reactor_notifier() generates a
    // reactor_notifier_osv, so we only can work on such notifiers.
    reactor_notifier_osv *n = dynamic_cast<reactor_notifier_osv *>(notifier);
    if (n->read()) {
        return make_ready_future<>();
    }
    n->enable(_poller);
    return n->_pr.get_future();
}


future<>
reactor_backend_osv::readable(pollable_fd_state& fd) {
    std::cout << "reactor_backend_osv does not support file descriptors - readable() shouldn't have been called!\n";
    abort();
}

future<>
reactor_backend_osv::writeable(pollable_fd_state& fd) {
    std::cout << "reactor_backend_osv does not support file descriptors - writeable() shouldn't have been called!\n";
    abort();
}

void
reactor_backend_osv::forget(pollable_fd_state& fd) {
    std::cout << "reactor_backend_osv does not support file descriptors - forget() shouldn't have been called!\n";
    abort();
}

future<>
reactor_backend_osv::receive_signal(int signo) {
    std::cout << "reactor_backend_osv::receive_signal() not yet implemented\n";
    abort();
}

void
reactor_backend_osv::enable_timer(clock_type::time_point when) {
    _poller.set_timer(when);
}

future<>
reactor_backend_osv::timers_completed() {
    if (_poller.expired()) {
        return make_ready_future<>();
    }
    return _timer_promise.get_future();
}
#endif
