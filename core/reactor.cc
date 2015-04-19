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
 * Copyright 2014 Cloudius Systems
 */

#include <sys/syscall.h>
#include "reactor.hh"
#include "memory.hh"
#include "core/posix.hh"
#include "net/packet.hh"
#include "resource.hh"
#include "print.hh"
#include "scollectd.hh"
#include "util/conversions.hh"
#include "core/future-util.hh"
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <boost/thread/barrier.hpp>
#include <atomic>
#include <dirent.h>
#ifdef HAVE_DPDK
#include <core/dpdk_rte.hh>
#include <rte_lcore.h>
#include <rte_launch.h>
#endif
#include "prefetch.hh"
#include <exception>
#ifdef __GNUC__
#include <iostream>
#include <system_error>
#include <cxxabi.h>
#endif

#ifdef HAVE_OSV
#include <osv/newpoll.hh>
#endif

using namespace net;

std::atomic<lowres_clock::rep> lowres_clock::_now;
constexpr std::chrono::milliseconds lowres_clock::_granularity;

timespec to_timespec(clock_type::time_point t) {
    using ns = std::chrono::nanoseconds;
    auto n = std::chrono::duration_cast<ns>(t.time_since_epoch()).count();
    return { n / 1'000'000'000, n % 1'000'000'000 };
}

lowres_clock::lowres_clock() {
    _timer.set_callback([this] { update(); });
    _timer.arm_periodic(_granularity);
}

void lowres_clock::update() {
    auto ticks = _granularity.count();
    _now.fetch_add(ticks, std::memory_order_relaxed);
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

// Wrapper for a system call result containing the return value,
// an output parameter that was returned from the syscall, and errno.
template <typename Extra>
struct syscall_result_extra {
    int result;
    Extra extra;
    int error;
    void throw_if_error() {
        if (result == -1) {
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

template <typename Extra>
syscall_result_extra<Extra>
wrap_syscall(int result, const Extra& extra) {
    return {result, extra, errno};
}

reactor_backend_epoll::reactor_backend_epoll()
    : _epollfd(file_desc::epoll_create(EPOLL_CLOEXEC)) {
}

reactor::signals::signals() : _pending_signals(0) {
}

reactor::signals::~signals() {
    sigset_t mask;
    sigfillset(&mask);
    ::sigprocmask(SIG_BLOCK, &mask, NULL);
}

reactor::signals::signal_handler::signal_handler(int signo, std::function<void ()>&& handler)
        : _handler(std::move(handler)) {
    auto mask = make_sigset_mask(signo);
    auto r = ::sigprocmask(SIG_UNBLOCK, &mask, NULL);
    throw_system_error_on(r == -1);
    struct sigaction sa;
    sa.sa_sigaction = action;
    sa.sa_mask = make_empty_sigset_mask();
    sa.sa_flags = SA_SIGINFO | SA_RESTART;
    r = ::sigaction(signo, &sa, nullptr);
    throw_system_error_on(r == -1);
}

void
reactor::signals::handle_signal(int signo, std::function<void ()>&& handler) {
    _signal_handlers.emplace(std::piecewise_construct,
        std::make_tuple(signo), std::make_tuple(signo, std::move(handler)));
}

void
reactor::signals::handle_signal_once(int signo, std::function<void ()>&& handler) {
    return handle_signal(signo, [fired = false, handler = std::move(handler)] () mutable {
        if (!fired) {
            fired = true;
            handler();
        }
    });
}

bool reactor::signals::poll_signal() {
    auto signals = _pending_signals.load(std::memory_order_relaxed);
    if (signals) {
        _pending_signals.fetch_and(~signals, std::memory_order_relaxed);
        for (size_t i = 0; i < sizeof(signals)*8; i++) {
            if (signals & (1ull << i)) {
               _signal_handlers.at(i)._handler();
            }
        }
    }
    return signals;
}

void reactor::signals::action(int signo, siginfo_t* siginfo, void* ignore) {
    engine()._signals._pending_signals.fetch_or(1ull << signo, std::memory_order_relaxed);
}

reactor::reactor()
    : _backend()
#ifdef HAVE_OSV
    , _timer_thread(
        [&] { timer_thread_func(); }, sched::thread::attr().stack(4096).name("timer_thread").pin(sched::cpu::current()))
    , _engine_thread(sched::thread::current())
#endif
    , _exit_future(_exit_promise.get_future())
    , _cpu_started(0)
    , _io_context(0)
    , _io_context_available(max_aio)
    , _reuseport(posix_reuseport_detect()) {

    auto r = ::io_setup(max_aio, &_io_context);
    assert(r >= 0);
#ifdef HAVE_OSV
    _timer_thread.start();
#else
    struct sigevent sev;
    sev.sigev_notify = SIGEV_THREAD_ID;
    sev._sigev_un._tid = syscall(SYS_gettid);
    sev.sigev_signo = SIGALRM;
    r = timer_create(CLOCK_REALTIME, &sev, &_timer);
    assert(r >= 0);
#endif
    memory::set_reclaim_hook([this] (std::function<void ()> reclaim_fn) {
        // push it in the front of the queue so we reclaim memory quickly
        _pending_tasks.push_front(make_task([fn = std::move(reclaim_fn)] {
            fn();
        }));
    });
}

#ifdef HAVE_OSV
void reactor::timer_thread_func() {
    sched::timer tmr(*sched::thread::current());
    WITH_LOCK(_timer_mutex) {
        while (!_stopped) {
            if (_timer_due != 0) {
                set_timer(tmr, _timer_due);
                _timer_cond.wait(_timer_mutex, &tmr);
                if (tmr.expired()) {
                    _timer_due = 0;
                    _engine_thread->unsafe_stop();
                    _pending_tasks.push_front(make_task([this] {
                        complete_timers(_timers, _expired_timers, [this] {
                            if (!_timers.empty()) {
                                enable_timer(_timers.get_next_timeout());
                            }
                        });
                    }));
                    _engine_thread->wake();
                } else {
                    tmr.cancel();
                }
            } else {
                _timer_cond.wait(_timer_mutex);
            }
        }
    }
}

void reactor::set_timer(sched::timer &tmr, s64 t) {
    using namespace osv::clock;
    tmr.set(wall::time_point(std::chrono::nanoseconds(t)));
}
#endif

void reactor::configure(boost::program_options::variables_map vm) {
    auto network_stack_ready = vm.count("network-stack")
        ? network_stack_registry::create(sstring(vm["network-stack"].as<std::string>()), vm)
        : network_stack_registry::create(vm);
    network_stack_ready.then([this] (std::unique_ptr<network_stack> stack) {
        _network_stack_ready_promise.set_value(std::move(stack));
    });

    _handle_sigint = !vm.count("no-handle-interrupt");
    _task_quota = vm["task-quota"].as<int>();
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
        engine().start_epoll();
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
    if (_reuseport)
        fd.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);

    fd.bind(sa.u.sa, sizeof(sa.u.sas));
    fd.listen(100);
    return pollable_fd(std::move(fd));
}

bool
reactor::posix_reuseport_detect() {
    try {
        file_desc fd = file_desc::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        fd.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1);
        return true;
    } catch(std::system_error& e) {
        return false;
    }
}

future<pollable_fd>
reactor::posix_connect(socket_address sa) {
    file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    fd.connect(sa.u.sa, sizeof(sa.u.sas));
    auto pfd = pollable_fd(std::move(fd));
    auto f = pfd.writeable();
    return f.then([pfd = std::move(pfd)] () mutable {
        int err;
        pfd.get_file_desc().getsockopt(SOL_SOCKET, SO_ERROR, err);
        throw_system_error_on(err != 0);
        return make_ready_future<pollable_fd>(std::move(pfd));
    });
}

server_socket
reactor::listen(socket_address sa, listen_options opt) {
    return _network_stack->listen(sa, opt);
}

future<connected_socket>
reactor::connect(socket_address sa) {
    return _network_stack->connect(sa);
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
        iocb* p = &io;
        auto r = ::io_submit(_io_context, 1, &p);
        throw_kernel_error(r);
        return pr.release()->get_future();
    });
}

bool reactor::process_io()
{
    io_event ev[max_aio];
    struct timespec timeout = {0, 0};
    auto n = ::io_getevents(_io_context, 1, max_aio, ev, &timeout);
    assert(n >= 0);
    for (size_t i = 0; i < size_t(n); ++i) {
        auto pr = reinterpret_cast<promise<io_event>*>(ev[i].data);
        pr->set_value(ev[i]);
        delete pr;
    }
    _io_context_available.signal(n);
    return n;
}

future<size_t>
posix_file_impl::write_dma(uint64_t pos, const void* buffer, size_t len) {
    return engine().submit_io([this, pos, buffer, len] (iocb& io) {
        io_prep_pwrite(&io, _fd, const_cast<void*>(buffer), len, pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
posix_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov) {
    return engine().submit_io([this, pos, iov = std::move(iov)] (iocb& io) {
        io_prep_pwritev(&io, _fd, iov.data(), iov.size(), pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
posix_file_impl::read_dma(uint64_t pos, void* buffer, size_t len) {
    return engine().submit_io([this, pos, buffer, len] (iocb& io) {
        io_prep_pread(&io, _fd, buffer, len, pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
posix_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov) {
    return engine().submit_io([this, pos, iov = std::move(iov)] (iocb& io) {
        io_prep_preadv(&io, _fd, iov.data(), iov.size(), pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<file>
reactor::open_file_dma(sstring name, open_flags flags) {
    return _thread_pool.submit<syscall_result<int>>([name, flags] {
        return wrap_syscall<int>(::open(name.c_str(), O_DIRECT | O_CLOEXEC | static_cast<int>(flags), S_IRWXU));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<file>(file(sr.result));
    });
}

future<>
reactor::remove_file(sstring pathname) {
    return engine()._thread_pool.submit<syscall_result<int>>([this, pathname] {
        return wrap_syscall<int>(::unlink(pathname.c_str()));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

directory_entry_type stat_to_entry_type(__mode_t type) {
    if (S_ISDIR(type)) {
        return directory_entry_type::directory;
    }
    if (S_ISBLK(type)) {
        return directory_entry_type::block_device;
    }
    if (S_ISCHR(type)) {
            return directory_entry_type::char_device;
    }
    if (S_ISFIFO(type)) {
        return directory_entry_type::fifo;
    }
    if (S_ISLNK(type)) {
        return directory_entry_type::link;
    }
    return directory_entry_type::regular;

}

future<std::experimental::optional<directory_entry_type>>
reactor::file_type(sstring name) {
    return _thread_pool.submit<syscall_result_extra<struct stat>>([name] {
        struct stat st;
        auto ret = stat(name.c_str(), &st);
        return wrap_syscall(ret, st);
    }).then([] (syscall_result_extra<struct stat> sr) {
        if (long(sr.result) == -1) {
            if (sr.error != ENOENT && sr.error != ENOTDIR) {
                sr.throw_if_error();
            }
            return make_ready_future<std::experimental::optional<directory_entry_type> >
                (std::experimental::optional<directory_entry_type>() );
        }
        return make_ready_future<std::experimental::optional<directory_entry_type> >
            (std::experimental::optional<directory_entry_type>(stat_to_entry_type(sr.extra.st_mode)) );
    });
}

future<file>
reactor::open_directory(sstring name) {
    return _thread_pool.submit<syscall_result<int>>([name] {
        return wrap_syscall<int>(::open(name.c_str(), O_DIRECTORY | O_CLOEXEC | O_RDONLY));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<file>(file(sr.result));
    });
}

future<>
posix_file_impl::flush(void) {
    return engine()._thread_pool.submit<syscall_result<int>>([this] {
        return wrap_syscall<int>(::fsync(_fd));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

future<struct stat>
posix_file_impl::stat(void) {
    return engine()._thread_pool.submit<syscall_result_extra<struct stat>>([this] {
        struct stat st;
        auto ret = ::fstat(_fd, &st);
        return wrap_syscall(ret, st);
    }).then([] (syscall_result_extra<struct stat> ret) {
        ret.throw_if_error();
        return make_ready_future<struct stat>(ret.extra);
    });
}

future<>
posix_file_impl::truncate(uint64_t length) {
    return engine()._thread_pool.submit<syscall_result<int>>([this, length] {
        return wrap_syscall<int>(::ftruncate(_fd, length));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

future<>
blockdev_file_impl::truncate(uint64_t length) {
    return make_ready_future<>();
}

future<>
posix_file_impl::discard(uint64_t offset, uint64_t length) {
    return engine()._thread_pool.submit<syscall_result<int>>([this, offset, length] () mutable {
        return wrap_syscall<int>(::fallocate(_fd, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE,
            offset, length));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

future<>
blockdev_file_impl::discard(uint64_t offset, uint64_t length) {
    return engine()._thread_pool.submit<syscall_result<int>>([this, offset, length] () mutable {
        uint64_t range[2] { offset, length };
        return wrap_syscall<int>(::ioctl(_fd, BLKDISCARD, &range));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

future<size_t>
posix_file_impl::size(void) {
    return posix_file_impl::stat().then([] (struct stat&& st) {
        return make_ready_future<size_t>(st.st_size);
    });
}

future<size_t>
blockdev_file_impl::size(void) {
    return engine()._thread_pool.submit<syscall_result_extra<size_t>>([this] {
        size_t size;
        int ret = ::ioctl(_fd, BLKGETSIZE64, &size);
        return wrap_syscall(ret, size);
    }).then([] (syscall_result_extra<size_t> ret) {
        ret.throw_if_error();
        return make_ready_future<size_t>(ret.extra);
    });
}

subscription<directory_entry>
posix_file_impl::list_directory(std::function<future<> (directory_entry de)> next) {
    struct work {
        stream<directory_entry> s;
        unsigned current = 0;
        unsigned total = 0;
        bool eof = false;
        int error = 0;
        char buffer[8192];
    };

    // While it would be natural to use fdopendir()/readdir(),
    // our syscall thread pool doesn't support malloc(), which is
    // required for this to work.  So resort to using getdents()
    // instead.

    // From getdents(2):
    struct linux_dirent {
        unsigned long  d_ino;     /* Inode number */
        unsigned long  d_off;     /* Offset to next linux_dirent */
        unsigned short d_reclen;  /* Length of this linux_dirent */
        char           d_name[];  /* Filename (null-terminated) */
        /* length is actually (d_reclen - 2 -
                             offsetof(struct linux_dirent, d_name)) */
        /*
        char           pad;       // Zero padding byte
        char           d_type;    // File type (only since Linux
                                  // 2.6.4); offset is (d_reclen - 1)
         */
    };

    auto w = make_lw_shared<work>();
    auto ret = w->s.listen(std::move(next));
    w->s.started().then([w, this] {
        auto eofcond = [w] { return w->eof; };
        return do_until(eofcond, [w, this] {
            if (w->current == w->total) {
                return engine()._thread_pool.submit<syscall_result<long>>([w , this] () {
                    auto ret = ::syscall(__NR_getdents, _fd, reinterpret_cast<linux_dirent*>(w->buffer), sizeof(w->buffer));
                    return wrap_syscall(ret);
                }).then([w] (syscall_result<long> ret) {
                    ret.throw_if_error();
                    if (ret.result == 0) {
                        w->eof = true;
                    } else {
                        w->current = 0;
                        w->total = ret.result;
                    }
                });
            }
            auto start = w->buffer + w->current;
            auto de = reinterpret_cast<linux_dirent*>(start);
            std::experimental::optional<directory_entry_type> type;
            switch (start[de->d_reclen - 1]) {
            case DT_BLK:
                type = directory_entry_type::block_device;
                break;
            case DT_CHR:
                type = directory_entry_type::char_device;
                break;
            case DT_DIR:
                type = directory_entry_type::directory;
                break;
            case DT_FIFO:
                type = directory_entry_type::fifo;
                break;
            case DT_REG:
                type = directory_entry_type::regular;
                break;
            case DT_SOCK:
                type = directory_entry_type::socket;
                break;
            default:
                // unknown, ignore
                ;
            }
            w->current += de->d_reclen;
            sstring name = de->d_name;
            if (name == "." || name == "..") {
                return make_ready_future<>();
            }
            return w->s.produce({std::move(name), type});
        });
    }).then([w] {
        w->s.close();
    });
    return ret;
}

void reactor::enable_timer(clock_type::time_point when)
{
#ifndef HAVE_OSV
    itimerspec its;
    its.it_interval = {};
    its.it_value = to_timespec(when);
    auto ret = timer_settime(_timer, TIMER_ABSTIME, &its, NULL);
    throw_system_error_on(ret == -1);
#else
    using ns = std::chrono::nanoseconds;
    WITH_LOCK(_timer_mutex) {
        _timer_due = std::chrono::duration_cast<ns>(when.time_since_epoch()).count();
        _timer_cond.wake_one();
    }
#endif
}

void reactor::add_timer(timer<>* tmr) {
    if (queue_timer(tmr)) {
        enable_timer(_timers.get_next_timeout());
    }
}

bool reactor::queue_timer(timer<>* tmr) {
    return _timers.insert(*tmr);
}

void reactor::del_timer(timer<>* tmr) {
    if (tmr->_expired) {
        _expired_timers.erase(_expired_timers.iterator_to(*tmr));
        tmr->_expired = false;
    } else {
        _timers.remove(*tmr);
    }
}

void reactor::add_timer(timer<lowres_clock>* tmr) {
    if (queue_timer(tmr)) {
        _lowres_next_timeout = _lowres_timers.get_next_timeout();
    }
}

bool reactor::queue_timer(timer<lowres_clock>* tmr) {
    return _lowres_timers.insert(*tmr);
}

void reactor::del_timer(timer<lowres_clock>* tmr) {
    if (tmr->_expired) {
        _expired_lowres_timers.erase(_expired_lowres_timers.iterator_to(*tmr));
        tmr->_expired = false;
    } else {
        _lowres_timers.remove(*tmr);
    }
}

future<> reactor::run_exit_tasks() {
    _exit_promise.set_value();
    return std::move(_exit_future);
}

void reactor::stop() {
    assert(engine()._id == 0);
    run_exit_tasks().then([this] {
        auto sem = new semaphore(0);
        for (unsigned i = 1; i < smp::count; i++) {
            smp::submit_to<>(i, []() {
                return engine().run_exit_tasks().then([] {
                        engine()._stopped = true;
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
            scollectd::add_polled_metric(scollectd::type_instance_id("reactor"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "idle")
                    , scollectd::make_typed(scollectd::data_type::GAUGE,
                            [this] () -> uint32_t { return _load * 100; })
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
                    "total_operations", "cross_cpu_free"),
                scollectd::make_typed(scollectd::data_type::DERIVE,
                        [] { return memory::stats().cross_cpu_frees(); })
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

void reactor::run_tasks(circular_buffer<std::unique_ptr<task>>& tasks, size_t quota) {
    task_quota = quota;
    while (!tasks.empty() && task_quota) {
        --task_quota;
        auto tsk = std::move(tasks.front());
        tasks.pop_front();
        tsk->run();
        tsk.reset();
        ++_tasks_processed;
    }
}

int reactor::run() {
    auto collectd_metrics = register_collectd_metrics();

#ifndef HAVE_OSV
    poller io_poller([&] { return process_io(); });
#endif

    poller sig_poller([&] { return _signals.poll_signal(); } );

    if (_id == 0) {
       if (_handle_sigint) {
          _signals.handle_signal_once(SIGINT, [this] { stop(); });
       }
       _signals.handle_signal_once(SIGTERM, [this] { stop(); });
    }

    _cpu_started.wait(smp::count).then([this] {
        _network_stack->initialize().then([this] {
            _start_promise.set_value();
        });
    });
    _network_stack_ready_promise.get_future().then([this] (std::unique_ptr<network_stack> stack) {
        _network_stack = std::move(stack);
        for (unsigned c = 0; c < smp::count; c++) {
            smp::submit_to(c, [] {
                    engine()._cpu_started.signal();
            });
        }
    });

    // Register smp queues poller
    std::experimental::optional<poller> smp_poller;
    if (smp::count > 1) {
        smp_poller = poller(smp::poll_queues);
    }

#ifndef HAVE_OSV
    _signals.handle_signal(SIGALRM, [this] {
        complete_timers(_timers, _expired_timers, [this] {
            if (!_timers.empty()) {
                enable_timer(_timers.get_next_timeout());
            }
        });
    });
#endif

    poller drain_cross_cpu_freelist([] {
        return memory::drain_cross_cpu_freelist();
    });

    poller expire_lowres_timers([this] {
        if (_lowres_next_timeout == lowres_clock::time_point()) {
            return false;
        }
        auto now = lowres_clock::now();
        if (now > _lowres_next_timeout) {
            complete_timers(_lowres_timers, _expired_lowres_timers, [this] {
                if (!_lowres_timers.empty()) {
                    _lowres_next_timeout = _lowres_timers.get_next_timeout();
                } else {
                    _lowres_next_timeout = lowres_clock::time_point();
                }
            });
            return true;
        }
        return false;
    });

    using namespace std::chrono_literals;
    timer<lowres_clock> load_timer;
    std::chrono::high_resolution_clock::rep idle_count = 0;
    auto idle_start = std::chrono::high_resolution_clock::now(), idle_end = idle_start;
    load_timer.set_callback([this, &idle_count, &idle_start, &idle_end] () mutable {
        auto load = double(idle_count + (idle_end - idle_start).count()) / double(std::chrono::duration_cast<std::chrono::high_resolution_clock::duration>(1s).count());
        load = std::min(load, 1.0);
        idle_count = 0;
        idle_start = idle_end;
        _loads.push_front(load);
        if (_loads.size() > 5) {
            auto drop = _loads.back();
            _loads.pop_back();
            _load -= (drop/5);
        }
        _load += (load/5);
    });
    load_timer.arm_periodic(1s);

    bool idle = false;

    while (true) {
        run_tasks(_pending_tasks, _task_quota);
        if (_stopped) {
            load_timer.cancel();
            run_tasks(_at_destroy_tasks, _at_destroy_tasks.size());
            if (_id == 0) {
                smp::join_all();
            }
            break;
        }

        if (!poll_once() && _pending_tasks.empty()) {
            idle_end = std::chrono::high_resolution_clock::now();
            if (!idle) {
                idle_start = idle_end;
                idle = true;
            }
            _mm_pause();
        } else {
            if (idle) {
                idle_count += (idle_end - idle_start).count();
                idle = false;
            }
        }
    }
    return _return;
}

bool
reactor::poll_once() {
    bool work = false;
    for (auto c : _pollers) {
        work |= c->poll_and_check_more_work();
    }

    return work;
}

class reactor::poller::registration_task : public task {
private:
    poller* _p;
public:
    explicit registration_task(poller* p) : _p(p) {}
    virtual void run() noexcept override {
        if (_p) {
            engine().register_poller(_p->_pollfn.get());
            _p->_registration_task = nullptr;
        }
    }
    void cancel() {
        _p = nullptr;
    }
    void moved(poller* p) {
        _p = p;
    }
};

class reactor::poller::deregistration_task : public task {
private:
    std::unique_ptr<pollfn> _p;
public:
    explicit deregistration_task(std::unique_ptr<pollfn>&& p) : _p(std::move(p)) {}
    virtual void run() noexcept override {
        engine().unregister_poller(_p.get());
    }
};

void reactor::register_poller(pollfn* p) {
    _pollers.push_back(p);
}

void reactor::unregister_poller(pollfn* p) {
    _pollers.erase(std::find(_pollers.begin(), _pollers.end(), p));
}

void reactor::replace_poller(pollfn* old, pollfn* neww) {
    std::replace(_pollers.begin(), _pollers.end(), old, neww);
}

reactor::poller::poller(poller&& x)
        : _pollfn(std::move(x._pollfn)), _registration_task(x._registration_task) {
    if (_pollfn && _registration_task) {
        _registration_task->moved(this);
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
reactor::poller::do_register() {
    // We can't just insert a poller into reactor::_pollers, because we
    // may be running inside a poller ourselves, and so in the middle of
    // iterating reactor::_pollers itself.  So we schedule a task to add
    // the poller instead.
    auto task = std::make_unique<registration_task>(this);
    auto tmp = task.get();
    engine().add_task(std::move(task));
    _registration_task = tmp;
}

reactor::poller::~poller() {
    // We can't just remove the poller from reactor::_pollers, because we
    // may be running inside a poller ourselves, and so in the middle of
    // iterating reactor::_pollers itself.  So we schedule a task to remove
    // the poller instead.
    //
    // Since we don't want to call the poller after we exit the destructor,
    // we replace it atomically with another one, and schedule a task to
    // delete the replacement.
    if (_pollfn) {
        if (_registration_task) {
            // not added yet, so don't do it at all.
            _registration_task->cancel();
        } else {
            auto dummy = make_pollfn([] { return false; });
            auto dummy_p = dummy.get();
            auto task = std::make_unique<deregistration_task>(std::move(dummy));
            engine().add_task(std::move(task));
            engine().replace_poller(_pollfn.get(), dummy_p);
        }
    }
}

bool
reactor_backend_epoll::wait_and_process() {
    std::array<epoll_event, 128> eevt;
    int nr = ::epoll_wait(_epollfd.get(), eevt.data(), eevt.size(), 0);
    if (nr == -1 && errno == EINTR) {
        return false; // gdb can cause this
    }
    assert(nr != -1);
    for (int i = 0; i < nr; ++i) {
        auto& evt = eevt[i];
        auto pfd = reinterpret_cast<pollable_fd_state*>(evt.data.ptr);
        auto events = evt.events & (EPOLLIN | EPOLLOUT);
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
    return nr;
}

syscall_work_queue::syscall_work_queue()
    : _pending()
    , _completed()
    , _start_eventfd(0) {
}

void syscall_work_queue::submit_item(syscall_work_queue::work_item* item) {
    _queue_has_room.wait().then([this, item] {
        _pending.push(item);
        _start_eventfd.signal(1);
    });
}

void syscall_work_queue::complete() {
    auto nr = _completed.consume_all([this] (work_item* wi) {
        wi->complete();
        delete wi;
    });
    _queue_has_room.signal(nr);
}

smp_message_queue::smp_message_queue()
    : _pending()
    , _completed()
{
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
    _last_snt_batch = nr;
    _sent += nr;
}

void smp_message_queue::submit_item(smp_message_queue::work_item* item) {
    _tx.a.pending_fifo.push_back(item);
    if (_tx.a.pending_fifo.size() >= batch_size) {
        move_pending();
    }
}

void smp_message_queue::respond(work_item* item) {
    _completed_fifo.push_back(item);
    if (_completed_fifo.size() >= batch_size || engine()._stopped) {
        flush_response_batch();
    }
}

void smp_message_queue::flush_response_batch() {
    if (!_completed_fifo.empty()) {
        _completed.push(_completed_fifo.begin(), _completed_fifo.end());
        _completed_fifo.clear();
    }
}

template<size_t PrefetchCnt, typename Func>
size_t smp_message_queue::process_queue(lf_queue& q, Func process) {
    // copy batch to local memory in order to minimize
    // time in which cross-cpu data is accessed
    work_item* items[queue_length + PrefetchCnt];
    work_item* wi;
    if (!q.pop(wi))
        return 0;
    // start prefecthing first item before popping the rest to overlap memory
    // access with potential cache miss the second pop may cause
    prefetch<2>(wi);
    auto nr = q.pop(items);
    std::fill(std::begin(items) + nr, std::begin(items) + nr + PrefetchCnt, nr ? items[nr - 1] : wi);
    unsigned i = 0;
    do {
        prefetch_n<2>(std::begin(items) + i, std::begin(items) + i + PrefetchCnt);
        process(wi);
        wi = items[i++];
    } while(i <= nr);

    return nr + 1;
}

size_t smp_message_queue::process_completions() {
    auto nr = process_queue<prefetch_cnt*2>(_completed, [] (work_item* wi) {
        wi->complete();
        delete wi;
    });
    _current_queue_length -= nr;
    _compl += nr;
    _last_cmpl_batch = nr;

    return nr;
}

void smp_message_queue::flush_request_batch() {
    move_pending();
}

size_t smp_message_queue::process_incoming() {
    auto nr = process_queue<prefetch_cnt>(_pending, [this] (work_item* wi) {
        wi->process().then([this, wi] {
            respond(wi);
        });
    });
    _received += nr;
    _last_rcv_batch = nr;
    return nr;
}

void smp_message_queue::start(unsigned cpuid) {
    _tx.init();
    char instance[10];
    std::snprintf(instance, sizeof(instance), "%u-%u", engine().cpu_id(), cpuid);
    _collectd_regs = {
            // queue_length     value:GAUGE:0:U
            // Absolute value of num packets in last tx batch.
            scollectd::add_polled_metric(scollectd::type_instance_id("smp"
                    , instance
                    , "queue_length", "send-batch")
            , scollectd::make_typed(scollectd::data_type::GAUGE, _last_snt_batch)
            ),
            scollectd::add_polled_metric(scollectd::type_instance_id("smp"
                    , instance
                    , "queue_length", "receive-batch")
            , scollectd::make_typed(scollectd::data_type::GAUGE, _last_rcv_batch)
            ),
            scollectd::add_polled_metric(scollectd::type_instance_id("smp"
                    , instance
                    , "queue_length", "complete-batch")
            , scollectd::make_typed(scollectd::data_type::GAUGE, _last_cmpl_batch)
            ),
            scollectd::add_polled_metric(scollectd::type_instance_id("smp"
                    , instance
                    , "queue_length", "send-queue-length")
            , scollectd::make_typed(scollectd::data_type::GAUGE, _current_queue_length)
            ),
            // total_operations value:DERIVE:0:U
            scollectd::add_polled_metric(scollectd::type_instance_id("smp"
                    , instance
                    , "total_operations", "received-messages")
            , scollectd::make_typed(scollectd::data_type::DERIVE, _received)
            ),
            // total_operations value:DERIVE:0:U
            scollectd::add_polled_metric(scollectd::type_instance_id("smp"
                    , instance
                    , "total_operations", "sent-messages")
            , scollectd::make_typed(scollectd::data_type::DERIVE, _sent)
            ),
            // total_operations value:DERIVE:0:U
            scollectd::add_polled_metric(scollectd::type_instance_id("smp"
                    , instance
                    , "total_operations", "completed-messages")
            , scollectd::make_typed(scollectd::data_type::DERIVE, _compl)
            ),
    };
}

/* not yet implemented for OSv. TODO: do the notification like we do class smp. */
#ifndef HAVE_OSV
thread_pool::thread_pool() : _worker_thread([this] { work(); }), _notify(pthread_self()) {
    engine()._signals.handle_signal(SIGUSR1, [this] { inter_thread_wq.complete(); });
}

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
        inter_thread_wq._pending.consume_all([this] (syscall_work_queue::work_item* wi) {
            wi->process();
            inter_thread_wq._completed.push(wi);
        });
        pthread_kill(_notify, SIGUSR1);
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
    return engine().readable(*_fd._s).then([this] {
        uint64_t count;
        int r = ::read(_fd.get_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
        return make_ready_future<size_t>(count);
    });
}

void schedule(std::unique_ptr<task> t) {
    engine().add_task(std::move(t));
}

void report_failed_future(std::exception_ptr exp) {
    try {
        std::rethrow_exception(std::move(exp));
    } catch (std::exception& ex) {
        std::cerr << "WARNING: exceptional future ignored: " << ex.what() << "\n";
    } catch (...) {
        std::cerr << "WARNING: exceptional future ignored\n";
    }
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

std::vector<smp::thread_adaptor> smp::_threads;
smp_message_queue** smp::_qs;
std::thread::id smp::_tmain;
unsigned smp::count = 1;

void smp::start_all_queues()
{
    for (unsigned c = 0; c < count; c++) {
        if (c != engine().cpu_id()) {
            _qs[c][engine().cpu_id()].start(c);
        }
    }
}

#ifdef HAVE_DPDK

int dpdk_thread_adaptor(void* f)
{
    (*static_cast<std::function<void ()>*>(f))();
    return 0;
}

void smp::join_all()
{
    rte_eal_mp_wait_lcore();
}

void smp::pin(unsigned cpu_id) {
}
#else
void smp::join_all()
{
    for (auto&& t: smp::_threads) {
        t.join();
    }
}

void smp::pin(unsigned cpu_id) {
    pin_this_thread(cpu_id);
}
#endif

void smp::allocate_reactor() {
    static thread_local std::unique_ptr<reactor> reactor_holder;

    assert(!reactor_holder);

    // we cannot just write "local_engin = new reactor" since reactor's constructor
    // uses local_engine
    auto buf = new (with_alignment(64)) char[sizeof(reactor)];
    local_engine = reinterpret_cast<reactor*>(buf);
    new (buf) reactor;
    reactor_holder.reset(local_engine);
}

void smp::cleanup() {
    smp::_threads = std::vector<thread_adaptor>();
}

void smp::configure(boost::program_options::variables_map configuration)
{
    smp::count = 1;
    smp::_tmain = std::this_thread::get_id();
    smp::count = configuration["smp"].as<unsigned>();
    resource::configuration rc;
    if (configuration.count("memory")) {
        rc.total_memory = parse_memory_size(configuration["memory"].as<std::string>());
#ifdef HAVE_DPDK
        if (configuration.count("hugepages") &&
            !configuration["network-stack"].as<std::string>().compare("native") &&
            configuration.count("dpdk-pmd")) {
            size_t dpdk_memory = dpdk::eal::mem_size(smp::count);

            if (dpdk_memory >= rc.total_memory) {
                std::cerr<<"Can't run with the given amount of memory: ";
                std::cerr<<configuration["memory"].as<std::string>();
                std::cerr<<". Consider giving more."<<std::endl;
                exit(1);
            }

            //
            // Subtract the memory we are about to give to DPDK from the total
            // amount of memory we are allowed to use.
            //
            rc.total_memory.value() -= dpdk_memory;
        }
#endif
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
    smp::pin(allocations[0].cpu_id);
    memory::configure(allocations[0].mem, hugepages_path);
    smp::_qs = new smp_message_queue* [smp::count];
    for(unsigned i = 0; i < smp::count; i++) {
        smp::_qs[i] = new smp_message_queue[smp::count];
    }

#ifdef HAVE_DPDK
    dpdk::eal::cpuset cpus;
    for (auto&& a : allocations) {
        cpus[a.cpu_id] = true;
    }
    dpdk::eal::init(cpus, configuration);
#endif

    // Better to put it into the smp class, but at smp construction time
    // correct smp::count is not known.
    static boost::barrier inited(smp::count);

    unsigned i;
    for (i = 1; i < smp::count; i++) {
        auto allocation = allocations[i];
        _threads.emplace_back([configuration, hugepages_path, i, allocation] {
            smp::pin(allocation.cpu_id);
            memory::configure(allocation.mem, hugepages_path);
            sigset_t mask;
            sigfillset(&mask);
            auto r = ::sigprocmask(SIG_BLOCK, &mask, NULL);
            throw_system_error_on(r == -1);
            allocate_reactor();
            engine()._id = i;
            start_all_queues();
            inited.wait();
            engine().configure(configuration);
            engine().run();
        });
    }

    allocate_reactor();

#ifdef HAVE_DPDK
    auto it = _threads.begin();
    RTE_LCORE_FOREACH_SLAVE(i) {
        rte_eal_remote_launch(dpdk_thread_adaptor, static_cast<void*>(&*(it++)), i);
    }
#endif

    start_all_queues();
    inited.wait();
    engine().configure(configuration);
    engine()._lowres_clock = std::make_unique<lowres_clock>();
}

__thread size_t future_avail_count = 0;
__thread size_t task_quota = 0;

__thread reactor* local_engine;

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
        return engine().notified(this);
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

bool
reactor_backend_osv::wait_and_process() {
    _poller.process();
    // osv::poller::process runs pollable's callbacks, but does not currently
    // have a timer expiration callback - instead if gives us an expired()
    // function we need to check:
    if (_poller.expired()) {
        _timer_promise.set_value();
        _timer_promise = promise<>();
    }
    return true;
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

void
reactor_backend_osv::enable_timer(clock_type::time_point when) {
    _poller.set_timer(when);
}

#endif

/**
 * engine_exit() exits the reactor. It should be given a pointer to the
 * exception which prompted this exit - or a null pointer if the exit
 * request was not caused by any exception.
 */
void engine_exit(std::exception_ptr eptr) {
    if (!eptr) {
        engine().exit(0);
        return;
    }
#ifndef __GNUC__
    std::cerr << "Exiting on unhandled exception.\n";
#else
    try {
        std::rethrow_exception(eptr);
    } catch(...) {
        auto tp = abi::__cxa_current_exception_type();
        std::cerr << "Exiting on unhandled exception ";
        if (tp) {
            int status;
            char *demangled = abi::__cxa_demangle(tp->name(), 0, 0, &status);
            std::cerr << "of type '";
            if (status == 0) {
                std::cerr << demangled;
                free(demangled);
            } else {
                std::cerr << tp->name();
            }
            std::cerr << "'.\n";
        } else {
            std::cerr << "of unknown type.\n";
        }
        // Print more information on some known exception types
        try {
            throw;
        } catch(const std::system_error &e) {
            std::cerr << "Error " << e.code() << " (" << e.code().message() << ")\n";
        } catch(const std::exception& e) {
            std::cerr << e.what() << "\n";
        }
    }
#endif
    engine().exit(1);
}

future<file> open_file_dma(sstring name, open_flags flags) {
    return engine().open_file_dma(std::move(name), flags);
}

future<file> open_directory(sstring name) {
    return engine().open_directory(std::move(name));
}

future<> remove_file(sstring pathname) {
    return engine().remove_file(std::move(pathname));
}

server_socket listen(socket_address sa) {
    return engine().listen(sa);
}

server_socket listen(socket_address sa, listen_options opts) {
    return engine().listen(sa, opts);
}

future<connected_socket> connect(socket_address sa) {
    return engine().connect(sa);
}
