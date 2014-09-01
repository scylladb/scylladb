/*
 * reactor.cc
 *
 *  Created on: Aug 1, 2014
 *      Author: avi
 */

#include "reactor.hh"
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>

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

reactor::reactor()
    : _epollfd(file_desc::epoll_create(EPOLL_CLOEXEC))
    , _io_eventfd()
    , _io_context(0)
    , _io_context_available(max_aio) {
    auto r = ::io_setup(max_aio, &_io_context);
    assert(r >= 0);
    _io_eventfd.wait().then([this] (size_t count) {
        process_io(count);
    });
}

future<> reactor::get_epoll_future(pollable_fd_state& pfd,
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

future<> reactor::readable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollin, EPOLLIN);
}

future<> reactor::writeable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollout, EPOLLOUT);
}

void reactor::forget(pollable_fd_state& fd) {
    if (fd.events_epoll) {
        ::epoll_ctl(_epollfd.get(), EPOLL_CTL_DEL, fd.fd.get(), nullptr);
    }
}

pollable_fd
reactor::listen(socket_address sa, listen_options opts) {
    file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (opts.reuse_address) {
        int opt = 1;
        fd.setsockopt(SOL_SOCKET, SO_REUSEADDR, opt);
    }

    fd.bind(sa.u.sa, sizeof(sa.u.sas));
    fd.listen(100);
    return pollable_fd(std::move(fd));
}

void reactor::complete_epoll_event(pollable_fd_state& pfd, promise<> pollable_fd_state::*pr,
        int events, int event) {
    if (pfd.events_requested & events & event) {
        pfd.events_requested &= ~EPOLLIN;
        pfd.events_known &= ~EPOLLIN;
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
reactor::write_dma(file& f, uint64_t pos, const void* buffer, size_t len) {
    return submit_io([&f, pos, buffer, len] (iocb& io) {
        io_prep_pwrite(&io, f._fd, const_cast<void*>(buffer), len, pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
reactor::write_dma(file& f, uint64_t pos, std::vector<iovec> iov) {
    return submit_io([&f, pos, iov = std::move(iov)] (iocb& io) {
        io_prep_pwritev(&io, f._fd, iov.data(), iov.size(), pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
reactor::read_dma(file& f, uint64_t pos, void* buffer, size_t len) {
    return submit_io([&f, pos, buffer, len] (iocb& io) {
        io_prep_pread(&io, f._fd, buffer, len, pos);
    }).then([] (io_event ev) {
        throw_kernel_error(long(ev.res));
        return make_ready_future<size_t>(size_t(ev.res));
    });
}

future<size_t>
reactor::read_dma(file& f, uint64_t pos, std::vector<iovec> iov) {
    return submit_io([&f, pos, iov = std::move(iov)] (iocb& io) {
        io_prep_preadv(&io, f._fd, iov.data(), iov.size(), pos);
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
reactor::flush(file& f) {
    return _thread_pool.submit<syscall_result<int>>([&f] {
        return wrap_syscall<int>(::fsync(f._fd));
    }).then([] (syscall_result<int> sr) {
        sr.throw_if_error();
        return make_ready_future<>();
    });
}

void reactor::add_timer(timer* tmr) {
    if (_timers.insert(*tmr) && tmr->get_timeout() < _next_timeout) {
        itimerspec its;
        its.it_interval = {};
        its.it_value = to_timespec(_timers.get_next_timeout());
        _timerfd.get_file_desc().timerfd_settime(TFD_TIMER_ABSTIME, its);
    }
}

void reactor::del_timer(timer* tmr) {
    _timers.remove(*tmr);
}

void reactor::complete_timers() {
    _timerfd.read_some(reinterpret_cast<char*>(&_timers_completed), sizeof(_timers_completed)).then(
            [this] (size_t n) {
        _timers.expire(clock_type::now());
        while (auto t = _timers.pop_expired()) {
            t->_pr.set_value();
            t->_pr = promise<>();
        }
        if (!_timers.empty()) {
            _next_timeout = _timers.get_next_timeout();
            itimerspec its;
            its.it_interval = {};
            its.it_value = to_timespec(_next_timeout);
            _timerfd.get_file_desc().timerfd_settime(TFD_TIMER_ABSTIME, its);
        }
        complete_timers();
    });
}

void reactor::run() {
    std::vector<std::unique_ptr<task>> current_tasks;
    _start_promise.set_value();
    complete_timers();
    while (true) {
        while (!_pending_tasks.empty()) {
            std::swap(_pending_tasks, current_tasks);
            for (auto&& tsk : current_tasks) {
                tsk->run();
                tsk.reset();
            }
            current_tasks.clear();
        }
        std::array<epoll_event, 128> eevt;
        int nr = ::epoll_wait(_epollfd.get(), eevt.data(), eevt.size(), -1);
        assert(nr != -1);
        for (int i = 0; i < nr; ++i) {
            auto& evt = eevt[i];
            auto pfd = reinterpret_cast<pollable_fd_state*>(evt.data.ptr);
            auto events = evt.events & (EPOLLIN | EPOLLOUT);
            std::unique_ptr<task> t_in, t_out;
            pfd->events_known |= events;
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
}

thread_pool::thread_pool()
    : _pending(queue_length)
    , _completed(queue_length)
    , _start_eventfd(0)
    , _complete_eventfd(0)
    , _worker_thread([this] { work(); }) {
    _worker_thread.detach();
    complete();
}

void thread_pool::work() {
    while (true) {
        uint64_t count;
        auto r = ::read(_start_eventfd.get_read_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
        auto nr = _pending.consume_all([this] (work_item* wi) {
            wi->process();
            _completed.push(wi);
        });
        count = nr;
        r = ::write(_complete_eventfd.get_write_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
    }
}

void thread_pool::submit_item(thread_pool::work_item* item) {
    _queue_has_room.wait().then([this, item] {
        _pending.push(item);
        _start_eventfd.signal(1);
    });
}

void thread_pool::complete() {
    _complete_eventfd.wait().then([this] (size_t count) {
        auto nr = _completed.consume_all([this] (work_item* wi) {
            wi->complete();
            delete wi;
        });
        _queue_has_room.signal(nr);
        complete();
    });
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

file_desc readable_eventfd::try_create_eventfd(size_t initial) {
    assert(size_t(int(initial)) == initial);
    return file_desc::eventfd(initial, EFD_CLOEXEC | EFD_NONBLOCK);
}

future<size_t> readable_eventfd::wait() {
    return the_reactor.readable(*_fd._s).then([this] {
        uint64_t count;
        int r = ::read(_fd.get_fd(), &count, sizeof(count));
        assert(r == sizeof(count));
        return make_ready_future<size_t>(count);
    });
}

socket_address make_ipv4_address(ipv4_addr addr) {
    socket_address sa;
    sa.u.in.sin_family = AF_INET;
    sa.u.in.sin_port = htons(addr.port);
    std::memcpy(&sa.u.in.sin_addr, addr.host, 4);
    return sa;
}

reactor the_reactor;
