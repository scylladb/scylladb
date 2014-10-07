/*
 * Copyright 2014 Cloudius Systems
 */

#include "reactor.hh"
#include "core/posix.hh"
#include "net/packet.hh"
#include "print.hh"
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>

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

reactor::reactor()
    : _network_stack(network_stack_registry::create({}))
    , _epollfd(file_desc::epoll_create(EPOLL_CLOEXEC))
    , _io_eventfd()
    , _io_context(0)
    , _io_context_available(max_aio) {
    auto r = ::io_setup(max_aio, &_io_context);
    assert(r >= 0);
}

void reactor::configure(boost::program_options::variables_map vm) {
    _network_stack = vm.count("network-stack")
        ? network_stack_registry::create(sstring(vm["network-stack"].as<std::string>()), vm)
        : network_stack_registry::create(vm);
    _handle_sigint = !vm.count("no-handle-interrupt");
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
reactor::posix_listen(socket_address sa, listen_options opts) {
    file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (opts.reuse_address) {
        int opt = 1;
        fd.setsockopt(SOL_SOCKET, SO_REUSEADDR, opt);
    }

    fd.bind(sa.u.sa, sizeof(sa.u.sas));
    fd.listen(100);
    return pollable_fd(std::move(fd));
}

server_socket
reactor::listen(socket_address sa, listen_options opt) {
    return _network_stack->listen(sa, opt);
}

class posix_connected_socket_impl final : public connected_socket_impl {
    pollable_fd _fd;
private:
    explicit posix_connected_socket_impl(pollable_fd fd) : _fd(std::move(fd)) {}
public:
    virtual input_stream<char> input() override { return input_stream<char>(posix_data_source(_fd)); }
    virtual output_stream<char> output() override { return output_stream<char>(posix_data_sink(_fd), 8192); }
    friend class posix_server_socket_impl;
    friend class posix_ap_server_socket_impl;
};

future<connected_socket, socket_address>
posix_server_socket_impl::accept() {
    return _lfd.accept().then([this] (pollable_fd fd, socket_address sa) {
        static unsigned balance = 0;
        auto cpu = balance++ % smp::count;

        if (cpu == engine._id) {
            std::unique_ptr<connected_socket_impl> csi(new posix_connected_socket_impl(std::move(fd)));
            return make_ready_future<connected_socket, socket_address>(
                    connected_socket(std::move(csi)), sa);
        } else {
            smp::submit_to(cpu, [this, fd = std::move(fd.get_file_desc()), sa] () mutable {
                posix_ap_server_socket_impl::move_connected_socket(_sa, pollable_fd(std::move(fd)), sa);
            });
            return accept();
        }
    });
}

future<connected_socket, socket_address> posix_ap_server_socket_impl::accept() {
    auto conni = conn_q.find(_sa.as_posix_sockaddr_in());
    if (conni != conn_q.end()) {
        connection c = std::move(conni->second);
        conn_q.erase(conni);
        std::unique_ptr<connected_socket_impl> csi(new posix_connected_socket_impl(std::move(c.fd)));
        return make_ready_future<connected_socket, socket_address>(connected_socket(std::move(csi)), std::move(c.addr));
    } else {
        auto i = sockets.emplace(std::piecewise_construct, std::make_tuple(_sa.as_posix_sockaddr_in()), std::make_tuple());
        assert(i.second);
        return i.first->second.get_future();
    }
}

void  posix_ap_server_socket_impl::move_connected_socket(socket_address sa, pollable_fd fd, socket_address addr) {
    auto i = sockets.find(sa.as_posix_sockaddr_in());
    if (i != sockets.end()) {
        std::unique_ptr<connected_socket_impl> csi(new posix_connected_socket_impl(std::move(fd)));
        i->second.set_value(connected_socket(std::move(csi)), std::move(addr));
        sockets.erase(i);
    } else {
        conn_q.emplace(std::piecewise_construct, std::make_tuple(sa.as_posix_sockaddr_in()), std::make_tuple(std::move(fd), std::move(addr)));
    }
}

void reactor::complete_epoll_event(pollable_fd_state& pfd, promise<> pollable_fd_state::*pr,
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

future<struct stat>
reactor::stat(file& f) {
    return _thread_pool.submit<struct stat>([&f] {
        struct stat st;
        auto ret = ::fstat(f._fd, &st);
        throw_system_error_on(ret == -1);
        return (st);
    });
}

void reactor::add_timer(timer* tmr) {
    if (_timers.insert(*tmr)) {
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
        for (auto& t : _timers.expired_set()) {
            t._queued = false;
        }
        while (auto t = _timers.pop_expired()) {
            if (t->_armed) {
                t->_armed = false;
                if (t->_period) {
                    t->arm_periodic(*t->_period);
                }
                t->_callback();
            }
        }
        if (!_timers.empty()) {
            itimerspec its;
            its.it_interval = {};
            its.it_value = to_timespec(_timers.get_next_timeout());
            _timerfd.get_file_desc().timerfd_settime(TFD_TIMER_ABSTIME, its);
        }
        complete_timers();
    });
}

void reactor::stop() {
    assert(engine._id == 0);
    auto sem = new semaphore(0);
    for (unsigned i = 1; i < smp::count; i++) {
        smp::submit_to<>(i, []() {
            engine._stopped = true;
        }).then([sem, i]() {
            sem->signal();
        });
    }
    sem->wait(smp::count - 1).then([sem, this](){
        _stopped = true;
        delete sem;
    });
}

void reactor::exit(int ret) {
    smp::submit_to(0, [this, ret] { _return = ret; stop(); });
}

int reactor::run() {
    _io_eventfd.wait().then([this] (size_t count) {
        process_io(count);
    });
    if (_handle_sigint && _id == 0) {
        receive_signal(SIGINT).then([this] { stop(); });
    }
    _start_promise.set_value();
    complete_timers();
    while (true) {
        while (!_pending_tasks.empty()) {
            auto tsk = std::move(_pending_tasks.front());
            _pending_tasks.pop_front();
            tsk->run();
            tsk.reset();
        }
        if (_stopped) {
            if (_id == 0) {
                smp::join_all();
            }
            break;
        }
        std::array<epoll_event, 128> eevt;
        int nr = ::epoll_wait(_epollfd.get(), eevt.data(), eevt.size(), -1);
        if (nr == -1 && errno == EINTR) {
            continue; // gdb can cause this
        }
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
    return _return;
}

future<>
reactor::receive_signal(int signo) {
    auto i = _signal_handlers.emplace(signo, signo).first;
    signal_handler& sh = i->second;
    return sh._signalfd.read_some(reinterpret_cast<char*>(&sh._siginfo), sizeof(sh._siginfo)).then([&sh] (size_t ignore) {
        sh._promise.set_value();
        sh._promise = promise<>();
        return make_ready_future<>();
    });
}

reactor::signal_handler::signal_handler(int signo)
    : _signalfd(file_desc::signalfd(make_sigset_mask(signo), SFD_CLOEXEC | SFD_NONBLOCK)) {
    auto mask = make_sigset_mask(signo);
    auto r = ::sigprocmask(SIG_BLOCK, &mask, NULL);
    throw_system_error_on(r == -1);
}

inter_thread_work_queue::inter_thread_work_queue()
    : _pending()
    , _completed()
    , _start_eventfd(0)
    , _complete_eventfd(0) {
}

void inter_thread_work_queue::submit_item(inter_thread_work_queue::work_item* item) {
    _queue_has_room.wait().then([this, item] {
        _pending.push(item);
        _start_eventfd.signal(1);
    });
}

void inter_thread_work_queue::complete() {
    _complete_eventfd.wait().then([this] (size_t count) {
        auto nr = _completed.consume_all([this] (work_item* wi) {
            wi->complete();
            delete wi;
        });
        _queue_has_room.signal(nr);
        complete();
    });
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
        auto nr = inter_thread_wq._pending.consume_all([this] (inter_thread_work_queue::work_item* wi) {
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

data_source posix_data_source(pollable_fd& fd) {
    return data_source(std::make_unique<posix_data_source_impl>(fd));
}

future<temporary_buffer<char>>
posix_data_source_impl::get() {
    return _fd.read_some(_buf.get_write(), _buf_size).then([this] (size_t size) {
        _buf.trim(size);
        auto ret = std::move(_buf);
        _buf = temporary_buffer<char>(_buf_size);
        return make_ready_future<temporary_buffer<char>>(std::move(ret));
    });
}

data_sink posix_data_sink(pollable_fd& fd) {
    return data_sink(std::make_unique<posix_data_sink_impl>(fd));
}

future<>
posix_data_sink_impl::put(std::vector<temporary_buffer<char>> data) {
    std::swap(data, _data);
    return do_write(0);
}

future<>
posix_data_sink_impl::do_write(size_t idx) {
    // FIXME: use writev
    return _fd.write_all(_data[idx].get(), _data[idx].size()).then([this, idx] (size_t size) mutable {
        assert(size == _data[idx].size()); // FIXME: exception? short write?
        if (++idx == _data.size()) {
            _data.clear();
            return make_ready_future<>();
        }
        return do_write(idx);
    });
}

server_socket
posix_network_stack::listen(socket_address sa, listen_options opt) {
    return server_socket(std::make_unique<posix_server_socket_impl>(sa, engine.posix_listen(sa, opt)));
}

thread_local std::unordered_map<::sockaddr_in, promise<connected_socket, socket_address>> posix_ap_server_socket_impl::sockets;
thread_local std::unordered_multimap<::sockaddr_in, posix_ap_server_socket_impl::connection> posix_ap_server_socket_impl::conn_q;

namespace std {
bool operator==(const ::sockaddr_in a, const ::sockaddr_in b) {
    return (a.sin_addr.s_addr == b.sin_addr.s_addr) && (a.sin_port == b.sin_port);
}
};

server_socket
posix_ap_network_stack::listen(socket_address sa, listen_options opt) {
    return server_socket(std::make_unique<posix_ap_server_socket_impl>(sa));
}

struct cmsg_with_pktinfo {
    struct cmsghdrcmh;
    struct in_pktinfo pktinfo;
};

std::vector<struct iovec> to_iovec(const packet& p) {
    std::vector<struct iovec> v;
    v.reserve(p.nr_frags());
    for (auto&& f : p.fragments()) {
        v.push_back({.iov_base = f.base, .iov_len = f.size});
    }
    return v;
}

class posix_udp_channel : public udp_channel_impl {
private:
    static constexpr int MAX_DATAGRAM_SIZE = 65507;
    struct recv_ctx {
        struct msghdr _hdr;
        struct iovec _iov;
        socket_address _src_addr;
        char* _buffer;
        cmsg_with_pktinfo _cmsg;

        recv_ctx() {
            memset(&_hdr, 0, sizeof(_hdr));
            _hdr.msg_iov = &_iov;
            _hdr.msg_iovlen = 1;
            _hdr.msg_name = &_src_addr.u.sa;
            _hdr.msg_namelen = sizeof(_src_addr.u.sas);
            memset(&_cmsg, 0, sizeof(_cmsg));
            _hdr.msg_control = &_cmsg;
            _hdr.msg_controllen = sizeof(_cmsg);
        }

        void prepare() {
            _buffer = new char[MAX_DATAGRAM_SIZE];
            _iov.iov_base = _buffer;
            _iov.iov_len = MAX_DATAGRAM_SIZE;
        }
    };
    struct send_ctx {
        struct msghdr _hdr;
        std::vector<struct iovec> _iovecs;
        socket_address _dst;
        packet _p;

        send_ctx() {
            memset(&_hdr, 0, sizeof(_hdr));
            _hdr.msg_name = &_dst.u.sa;
            _hdr.msg_namelen = sizeof(_dst.u.sas);
        }

        void prepare(ipv4_addr dst, packet p) {
            _dst = make_ipv4_address(dst);
            _p = std::move(p);
            _iovecs = std::move(to_iovec(_p));
            _hdr.msg_iov = _iovecs.data();
            _hdr.msg_iovlen = _iovecs.size();
        }
    };
    std::unique_ptr<pollable_fd> _fd;
    ipv4_addr _address;
    recv_ctx _recv;
    send_ctx _send;
    bool _closed;
public:
    posix_udp_channel(ipv4_addr bind_address)
            : _closed(false) {
        auto sa = make_ipv4_address(bind_address);
        file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        bool pktinfo_flag = true;
        ::setsockopt(fd.get(), SOL_IP, IP_PKTINFO, &pktinfo_flag, sizeof(pktinfo_flag));
        fd.bind(sa.u.sa, sizeof(sa.u.sas));
        _address = ipv4_addr(fd.get_address());
        _fd = std::make_unique<pollable_fd>(std::move(fd));
    }
    virtual ~posix_udp_channel() {};
    virtual future<udp_datagram> receive() override;
    virtual future<> send(ipv4_addr dst, const char *msg);
    virtual future<> send(ipv4_addr dst, packet p);
    virtual void close() override {
        _closed = true;
        _fd.reset();
    }
    virtual bool is_closed() const override { return _closed; }
};

future<> posix_udp_channel::send(ipv4_addr dst, const char *message) {
    auto len = strlen(message);
    return _fd->sendto(make_ipv4_address(dst), message, len)
            .then([len] (size_t size) { assert(size == len); });
}

future<> posix_udp_channel::send(ipv4_addr dst, packet p) {
    auto len = p.len();
    _send.prepare(dst, std::move(p));
    return _fd->sendmsg(&_send._hdr)
            .then([len] (size_t size) { assert(size == len); });
}

udp_channel
posix_network_stack::make_udp_channel(ipv4_addr addr) {
    return udp_channel(std::make_unique<posix_udp_channel>(addr));
}

class posix_datagram : public udp_datagram_impl {
private:
    ipv4_addr _src;
    ipv4_addr _dst;
    packet _p;
public:
    posix_datagram(ipv4_addr src, ipv4_addr dst, packet p) : _src(src), _dst(dst), _p(std::move(p)) {}
    virtual ipv4_addr get_src() override { return _src; }
    virtual ipv4_addr get_dst() override { return _dst; }
    virtual uint16_t get_dst_port() override { return _dst.port; }
    virtual packet& get_data() override { return _p; }
};

future<udp_datagram>
posix_udp_channel::receive() {
    _recv.prepare();
    return _fd->recvmsg(&_recv._hdr).then([this] (size_t size) {
        auto dst = ipv4_addr(_recv._cmsg.pktinfo.ipi_addr.s_addr, _address.port);
        return make_ready_future<udp_datagram>(udp_datagram(std::make_unique<posix_datagram>(
            _recv._src_addr, dst, packet(fragment{_recv._buffer, size}, [buf = _recv._buffer] { delete[] buf; }))));
    });
}

void network_stack_registry::register_stack(sstring name,
        boost::program_options::options_description opts,
        std::function<std::unique_ptr<network_stack> (options opts)> create, bool make_default) {
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

std::unique_ptr<network_stack>
network_stack_registry::create(options opts) {
    return create(_default(), opts);
}

std::unique_ptr<network_stack>
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
        ;
    opts.add(network_stack_registry::options_description());
    return opts;
}

network_stack_registrator nsr_posix{"posix",
    boost::program_options::options_description(),
    [](boost::program_options::variables_map ops) {
        return smp::main_thread() ? posix_network_stack::create(ops) : posix_ap_network_stack::create(ops);
    },
    true
};

boost::program_options::options_description
smp::get_options_description()
{
    namespace bpo = boost::program_options;
    bpo::options_description opts("SMP options");
    opts.add_options()
        ("smp,c", bpo::value<unsigned>(), "number of threads")
        ;
    return opts;
}

std::vector<posix_thread> smp::_threads;
inter_thread_work_queue** smp::_qs;
std::thread::id smp::_tmain;
unsigned smp::count = 1;

void smp::listen_one(inter_thread_work_queue& q, std::unique_ptr<readable_eventfd>&& rfd, std::unique_ptr<writeable_eventfd>&& wfd) {
    auto f = rfd->wait();
    f.then([&q, rfd = std::move(rfd), wfd = std::move(wfd)](size_t count) mutable {
        auto nr = q._pending.consume_all([&q, &rfd, &wfd] (inter_thread_work_queue::work_item* wi) {
            wi->process();
            q._completed.push(wi);
        });
        wfd->signal(nr);
        smp::listen_one(q, std::move(rfd), std::move(wfd));
    });
}

void smp::listen_all(inter_thread_work_queue* qs)
{
    for (unsigned i = 0; i < smp::count; i++) {
        listen_one(qs[i],
                std::make_unique<readable_eventfd>(qs[i]._start_eventfd.read_side()),
                std::make_unique<writeable_eventfd>(qs[i]._complete_eventfd.write_side()));
    }
}

void smp::start_all_queues()
{
    for (unsigned c = 0; c < count; c++) {
        _qs[c][engine._id].start();
    }
    listen_all(_qs[engine._id]);
}

void smp::configure(boost::program_options::variables_map configuration)
{
    smp::count = 1;
    smp::_tmain = std::this_thread::get_id();
    engine.configure(configuration);
    if (configuration.count("smp")) {
        smp::count = configuration["smp"].as<unsigned>();

        smp::_qs = new inter_thread_work_queue* [smp::count];
        for(unsigned i = 0; i < smp::count; i++) {
            smp::_qs[i] = new inter_thread_work_queue[smp::count];
        }

        for (unsigned i = 1; i < smp::count; i++) {
            _threads.emplace_back([configuration, i] {
                sigset_t mask;
                sigfillset(&mask);
                auto r = ::sigprocmask(SIG_BLOCK, &mask, NULL);
                throw_system_error_on(r == -1);
                engine._id = i;
                engine.configure(configuration);
                engine.when_started().then([i] {
                    start_all_queues();
                });
                engine.run();
            });
        }
        start_all_queues();
    }
}

void smp::join_all()
{
    for (auto&& t: smp::_threads) {
        t.join();
    }
}


thread_local reactor engine;
