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
    memory::set_reclaim_hook([this] (std::function<void ()> reclaim_fn) {
        // push it in the front of the queue so we reclaim memory quickly
        _pending_tasks.push_front(make_task([fn = std::move(reclaim_fn)] {
            fn();
        }));
    });
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

void reactor::add_timer(timer* tmr) {
    if (_timers.insert(*tmr)) {
        itimerspec its;
        its.it_interval = {};
        its.it_value = to_timespec(_timers.get_next_timeout());
        _timerfd.get_file_desc().timerfd_settime(TFD_TIMER_ABSTIME, its);
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

void reactor::complete_timers() {
    _timerfd.read_some(reinterpret_cast<char*>(&_timers_completed), sizeof(_timers_completed)).then(
            [this] (size_t n) {
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
    uint64_t tasks_processed = 0;
    scollectd::registration regs[] = {
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
                    , scollectd::make_typed(scollectd::data_type::DERIVE, tasks_processed)
            ),
            // queue_length     value:GAUGE:0:U
            // Absolute value of num timers in queue.
            scollectd::add_polled_metric(scollectd::type_instance_id("reactor"
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "timers-pending")
                    , scollectd::make_typed(scollectd::data_type::GAUGE
                            , std::bind(&decltype(_timers)::size, &_timers))
            ),
    };

    _io_eventfd.wait().then([this] (size_t count) {
        process_io(count);
    });
    if (_handle_sigint && _id == 0) {
        receive_signal(SIGINT).then([this] { stop(); });
    }
    _network_stack->initialize().then([this]() {
        _start_promise.set_value();
    });
    complete_timers();
    while (true) {
        unsigned loop = 0;
        while (!_pending_tasks.empty() && loop++ < 200) {
            auto tsk = std::move(_pending_tasks.front());
            _pending_tasks.pop_front();
            tsk->run();
            tsk.reset();
            ++tasks_processed;
        }
        if (_stopped) {
            if (_id == 0) {
                smp::join_all();
            }
            break;
        }

        smp::poll_queues();

        if (_pending_tasks.empty()) {
            _idle.store(true, std::memory_order_seq_cst);

            if (smp::poll_queues() && !_pending_tasks.empty()) {
                _idle.store(false, std::memory_order_relaxed);
            }
        }

        std::array<epoll_event, 128> eevt;
        int nr = ::epoll_wait(_epollfd.get(), eevt.data(), eevt.size(), _pending_tasks.empty() ? -1 : 0);
        if (_pending_tasks.empty()) { // _idle already false, so avoid dirtying cache line
            _idle.store(false, std::memory_order_relaxed);
        }
        if (nr == -1 && errno == EINTR) {
            continue; // gdb can cause this
        }
        assert(nr != -1);
        for (int i = 0; i < nr; ++i) {
            auto& evt = eevt[i];
            auto pfd = reinterpret_cast<pollable_fd_state*>(evt.data.ptr);
            auto events = evt.events & (EPOLLIN | EPOLLOUT);
            // FIXME: it is enough to check that pfd's task is not in _pending_tasks here
            if (_pending_tasks.empty()) {
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
    , _start_eventfd(0)
    , _complete_eventfd(0)
    , _complete_eventfd_write(_complete_eventfd.write_side())
    , _start_eventfd_read(_start_eventfd.read_side()) {
}

void smp_message_queue::submit_kick() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (_pending_peer->idle()) {
        _start_eventfd.signal(1);
    }
}

void smp_message_queue::complete_kick() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (_complete_peer->idle()) {
        _complete_eventfd_write.signal(1);
    }
}

void smp_message_queue::move_pending() {
    bool kick = false;

    while (_current_queue_length < queue_length && !_pending_fifo.empty()) {
        _pending.push(_pending_fifo.front());
        _pending_fifo.pop();
        _current_queue_length++;
        kick = true;
    }

    if (kick) {
        submit_kick();
    }
}

void smp_message_queue::submit_item(smp_message_queue::work_item* item) {
    _pending_fifo.push(item);
    move_pending();
}

void smp_message_queue::respond(work_item* item) {
    // FIXME: batcing
    _completed.push(item);
    complete_kick();
}

size_t smp_message_queue::process_completions() {
    auto nr = _completed.consume_all([this] (work_item* wi) {
        wi->complete();
        delete wi;
    });

    _current_queue_length -= nr;

    move_pending();

    return nr;
}

void smp_message_queue::complete() {
    _complete_eventfd.wait().then([this] (size_t count) {
        process_completions();
        complete();
    });
}

size_t smp_message_queue::process_incoming() {
    return _pending.consume_all([this] (smp_message_queue::work_item* wi) {
        wi->process().then([this, wi] {
            respond(wi);
        });
    });
}

void smp_message_queue::start() {
    _complete_peer = &engine;
    complete();
}

void smp_message_queue::listen() {
    _start_eventfd_read.wait().then([this] (size_t count) mutable {
        process_incoming();
        listen();
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
    rc.cpus = smp::count;
    std::vector<resource::cpu> allocations = resource::allocate(rc);
    pin_this_thread(allocations[0].cpu_id);
    memory::configure(allocations[0].mem);
    smp::_qs = new smp_message_queue* [smp::count];
    for(unsigned i = 0; i < smp::count; i++) {
        smp::_qs[i] = new smp_message_queue[smp::count];
    }

    boost::barrier inited(smp::count);

    for (unsigned i = 1; i < smp::count; i++) {
        auto allocation = allocations[i];
        _threads.emplace_back([configuration, i, allocation, &inited] {
            pin_this_thread(allocation.cpu_id);
            memory::configure(allocation.mem);
            sigset_t mask;
            sigfillset(&mask);
            auto r = ::sigprocmask(SIG_BLOCK, &mask, NULL);
            throw_system_error_on(r == -1);
            engine._id = i;
            start_all_queues();
            engine.configure(configuration);
            inited.wait();
            engine.run();
        });
    }
    inited.wait();
    start_all_queues();
    engine.configure(configuration);
}

void smp::join_all()
{
    for (auto&& t: smp::_threads) {
        t.join();
    }
}

__thread size_t future_avail_count = 0;

thread_local reactor engine;
