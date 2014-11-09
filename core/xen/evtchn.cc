#include <stdint.h>
#include <unistd.h>
#include <xen/xen.h>
#include <xen/event_channel.h> // kernel interface
#include <xen/sys/evtchn.h>    // userspace interface

#include "core/reactor.hh"
#include "core/semaphore.hh"
#include "core/future-util.hh"
#include "evtchn.hh"
#include "osv_xen.hh"

namespace xen {

void evtchn::make_ready_port(int port) {
    auto ports = _ports.equal_range(port);
    for (auto i = ports.first; i != ports.second; ++i) {
        i->second->_sem.signal();
    }
}

void evtchn::port_moved(int prt, port* old, port* now) {
    auto ports = _ports.equal_range(prt);
    for (auto i = ports.first; i != ports.second; ++i) {
        if (i->second == old) {
            i->second = now;
        }
    }
}

void evtchn::port_deleted(int prt, port* obj) {
    auto ports = _ports.equal_range(prt);
    for (auto i = ports.first; i != ports.second; ++i) {
        if (i->second == obj) {
            i = _ports.erase(i);
        }
    }
}

port::port(int p)
    : _port(p), _sem(0), _evtchn(evtchn::instance()) {
    _evtchn->_ports.emplace(p, this);
}

port::port(port&& other)
    : _port(other._port), _sem(std::move(other._sem)), _evtchn(other._evtchn) {
    _evtchn->port_moved(_port, &other, this);
}

port::~port() {
    // FIXME: unbind from Xen
    _evtchn->port_deleted(_port, this);
}

future<> port::pending() {
    return _sem.wait();
}

void port::notify() {
    _evtchn->notify(_port);
}

class userspace_evtchn: public evtchn {
    pollable_fd _evtchn;
    void umask(int *port, unsigned count);
public:
    userspace_evtchn(unsigned otherend);
    virtual port bind() override;
    virtual void notify(int port) override;
};

userspace_evtchn::userspace_evtchn(unsigned otherend)
    : evtchn(otherend)
    , _evtchn(pollable_fd(file_desc::open("/dev/xen/evtchn", O_RDWR | O_NONBLOCK)))
{
    keep_doing([this] {
        int ports[2];
        return _evtchn.read_some(reinterpret_cast<char *>(&ports), sizeof(ports)).then([this, &ports] (size_t s)
        {
            auto count = s / sizeof(ports[0]);
            umask(ports, count);
            for (unsigned i = 0; i < count; ++i) {
                make_ready_port(ports[i]);
            }
            make_ready_future<>();
        });
    });
}

port userspace_evtchn::bind()
{
    struct ioctl_evtchn_bind_unbound_port bind = { _otherend };

    auto ret = _evtchn.get_file_desc().ioctl<struct ioctl_evtchn_bind_unbound_port>(IOCTL_EVTCHN_BIND_UNBOUND_PORT, bind);
    return port(ret);
}

void userspace_evtchn::notify(int port)
{
    struct ioctl_evtchn_notify notify;
    notify.port = port;

    _evtchn.get_file_desc().ioctl<struct ioctl_evtchn_notify>(IOCTL_EVTCHN_NOTIFY, notify);
}

void userspace_evtchn::umask(int *port, unsigned count)
{
    _evtchn.get_file_desc().write(port, count * sizeof(port));
}

#ifdef HAVE_OSV
class kernel_evtchn: public evtchn {
    static void make_ready(void *arg);
    void process_interrupts(readable_eventfd* fd, int port);
public:
    kernel_evtchn(unsigned otherend) : evtchn(otherend) {}
    virtual port bind() override;
    virtual void notify(int port) override;
};

void kernel_evtchn::make_ready(void *arg) {
    int fd = reinterpret_cast<uintptr_t>(arg);
    uint64_t one = 1;
    ::write(fd, &one, sizeof(one));
}

port kernel_evtchn::bind() {

    unsigned int irq;

    int newp;
    irq = bind_listening_port_to_irq(_otherend, &newp);

    port p(newp);
    // We need to convert extra-seastar events to intra-seastar events
    // (in this case, the semaphore interface of evtchn).  The only
    // way to do that currently is via eventfd.
    auto fd = new readable_eventfd;
    auto wfd = fd->get_write_fd();
    intr_add_handler("", irq, NULL, make_ready, reinterpret_cast<void*>(uintptr_t(wfd)), 0, 0);
    unmask_evtchn(newp);
    process_interrupts(fd, newp);
    return p;
}

void kernel_evtchn::process_interrupts(readable_eventfd* fd, int port) {
    fd->wait().then([this, fd, port] (size_t ignore) {
        make_ready_port(port);
        process_interrupts(fd, port);
    });
}

void kernel_evtchn::notify(int port) {
    notify_remote_via_evtchn(port);
}
#endif

evtchn *evtchn::_instance = nullptr;

evtchn *evtchn::instance()
{
    if (!_instance) {
        throw std::runtime_error("Acquiring evtchn instance without specifying otherend: invalid context");
    }

    return _instance;
}

evtchn *evtchn::instance(bool userspace, unsigned otherend)
{
    if (!_instance) {
#ifdef HAVE_OSV
        if (!userspace) {
            _instance = new kernel_evtchn(otherend);
        } else
#endif
            _instance = new userspace_evtchn(otherend);
    }
    return _instance;
}

}
