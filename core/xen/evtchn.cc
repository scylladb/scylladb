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
    if (_port != -1) {
        _evtchn->port_moved(_port, &other, this);
    }
}

port::~port() {
    // FIXME: unbind from Xen
    if (_port != -1) {
        _evtchn->port_deleted(_port, this);
    }
}

port& port::operator=(port&& other) {
    if (this != &other) {
        this->~port();
        new (this) port(std::move(other));
    }
    return *this;
}

future<> port::pending() {
    return _sem.wait();
}

void port::notify() {
    _evtchn->notify(_port);
}

void port::umask() {
    _evtchn->umask(&_port, 1);
}

class userspace_evtchn: public evtchn {
    pollable_fd _evtchn;
    int ports[2];
protected:
    virtual void umask(int *port, unsigned count) override;
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
        return _evtchn.read_some(reinterpret_cast<char *>(&ports), sizeof(ports)).then([this] (size_t s)
        {
            auto count = s / sizeof(ports[0]);
            for (unsigned i = 0; i < count; ++i) {
                make_ready_port(ports[i]);
            }
            umask(ports, count);
            return make_ready_future<>();
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
    // We need to convert extra-seastar events to intra-seastar events
    // (in this case, the semaphore interface of evtchn). The interface for
    // that currently is the reactor notifier.
    std::unique_ptr<reactor_notifier> _notified;
    static void make_ready(void *arg);
    void process_interrupts(int port);
public:
    kernel_evtchn(unsigned otherend)
        : evtchn(otherend)
        , _notified(engine().make_reactor_notifier()) {}
    virtual port bind() override;
    virtual void notify(int port) override;
};

void kernel_evtchn::make_ready(void *arg) {
    auto notifier = reinterpret_cast<reactor_notifier *>(arg);
    notifier->signal();
}

port kernel_evtchn::bind() {

    unsigned int irq;

    int newp;
    irq = bind_listening_port_to_irq(_otherend, &newp);

    port p(newp);

    auto notifier = reinterpret_cast<void *>(_notified.get());

    intr_add_handler("", irq, NULL, make_ready, notifier, 0, 0);
    unmask_evtchn(newp);
    process_interrupts(newp);
    return p;
}

void kernel_evtchn::process_interrupts(int port) {
    _notified->wait().then([this, port] () {
        make_ready_port(port);
        process_interrupts(port);
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
