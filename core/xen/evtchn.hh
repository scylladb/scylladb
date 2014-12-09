#ifndef _XEN_EVTCHN_HH
#define _XEN_EVTCHN_HH

#include "core/posix.hh"
#include "core/future.hh"

namespace xen {

class evtchn;

class port {
    int _port = -1;
    semaphore _sem;
    evtchn *_evtchn;
public:
    explicit port(int p);
    port() = default;
    port(port&& other);
    ~port();
    port& operator=(port&& other);
    int number() const { return _port; }
    future<> pending();
    void notify();
    void umask();

    friend class evtchn;
};

class evtchn {
    static evtchn *_instance;
protected:
    unsigned _otherend;
    void make_ready_port(int port);
    void port_moved(int prt, port* old, port* now);
    void port_deleted(int prt, port* old);
    std::unordered_multimap<int, port*> _ports;
    virtual void notify(int port) = 0;
    virtual void umask(int *port, unsigned count) {};
    friend class port;
public:
    static evtchn *instance(bool userspace, unsigned otherend);
    static evtchn *instance();
    evtchn(unsigned otherend) : _otherend(otherend) {}
    virtual port bind() = 0;
    port bind(int p) { return port(p); };
};

}

#endif
