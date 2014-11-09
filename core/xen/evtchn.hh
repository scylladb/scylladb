#ifndef _XEN_EVTCHN_HH
#define _XEN_EVTCHN_HH

#include "core/posix.hh"
#include "core/future.hh"

class evtchn;

class port {
    int _port = -1;
    semaphore _sem;
    evtchn *_evtchn;
public:
    port(int p);
    operator int() { return _port; }
    semaphore *sem() { return &_sem; }
    future<> pending();
    void notify();

    friend class evtchn;
};

class evtchn {
    static evtchn *_instance;
protected:
    unsigned _otherend;
    void make_ready_port(int port);
    std::unordered_multimap<int, port*> _ports;
    virtual void notify(int port) = 0;
    friend class port;
public:
    static evtchn *instance(bool userspace, unsigned otherend);
    static evtchn *instance();
    evtchn(unsigned otherend) : _otherend(otherend) {}
    virtual port *bind() = 0;
    port *bind(int p) { return new port(p); };
};
#endif
