#ifndef _XEN_EVTCHN_HH
#define _XEN_EVTCHN_HH

#include "core/posix.hh"
#include "core/future.hh"

typedef std::list<semaphore *> semlist;

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
};

class evtchn {
    static evtchn *_instance;
protected:
    inline semlist* port_to_sem(int port) {
        auto handle = _promises.find(port);
        if (handle == _promises.end()) {
            throw std::runtime_error("listening on unbound port");
        }
        return &((*handle).second);
    }

    unsigned _otherend;
    semlist* init_port(port &p);
    void make_ready_port(int port);
    std::unordered_map<int, semlist> _promises;
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
