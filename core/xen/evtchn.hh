#ifndef _XEN_EVTCHN_HH
#define _XEN_EVTCHN_HH

#include "core/posix.hh"
#include "core/future.hh"

class evtchn {
    static evtchn *_instance;
    inline semaphore *port_to_sem(int port) {
        auto handle = _promises.find(port);
        if (handle == _promises.end()) {
            throw std::runtime_error("listening on unbound port");
        }
        return &((*handle).second);
    }

protected:
    unsigned _otherend;
    semaphore* init_port(int port);
    void make_ready_port(int port);
    std::unordered_map<int, semaphore> _promises;
public:
    static evtchn *instance(bool userspace, unsigned otherend);
    static evtchn *instance();
    evtchn(unsigned otherend) : _otherend(otherend) {}
    virtual int bind() = 0;
    virtual void notify(int port) = 0;

    future<> pending(int port);
};
#endif
