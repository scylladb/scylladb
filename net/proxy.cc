#include "core/reactor.hh"
#include "proxy.hh"
#include <utility>

namespace net {

class proxy_net_device : public qp {
private:
    static constexpr size_t _send_queue_length = 1000;
    size_t _send_depth = 0;
    promise<> _send_promise;
    unsigned _cpu;
    distributed_device* _dev;
public:
    explicit proxy_net_device(unsigned cpu, distributed_device* dev);
    virtual future<> send(packet p) override;
};

proxy_net_device::proxy_net_device(unsigned cpu, distributed_device* dev) :
        _cpu(cpu),
        _dev(dev)
{
}

future<> proxy_net_device::send(packet p)
{
    if (_send_depth < _send_queue_length) {
        _send_depth++;

        qp* dev = &_dev->queue_for_cpu(_cpu);
        auto cpu = engine.cpu_id();
        smp::submit_to(_cpu, [dev, p = std::move(p), cpu]() mutable {
            return dev->send(p.free_on_cpu(cpu));
        }).then([this] () {
            if (_send_depth == _send_queue_length) {
                _send_promise.set_value();
            }
            _send_depth--;
        });

        if (_send_depth == _send_queue_length) {
            _send_promise = promise<>();
            return _send_promise.get_future();
        }
    }
    return make_ready_future();
}

std::unique_ptr<qp> create_proxy_net_device(unsigned master_cpu, distributed_device* dev) {
    return std::make_unique<proxy_net_device>(master_cpu, dev);
}
}
