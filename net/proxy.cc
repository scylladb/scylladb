#include "core/reactor.hh"
#include "proxy.hh"
#include <utility>

namespace net {

class proxy_net_device : public qp {
private:
    static constexpr size_t _send_queue_length = 128;
    size_t _send_depth = 0;
    unsigned _cpu;
    device* _dev;
    std::vector<packet> _moving;
public:
    explicit proxy_net_device(unsigned cpu, device* dev);
    virtual future<> send(packet p) override {
        abort();
    }
    virtual uint32_t send(circular_buffer<packet>& p) override;
};

proxy_net_device::proxy_net_device(unsigned cpu, device* dev) :
        _cpu(cpu),
        _dev(dev)
{
    _moving.reserve(_send_queue_length);
}

uint32_t proxy_net_device::send(circular_buffer<packet>& p)
{
    if (!_moving.empty() || _send_depth  == _send_queue_length) {
        return 0;
    }

    for (size_t i = 0; !p.empty() && _send_depth < _send_queue_length; i++, _send_depth++) {
        _moving.push_back(std::move(p.front()));
        p.pop_front();
    }

    if (!_moving.empty()) {
        qp* dev = &_dev->queue_for_cpu(_cpu);
        auto cpu = engine.cpu_id();
        smp::submit_to(_cpu, [this, dev, cpu]() mutable {
            for(size_t i = 0; i < _moving.size(); i++) {
                dev->proxy_send(_moving[i].free_on_cpu(cpu, [this] { _send_depth--; }));
            }
        }).then([this] {
            _moving.clear();
        });
    }

    return _moving.size();
}

std::unique_ptr<qp> create_proxy_net_device(unsigned master_cpu, device* dev) {
    return std::make_unique<proxy_net_device>(master_cpu, dev);
}
}
