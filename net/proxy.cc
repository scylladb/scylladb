#include "core/reactor.hh"
#include "proxy.hh"
#include <utility>

namespace net {

class proxy_net_device : public slave_device {
private:
    static constexpr size_t _send_queue_length = 1000;
    size_t _send_depth = 0;
    promise<> _send_promise;
    unsigned _cpu;
    device* _dev;
public:
    explicit proxy_net_device(unsigned cpu, device *dev);
    virtual future<> send(packet p) override;
    virtual ethernet_address hw_address() override { return _dev->hw_address(); }
    virtual net::hw_features hw_features() override { return _dev->hw_features(); };
    virtual device* cpu2device(unsigned cpu) override { return (cpu == _cpu) ? _dev : _dev->cpu2device(cpu); }
};

proxy_net_device::proxy_net_device(unsigned cpu, device* dev) :
        _cpu(cpu),
        _dev(dev)
{
}

future<> proxy_net_device::send(packet p)
{
    if (_send_depth < _send_queue_length) {
        _send_depth++;

        smp::submit_to(_cpu, [dev = _dev, p = std::move(p), cpu = engine.cpu_id()]() mutable {
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

std::unique_ptr<slave_device> create_proxy_net_device(unsigned cpu, master_device* dev) {
    return std::make_unique<proxy_net_device>(cpu, dev);
}
}
