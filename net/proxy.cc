#include "core/reactor.hh"
#include "proxy.hh"
#include <utility>

namespace net {

class proxy_net_device : public slave_device {
private:
    static constexpr size_t _send_queue_length = 1000;
    boost::program_options::variables_map _opts;
    stream<packet> _rx_stream;
    size_t _send_depth = 0;
    promise<> _send_promise;
    unsigned _cpu;
    device* _dev;
public:
    explicit proxy_net_device(boost::program_options::variables_map opts, unsigned cpu, device *dev);
    virtual subscription<packet> receive(std::function<future<> (packet)> next) override;
    virtual future<> send(packet p) override;
    virtual ethernet_address hw_address() override { return _dev->hw_address(); }
    virtual net::hw_features hw_features() override { return _dev->hw_features(); };
    virtual future<> l2inject(packet p) override;
};

proxy_net_device::proxy_net_device(boost::program_options::variables_map opts, unsigned cpu, device* dev) :
        _opts(std::move(opts)),
        _rx_stream(),
        _cpu(cpu),
        _dev(dev)
{
    _rx_stream.started();
}

subscription<packet> proxy_net_device::receive(std::function<future<> (packet)> next)
{
    return _rx_stream.listen(std::move(next));
}

future<> proxy_net_device::l2inject(packet p)
{
    _rx_stream.produce(std::move(p));
    return make_ready_future<>();
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

std::unique_ptr<slave_device> create_proxy_net_device(unsigned cpu, master_device* dev, boost::program_options::variables_map opts) {
    return std::make_unique<proxy_net_device>(opts, cpu, dev);
}
}
