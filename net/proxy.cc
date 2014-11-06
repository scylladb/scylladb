#include "core/reactor.hh"
#include "proxy.hh"
#include <utility>

namespace net {

class proxy_net_device : public net::device {
private:
    static constexpr size_t _send_queue_length = 1000;
    boost::program_options::variables_map _opts;
    stream<packet> _rx_stream;
    net::hw_features _hw_features;
    size_t _send_depth = 0;
    promise<> _send_promise;
public:
    explicit proxy_net_device(boost::program_options::variables_map opts);
    virtual subscription<packet> receive(std::function<future<> (packet)> next) override;
    virtual future<> send(packet p) override;
    virtual ethernet_address hw_address() override { return {{{ 0x12, 0x23, 0x34, 0x56, 0x67, 0x78 }}}; }
    virtual net::hw_features hw_features() override { return _hw_features; };
    virtual future<> l2inject(packet p) override;
};

proxy_net_device::proxy_net_device(boost::program_options::variables_map opts) :
        _opts(std::move(opts)),
        _rx_stream()
{
    if (!(_opts.count("csum-offload") && _opts["csum-offload"].as<std::string>() == "off")) {
        _hw_features.tx_csum_offload = true;
        _hw_features.rx_csum_offload = true;
    } else {
        _hw_features.tx_csum_offload = false;
        _hw_features.rx_csum_offload = false;
    }
    if (!(_opts.count("tso") && _opts["tso"].as<std::string>() == "off")) {
        _hw_features.tx_tso = true;
    } else {
        _hw_features.tx_tso = false;
    }
    if (!(_opts.count("ufo") && _opts["ufo"].as<std::string>() == "off")) {
        _hw_features.tx_ufo = true;
    } else {
        _hw_features.tx_ufo = false;
    }
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

        // Assumes that there is only one virtio device and it is on cpu 0
        smp::submit_to(0, [p = std::move(p), cpu = engine.cpu_id()]() mutable {
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

std::unique_ptr<device> create_proxy_net_device(boost::program_options::variables_map opts) {
    auto ptr = std::make_unique<proxy_net_device>(opts);
    // This assumes only one device per cpu. Will need to be fixed when
    // this assumption will no longer hold.
    dev = ptr.get();
    return std::move(ptr);
}
}
