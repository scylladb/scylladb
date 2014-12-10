/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef NET_HH_
#define NET_HH_

#include "core/reactor.hh"
#include "core/deleter.hh"
#include "core/queue.hh"
#include "core/stream.hh"
#include "ethernet.hh"
#include "packet.hh"
#include "const.hh"
#include <unordered_map>

namespace net {

class packet;
class interface;
class distributed_device;
class device;
class l3_protocol;

struct hw_features {
    // Enable tx ip header checksum offload
    bool tx_csum_ip_offload = false;
    // Enable tx l4 (TCP or UDP) checksum offload
    bool tx_csum_l4_offload = false;
    // Enable rx checksum offload
    bool rx_csum_offload = false;
    // Enable tx TCP segment offload
    bool tx_tso = false;
    // Enable tx UDP fragmentation offload
    bool tx_ufo = false;
    // Maximum Transmission Unit
    uint16_t mtu = 1500;
    // Maximun packet len when TCP/UDP offload is enabled
    uint16_t max_packet_len = net::ip_packet_len_max - net::eth_hdr_len;
};

class l3_protocol {
    interface* _netif;
    eth_protocol_num _proto_num;
public:
    explicit l3_protocol(interface* netif, eth_protocol_num proto_num);
    subscription<packet, ethernet_address> receive(
            std::function<future<> (packet, ethernet_address)> rx_fn,
            std::function<unsigned (packet&, size_t)> forward);
    future<> send(ethernet_address to, packet p);
private:
    friend class interface;
};

class interface {
    struct l3_rx_stream {
        stream<packet, ethernet_address> packet_stream;
        future<> ready;
        std::function<unsigned (packet&, size_t)> forward;
        l3_rx_stream(std::function<unsigned (packet&, size_t)>&& fw) : ready(packet_stream.started()), forward(fw) {}
    };
    std::unordered_map<uint16_t, l3_rx_stream> _proto_map;
    std::shared_ptr<distributed_device> _dev;
    subscription<packet> _rx;
    ethernet_address _hw_address;
    net::hw_features _hw_features;
private:
    future<> dispatch_packet(packet p);
    future<> send(eth_protocol_num proto_num, ethernet_address to, packet p);
public:
    explicit interface(std::shared_ptr<distributed_device> dev);
    ethernet_address hw_address() { return _hw_address; }
    net::hw_features hw_features() { return _hw_features; }
    subscription<packet, ethernet_address> register_l3(eth_protocol_num proto_num,
            std::function<future<> (packet p, ethernet_address from)> next,
            std::function<unsigned (packet&, size_t)> forward);
    void forward(unsigned cpuid, packet p);
    friend class l3_protocol;
};

class device {
    std::vector<unsigned> proxies;
    stream<packet> _rx_stream;
public:
    virtual ~device() {}
    virtual future<> send(packet p) = 0;
    virtual void rx_start() {};
    bool may_forward() { return !proxies.empty(); }
    void add_proxy(unsigned cpu) { proxies.push_back(cpu); }
    friend class distributed_device;
};

class distributed_device {
protected:
    std::unique_ptr<device*[]> _queues;
public:
    distributed_device() {
        _queues = std::make_unique<device*[]>(smp::count);
    }
    virtual ~distributed_device() {};
    device& queue_for_cpu(unsigned cpu) { return *_queues[cpu]; }
    device& local_queue() { return queue_for_cpu(engine.cpu_id()); }
    void l2receive(packet p) { _queues[engine.cpu_id()]->_rx_stream.produce(std::move(p)); }
    subscription<packet> receive(std::function<future<> (packet)> next_packet) {
        auto sub = _queues[engine.cpu_id()]->_rx_stream.listen(std::move(next_packet));
        _queues[engine.cpu_id()]->rx_start();
        return std::move(sub);
    }
    virtual ethernet_address hw_address() = 0;
    virtual net::hw_features hw_features() = 0;
    virtual void init_local_queue(boost::program_options::variables_map opts) = 0;
protected:
    void set_local_queue(std::unique_ptr<device> dev) {
        _queues[engine.cpu_id()] = dev.get();
        engine.at_exit([dev = std::move(dev)] {});
    }
};

struct device_placement {
    std::unique_ptr<net::device> device;
    std::set<unsigned> slaves_placement;
};
}

#endif /* NET_HH_ */
