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
class device;
class l3_protocol;

struct hw_features {
    // Enable tx checksum offload
    bool tx_csum_offload = false;
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
    std::unique_ptr<device> _dev;
    subscription<packet> _rx;
    ethernet_address _hw_address;
    net::hw_features _hw_features;
private:
    future<> dispatch_packet(packet p);
    future<> send(eth_protocol_num proto_num, ethernet_address to, packet p);
    void forward(unsigned cpuid, packet p);
public:
    explicit interface(std::unique_ptr<device> dev);
    ethernet_address hw_address() { return _hw_address; }
    net::hw_features hw_features() { return _hw_features; }
    subscription<packet, ethernet_address> register_l3(eth_protocol_num proto_num,
            std::function<future<> (packet p, ethernet_address from)> next,
            std::function<unsigned (packet&, size_t)> forward);
    friend class l3_protocol;
};

class device {
protected:
    stream<packet> _rx_stream;
public:
    device() : _rx_stream() { _rx_stream.started(); }
    virtual ~device() {}
    virtual subscription<packet> receive(std::function<future<> (packet)> next_packet) {
        return _rx_stream.listen(std::move(next_packet));
    }
    virtual void l2inject(packet p) {
        _rx_stream.produce(std::move(p));
    }
    virtual future<> send(packet p) = 0;
    virtual device* cpu2device(unsigned cpu) = 0;
    virtual bool may_forward() = 0;
    virtual ethernet_address hw_address() = 0;
    virtual net::hw_features hw_features() = 0;
};

class slave_device : public device {
public:
    virtual ~slave_device() {}
    virtual bool may_forward() override { return false; }
};

class master_device : public device {
private:
    std::vector<slave_device*> slaves;
public:
    virtual ~master_device() {}
    master_device() { slaves.resize(smp::count, nullptr); };
    virtual device* cpu2device(unsigned cpu) { return slaves[cpu]; }
    virtual void enslave(unsigned cpu, slave_device* dev) { slaves[cpu] = dev; }
    virtual bool may_forward() override { return true; }
};

struct device_placement {
    std::unique_ptr<net::master_device> device;
    std::set<unsigned> slaves_placement;
};
}

#endif /* NET_HH_ */
