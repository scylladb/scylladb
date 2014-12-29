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
class qp;
class l3_protocol;

class forward_hash {
    uint8_t data[64];
    size_t end_idx = 0;
public:
    size_t size() const {
        return end_idx;
    }
    void push_back(uint8_t b) {
        assert(end_idx < sizeof(data));
        data[end_idx++] = b;
    }
    void push_back(uint16_t b) {
        push_back(uint8_t(b));
        push_back(uint8_t(b >> 8));
    }
    void push_back(uint32_t b) {
        push_back(uint16_t(b));
        push_back(uint16_t(b >> 16));
    }
    const uint8_t& operator[](size_t idx) const {
        return data[idx];
    }
};

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
            std::function<bool (forward_hash&, packet&, size_t)> forward);
    future<> send(ethernet_address to, packet p);
private:
    friend class interface;
};

class interface {
    struct l3_rx_stream {
        stream<packet, ethernet_address> packet_stream;
        future<> ready;
        std::function<bool (forward_hash&, packet&, size_t)> forward;
        l3_rx_stream(std::function<bool (forward_hash&, packet&, size_t)>&& fw) : ready(packet_stream.started()), forward(fw) {}
    };
    std::unordered_map<uint16_t, l3_rx_stream> _proto_map;
    std::shared_ptr<device> _dev;
    subscription<packet> _rx;
    ethernet_address _hw_address;
    net::hw_features _hw_features;
private:
    future<> dispatch_packet(packet p);
    future<> send(eth_protocol_num proto_num, ethernet_address to, packet p);
public:
    explicit interface(std::shared_ptr<device> dev);
    ethernet_address hw_address() { return _hw_address; }
    net::hw_features hw_features() { return _hw_features; }
    subscription<packet, ethernet_address> register_l3(eth_protocol_num proto_num,
            std::function<future<> (packet p, ethernet_address from)> next,
            std::function<bool (forward_hash&, packet&, size_t)> forward);
    void forward(unsigned cpuid, packet p);
    unsigned hash2cpu(uint32_t hash);
    friend class l3_protocol;
};

class qp {
    std::vector<unsigned> proxies;
    stream<packet> _rx_stream;
public:
    virtual ~qp() {}
    virtual future<> send(packet p) = 0;
    virtual void rx_start() {};
    bool may_forward() { return !proxies.empty(); }
    void add_proxy(unsigned cpu) { proxies.push_back(cpu); }
    friend class device;
};

class device {
protected:
    std::unique_ptr<qp*[]> _queues;
    size_t _rss_table_bits = 0;
public:
    device() {
        _queues = std::make_unique<qp*[]>(smp::count);
    }
    virtual ~device() {};
    qp& queue_for_cpu(unsigned cpu) { return *_queues[cpu]; }
    qp& local_queue() { return queue_for_cpu(engine.cpu_id()); }
    void l2receive(packet p) { _queues[engine.cpu_id()]->_rx_stream.produce(std::move(p)); }
    subscription<packet> receive(std::function<future<> (packet)> next_packet) {
        auto sub = _queues[engine.cpu_id()]->_rx_stream.listen(std::move(next_packet));
        _queues[engine.cpu_id()]->rx_start();
        return std::move(sub);
    }
    virtual ethernet_address hw_address() = 0;
    virtual net::hw_features hw_features() = 0;
    virtual uint16_t hw_queues_count() { return 1; }
    virtual future<> link_ready() { return make_ready_future<>(); }
    virtual std::unique_ptr<qp> init_local_queue(boost::program_options::variables_map opts, uint16_t qid) = 0;
    virtual unsigned hash2qid(uint32_t hash) {
        return hash % hw_queues_count();
    }
    void set_local_queue(std::unique_ptr<qp> dev) {
        assert(!_queues[engine.cpu_id()]);
        _queues[engine.cpu_id()] = dev.get();
        engine.at_exit([dev = std::move(dev)] {});
    }
    template <typename Func>
    unsigned forward_dst(unsigned src_cpuid, Func&& hashfn) {
        auto& qp = queue_for_cpu(src_cpuid);
        if (!qp.may_forward()) {
            return src_cpuid;
        }
        auto hash = hashfn() >> _rss_table_bits;
        auto idx = hash % (qp.proxies.size() + 1);
        return idx ? qp.proxies[idx - 1] : src_cpuid;
    }
    virtual unsigned hash2cpu(uint32_t hash) {
        // there is an assumption here that qid == cpu_id which will
        // not necessary be true in the future
        return forward_dst(hash2qid(hash), [hash] { return hash; });
    }
};

}

#endif /* NET_HH_ */
