/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef NET_HH_
#define NET_HH_

#include "core/reactor.hh"
#include "core/deleter.hh"
#include "core/queue.hh"
#include "core/stream.hh"
#include "core/scollectd.hh"
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
    // LRO is enabled
    bool rx_lro = false;
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
public:
    struct l3packet {
        eth_protocol_num proto_num;
        ethernet_address to;
        packet p;
    };
    using packet_provider_type = std::function<std::experimental::optional<l3packet> ()>;
private:
    interface* _netif;
    eth_protocol_num _proto_num;
public:
    explicit l3_protocol(interface* netif, eth_protocol_num proto_num, packet_provider_type func);
    subscription<packet, ethernet_address> receive(
            std::function<future<> (packet, ethernet_address)> rx_fn,
            std::function<bool (forward_hash&, packet&, size_t)> forward);
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
    std::vector<l3_protocol::packet_provider_type> _pkt_providers;
private:
    future<> dispatch_packet(packet p);
public:
    explicit interface(std::shared_ptr<device> dev);
    ethernet_address hw_address() { return _hw_address; }
    const net::hw_features& hw_features() const { return _hw_features; }
    subscription<packet, ethernet_address> register_l3(eth_protocol_num proto_num,
            std::function<future<> (packet p, ethernet_address from)> next,
            std::function<bool (forward_hash&, packet&, size_t)> forward);
    void forward(unsigned cpuid, packet p);
    unsigned hash2cpu(uint32_t hash);
    void register_packet_provider(l3_protocol::packet_provider_type func) {
        _pkt_providers.push_back(std::move(func));
    }
    friend class l3_protocol;
};

struct qp_stats_good {
    /**
     * Update the packets bunch related statistics.
     *
     * Update the last packets bunch size and the total packets counter.
     *
     * @param count Number of packets in the last packets bunch.
     */
    void update_pkts_bunch(uint64_t count) {
        last_bunch = count;
        packets   += count;
    }

    /**
     * Increment the appropriate counters when a few fragments have been
     * processed in a copy-way.
     *
     * @param nr_frags Number of copied fragments
     * @param bytes    Number of copied bytes
     */
    void update_copy_stats(uint64_t nr_frags, uint64_t bytes) {
        copy_frags += nr_frags;
        copy_bytes += bytes;
    }

    /**
     * Increment total fragments and bytes statistics
     *
     * @param nfrags Number of processed fragments
     * @param nbytes Number of bytes in the processed fragments
     */
    void update_frags_stats(uint64_t nfrags, uint64_t nbytes) {
        nr_frags += nfrags;
        bytes    += nbytes;
    }

    uint64_t bytes;      // total number of bytes
    uint64_t nr_frags;   // total number of fragments
    uint64_t copy_frags; // fragments that were copied on L2 level
    uint64_t copy_bytes; // bytes that were copied on L2 level
    uint64_t packets;    // total number of packets
    uint64_t last_bunch; // number of packets in the last sent/received bunch
};

struct qp_stats {
    qp_stats() {
        std::memset(&rx, 0, sizeof(rx));
        std::memset(&tx, 0, sizeof(tx));
    }

    struct {
        struct qp_stats_good good;

        struct {
            void inc_csum_err() {
                ++csum;
                ++total;
            }

            void inc_no_mem() {
                ++no_mem;
                ++total;
            }

            uint64_t no_mem;       // Packets dropped due to allocation failure
            uint64_t total;        // total number of erroneous packets
            uint64_t csum;         // packets with bad checksum
        } bad;
    } rx;

    struct {
        struct qp_stats_good good;
    } tx;
};

class qp {
    using packet_provider_type = std::function<std::experimental::optional<packet> ()>;
    std::vector<packet_provider_type> _pkt_providers;
    std::experimental::optional<std::array<uint8_t, 128>> _sw_reta;
    circular_buffer<packet> _proxy_packetq;
    stream<packet> _rx_stream;
    reactor::poller _tx_poller;
    circular_buffer<packet> _tx_packetq;

protected:
    const std::string _stats_plugin_name;
    const std::string _queue_name;
    scollectd::registrations _collectd_regs;
    qp_stats _stats;

public:
    qp(bool register_copy_stats = false,
       const std::string stats_plugin_name = std::string("network"),
       uint8_t qid = 0);
    virtual ~qp();
    virtual future<> send(packet p) = 0;
    virtual uint32_t send(circular_buffer<packet>& p) {
        uint32_t sent = 0;
        while (!p.empty()) {
            send(std::move(p.front()));
            p.pop_front();
            sent++;
        }
        return sent;
    }
    virtual void rx_start() {};
    void configure_proxies(const std::map<unsigned, float>& cpu_weights);
    // build REdirection TAble for cpu_weights map: target cpu -> weight
    void build_sw_reta(const std::map<unsigned, float>& cpu_weights);
    void proxy_send(packet p) {
        _proxy_packetq.push_back(std::move(p));
    }
    void register_packet_provider(packet_provider_type func) {
        _pkt_providers.push_back(std::move(func));
    }
    bool poll_tx();
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
    qp& local_queue() { return queue_for_cpu(engine().cpu_id()); }
    void l2receive(packet p) { _queues[engine().cpu_id()]->_rx_stream.produce(std::move(p)); }
    subscription<packet> receive(std::function<future<> (packet)> next_packet);
    virtual ethernet_address hw_address() = 0;
    virtual net::hw_features hw_features() = 0;
    virtual uint16_t hw_queues_count() { return 1; }
    virtual future<> link_ready() { return make_ready_future<>(); }
    virtual std::unique_ptr<qp> init_local_queue(boost::program_options::variables_map opts, uint16_t qid) = 0;
    virtual unsigned hash2qid(uint32_t hash) {
        return hash % hw_queues_count();
    }
    void set_local_queue(std::unique_ptr<qp> dev);
    template <typename Func>
    unsigned forward_dst(unsigned src_cpuid, Func&& hashfn) {
        auto& qp = queue_for_cpu(src_cpuid);
        if (!qp._sw_reta) {
            return src_cpuid;
        }
        auto hash = hashfn() >> _rss_table_bits;
        auto& reta = *qp._sw_reta;
        return reta[hash % reta.size()];
    }
    virtual unsigned hash2cpu(uint32_t hash) {
        // there is an assumption here that qid == cpu_id which will
        // not necessary be true in the future
        return forward_dst(hash2qid(hash), [hash] { return hash; });
    }
};

}

#endif /* NET_HH_ */
