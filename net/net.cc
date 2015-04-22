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
 *
 */

#include <boost/asio/ip/address_v4.hpp>
#include <boost/algorithm/string.hpp>
#include "net.hh"
#include <utility>
#include "toeplitz.hh"

using std::move;

ipv4_addr::ipv4_addr(const std::string &addr) {
    std::vector<std::string> items;
    boost::split(items, addr, boost::is_any_of(":"));
    if (items.size() == 1) {
        ip = boost::asio::ip::address_v4::from_string(addr).to_ulong();
        port = 0;
    } else if (items.size() == 2) {
        ip = boost::asio::ip::address_v4::from_string(items[0]).to_ulong();
        port = std::stoul(items[1]);
    } else {
        throw std::invalid_argument("invalid format: " + addr);
    }
}

namespace net {

inline
bool qp::poll_tx() {
    if (_tx_packetq.size() < 16) {
        // refill send queue from upper layers
        uint32_t work;
        do {
            work = 0;
            for (auto&& pr : _pkt_providers) {
                auto p = pr();
                if (p) {
                    work++;
                    _tx_packetq.push_back(std::move(p.value()));
                    if (_tx_packetq.size() == 128) {
                        break;
                    }
                }
            }
        } while (work && _tx_packetq.size() < 128);
    }
    if (!_tx_packetq.empty()) {
        _stats.tx.good.update_pkts_bunch(send(_tx_packetq));
        return true;
    }

    return false;
}

qp::qp(bool register_copy_stats,
       const std::string stats_plugin_name, uint8_t qid)
        : _tx_poller([this] { return poll_tx(); })
        , _stats_plugin_name(stats_plugin_name)
        , _queue_name(std::string("queue") + std::to_string(qid))
        , _collectd_regs({

            //
            // Packets rate: DERIVE:0:u
            //
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "if_packets", _queue_name)
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.rx.good.packets)
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.tx.good.packets)
            ),
            //
            // Bytes rate: DERIVE:0:U
            //
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "if_octets", _queue_name)
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.rx.good.bytes)
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.tx.good.bytes)
            ),

            //
            // Queue length: GAUGE:0:U
            //
            // Tx
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "queue_length", "tx-packet-queue")
                    , scollectd::make_typed(scollectd::data_type::GAUGE
                    , std::bind(&decltype(_tx_packetq)::size, &_tx_packetq))
            ),

            //
            // Number of packets in last bunch: GAUGE:0:U
            //
            // Tx
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "requests", "tx-packet-queue-last-bunch")
                    , scollectd::make_typed(scollectd::data_type::GAUGE
                    , _stats.tx.good.last_bunch)
            ),
            // Rx
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "requests", "rx-packet-queue-last-bunch")
                    , scollectd::make_typed(scollectd::data_type::GAUGE
                    , _stats.rx.good.last_bunch)
            ),

            //
            // Fragments rate: DERIVE:0:U
            //
            // Tx
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "tx-frags")
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.tx.good.nr_frags)
            ),
            // Rx
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "rx-frags")
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.rx.good.nr_frags)
            ),
    })
{
    if (register_copy_stats) {
        //
        // Non-zero-copy data bytes rate: DERIVE:0:u
        //
        _collectd_regs.push_back(
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "if_octets", _queue_name + " Copy Bytes")
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.rx.good.copy_bytes)
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.tx.good.copy_bytes)
            ));
        //
        // Non-zero-copy data fragments rate: DERIVE:0:u
        //
        // Tx
        _collectd_regs.push_back(
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "tx-frags-copy")
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.tx.good.copy_frags)
            ));
        // Rx
        _collectd_regs.push_back(
            scollectd::add_polled_metric(scollectd::type_instance_id(
                    _stats_plugin_name
                    , scollectd::per_cpu_plugin_instance
                    , "total_operations", "rx-frags-copy")
                    , scollectd::make_typed(scollectd::data_type::DERIVE
                    , _stats.rx.good.copy_frags)
            ));
    }
}

qp::~qp() {
}

void qp::configure_proxies(const std::map<unsigned, float>& cpu_weights) {
    assert(!cpu_weights.empty());
    if ((cpu_weights.size() == 1 && cpu_weights.begin()->first == engine().cpu_id())) {
        // special case queue sending to self only, to avoid requiring a hash value
        return;
    }
    register_packet_provider([this] {
        std::experimental::optional<packet> p;
        if (!_proxy_packetq.empty()) {
            p = std::move(_proxy_packetq.front());
            _proxy_packetq.pop_front();
        }
        return p;
    });
    build_sw_reta(cpu_weights);
}

void qp::build_sw_reta(const std::map<unsigned, float>& cpu_weights) {
    float total_weight = 0;
    for (auto&& x : cpu_weights) {
        total_weight += x.second;
    }
    float accum = 0;
    unsigned idx = 0;
    std::array<uint8_t, 128> reta;
    for (auto&& entry : cpu_weights) {
        auto cpu = entry.first;
        auto weight = entry.second;
        accum += weight;
        while (idx < (accum / total_weight * reta.size() - 0.5)) {
            reta[idx++] = cpu;
        }
    }
    _sw_reta = reta;
}

subscription<packet>
device::receive(std::function<future<> (packet)> next_packet) {
    auto sub = _queues[engine().cpu_id()]->_rx_stream.listen(std::move(next_packet));
    _queues[engine().cpu_id()]->rx_start();
    return std::move(sub);
}

void device::set_local_queue(std::unique_ptr<qp> dev) {
    assert(!_queues[engine().cpu_id()]);
    _queues[engine().cpu_id()] = dev.get();
    engine().at_destroy([dev = std::move(dev)] {});
}


l3_protocol::l3_protocol(interface* netif, eth_protocol_num proto_num, packet_provider_type func)
    : _netif(netif), _proto_num(proto_num)  {
        _netif->register_packet_provider(std::move(func));
}

subscription<packet, ethernet_address> l3_protocol::receive(
        std::function<future<> (packet p, ethernet_address from)> rx_fn,
        std::function<bool (forward_hash&, packet&, size_t)> forward) {
    return _netif->register_l3(_proto_num, std::move(rx_fn), std::move(forward));
};

interface::interface(std::shared_ptr<device> dev)
    : _dev(dev)
    , _rx(_dev->receive([this] (packet p) { return dispatch_packet(std::move(p)); }))
    , _hw_address(_dev->hw_address())
    , _hw_features(_dev->hw_features()) {
    dev->local_queue().register_packet_provider([this, idx = 0u] () mutable {
            std::experimental::optional<packet> p;
            for (size_t i = 0; i < _pkt_providers.size(); i++) {
                auto l3p = _pkt_providers[idx++]();
                if (idx == _pkt_providers.size())
                    idx = 0;
                if (l3p) {
                    auto l3pv = std::move(l3p.value());
                    auto eh = l3pv.p.prepend_header<eth_hdr>();
                    eh->dst_mac = l3pv.to;
                    eh->src_mac = _hw_address;
                    eh->eth_proto = uint16_t(l3pv.proto_num);
                    *eh = hton(*eh);
                    p = std::move(l3pv.p);
                    return p;
                }
            }
            return p;
        });
}

subscription<packet, ethernet_address>
interface::register_l3(eth_protocol_num proto_num,
        std::function<future<> (packet p, ethernet_address from)> next,
        std::function<bool (forward_hash&, packet& p, size_t)> forward) {
    auto i = _proto_map.emplace(std::piecewise_construct, std::make_tuple(uint16_t(proto_num)), std::forward_as_tuple(std::move(forward)));
    assert(i.second);
    l3_rx_stream& l3_rx = i.first->second;
    return l3_rx.packet_stream.listen(std::move(next));
}

unsigned interface::hash2cpu(uint32_t hash) {
    return _dev->hash2cpu(hash);
}

void interface::forward(unsigned cpuid, packet p) {
    static __thread unsigned queue_depth;

    if (queue_depth < 1000) {
        queue_depth++;
        auto src_cpu = engine().cpu_id();
        smp::submit_to(cpuid, [this, p = std::move(p), src_cpu]() mutable {
            _dev->l2receive(p.free_on_cpu(src_cpu));
        }).then([] {
            queue_depth--;
        });
    }
}

future<> interface::dispatch_packet(packet p) {
    auto eh = p.get_header<eth_hdr>();
    if (eh) {
        auto i = _proto_map.find(ntoh(eh->eth_proto));
        if (i != _proto_map.end()) {
            l3_rx_stream& l3 = i->second;
            auto fw = _dev->forward_dst(engine().cpu_id(), [&p, &l3] () {
                auto hwrss = p.rss_hash();
                if (hwrss) {
                    return hwrss.value();
                } else {
                    forward_hash data;
                    if (l3.forward(data, p, sizeof(eth_hdr))) {
                        return toeplitz_hash(rsskey, data);
                    }
                    return 0u;
                }
            });
            if (fw != engine().cpu_id()) {
                forward(fw, std::move(p));
            } else {
                auto h = ntoh(*eh);
                auto from = h.src_mac;
                p.trim_front(sizeof(*eh));
                // avoid chaining, since queue lenth is unlimited
                // drop instead.
                if (l3.ready.available()) {
                    l3.ready = l3.packet_stream.produce(std::move(p), from);
                }
            }
        }
    }
    return make_ready_future<>();
}

}
