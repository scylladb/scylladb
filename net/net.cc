/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "net.hh"
#include <utility>
#include "toeplitz.hh"

using std::move;

namespace net {

l3_protocol::l3_protocol(interface* netif, eth_protocol_num proto_num)
    : _netif(netif), _proto_num(proto_num) {
}

subscription<packet, ethernet_address> l3_protocol::receive(
        std::function<future<> (packet p, ethernet_address from)> rx_fn,
        std::function<bool (forward_hash&, packet&, size_t)> forward) {
    return _netif->register_l3(_proto_num, std::move(rx_fn), std::move(forward));
};

future<> l3_protocol::send(ethernet_address to, packet p) {
    return _netif->send(_proto_num, to, std::move(p));
}

interface::interface(std::shared_ptr<device> dev)
    : _dev(dev)
    , _rx(_dev->receive([this] (packet p) { return dispatch_packet(std::move(p)); }))
    , _hw_address(_dev->hw_address())
    , _hw_features(_dev->hw_features()) {
    dev->local_queue().register_packet_provider([this] {
            std::experimental::optional<packet> p;
            if(!_packetq.empty()) {
                p = std::move(_packetq.front());
                _packetq.pop_front();
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
        auto src_cpu = engine.cpu_id();
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
            auto fw = _dev->forward_dst(engine.cpu_id(), [&p, &l3] () {
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
            if (fw != engine.cpu_id()) {
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

future<> interface::send(eth_protocol_num proto_num, ethernet_address to, packet p) {
    auto eh = p.prepend_header<eth_hdr>();
    eh->dst_mac = to;
    eh->src_mac = _hw_address;
    eh->eth_proto = uint16_t(proto_num);
    *eh = hton(*eh);
    _packetq.push_back(std::move(p));
    return make_ready_future<>();
}

}
