/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "net.hh"
#include <utility>

using std::move;

namespace net {

__thread device *dev;

l3_protocol::l3_protocol(interface* netif, uint16_t proto_num)
    : _netif(netif), _proto_num(proto_num) {
}

subscription<packet, ethernet_address> l3_protocol::receive(
        std::function<future<> (packet p, ethernet_address from)> rx_fn,
        std::function<unsigned (packet&, size_t)> forward) {
    return _netif->register_l3(_proto_num, std::move(rx_fn), std::move(forward));
};

future<> l3_protocol::send(ethernet_address to, packet p) {
    return _netif->send(_proto_num, to, std::move(p));
}

interface::interface(std::unique_ptr<device> dev)
    : _dev(std::move(dev))
    , _rx(_dev->receive([this] (packet p) { return dispatch_packet(std::move(p)); }))
    , _hw_address(_dev->hw_address())
    , _hw_features(_dev->hw_features()) {
}

subscription<packet, ethernet_address>
interface::register_l3(uint16_t proto_num,
        std::function<future<> (packet p, ethernet_address from)> next,
        std::function<unsigned (packet&, size_t)> forward) {
    auto i = _proto_map.emplace(std::piecewise_construct, std::make_tuple(proto_num), std::forward_as_tuple(std::move(forward)));
    assert(i.second);
    l3_rx_stream& l3_rx = i.first->second;
    return l3_rx.packet_stream.listen(std::move(next));
}

future<> interface::dispatch_packet(packet p) {
    auto eh = p.get_header<eth_hdr>();
    if (eh) {
        ntoh(*eh);
        auto i = _proto_map.find(eh->eth_proto);
        if (i != _proto_map.end()) {
            l3_rx_stream& l3 = i->second;
            auto from = eh->src_mac;
            p.trim_front(sizeof(*eh));
            // avoid chaining, since queue lenth is unlimited
            // drop instead.
            if (l3.ready.available()) {
                l3.ready = l3.packet_stream.produce(std::move(p), from);
            }
        }
    }
    return make_ready_future<>();
}

future<> interface::send(uint16_t proto_num, ethernet_address to, packet p) {
    auto eh = p.prepend_header<eth_hdr>();
    eh->dst_mac = to;
    eh->src_mac = _hw_address;
    eh->eth_proto = proto_num;
    hton(*eh);
    return _dev->send(std::move(p));
}

}
