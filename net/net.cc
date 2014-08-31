/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "net.hh"
#include <utility>

using std::move;

namespace net {

constexpr size_t packet::internal_data_size;

void packet::linearize(size_t at_frag, size_t desired_size) {
    size_t nr_frags = 0;
    size_t accum_size = 0;
    while (accum_size < desired_size) {
        accum_size += fragments[at_frag + nr_frags].size;
        ++nr_frags;
    }
    std::unique_ptr<char[]> new_frag{new char[accum_size]};
    auto p = new_frag.get();
    for (size_t i = 0; i < nr_frags; ++i) {
        auto& f = fragments[at_frag + i];
        p = std::copy(f.base, f.base + f.size, p);
    }
    fragments.erase(fragments.begin() + at_frag + 1, fragments.begin() + at_frag + nr_frags);
    fragments[at_frag] = fragment{new_frag.get(), accum_size};
    _deleter = make_deleter(std::move(_deleter), [buf = std::move(new_frag)] {});
}

future<packet, ethernet_address> l3_protocol::receive() {
    return _netif->receive(_proto_num);
};

future<> l3_protocol::send(ethernet_address to, packet p) {
    return _netif->send(_proto_num, to, std::move(p));
}

future<packet, ethernet_address> interface::receive(uint16_t proto_num) {
    auto& pr = _proto_map[proto_num] = promise<packet, ethernet_address>();
    return pr.get_future();
}

interface::interface(std::unique_ptr<device> dev)
    : _dev(std::move(dev)), _hw_address(_dev->hw_address()) {
}

void interface::run() {
    _dev->receive().then([this] (packet p) {
        auto eh = p.get_header<eth_hdr>(0);
        if (eh) {
            ntoh(*eh);
            auto i = _proto_map.find(eh->eth_proto);
            if (i != _proto_map.end()) {
                auto from = eh->src_mac;
                p.trim_front(sizeof(*eh));
                i->second.set_value(std::move(p), from);
                _proto_map.erase(i);
            }
        }
        run();
    });
}

future<> interface::send(uint16_t proto_num, ethernet_address to, packet p) {
    eth_hdr eh;
    eh.dst_mac = to;
    eh.src_mac = _hw_address;
    eh.eth_proto = proto_num;
    hton(eh);
    return _dev->send(packet(fragment{reinterpret_cast<char*>(&eh), sizeof(eh)}, std::move(p)));
}

}
