/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "net/virtio.hh"
#include "core/reactor.hh"
#include "net/ip.hh"
#include <iostream>
#include <utility>
#include <algorithm>

using namespace net;

void dump_packet(const packet& p) {
    std::cout << "rx:";
    auto f = p.frag(0);
    for (unsigned i = 0; i < std::min(f.size, size_t(30)); ++i) {
        char x[4];
        std::sprintf(x, " %02x", uint8_t(f.base[i]));
        std::cout << x;
    }
    std::cout << "\n";
}

future<> echo_packet(net::device& netif, packet p) {
    auto f = p.frag(0);
    if (f.size < sizeof(eth_hdr)) {
        return make_ready_future<>();
    }
    auto pos = 0;
    auto eh = reinterpret_cast<eth_hdr*>(f.base + pos);
    pos += sizeof(*eh);
    ntoh(*eh);
    if (eh->eth_proto != 0x0800) {
        return make_ready_future<>();
    }
    auto iph = reinterpret_cast<ip_hdr*>(f.base + pos);
    ntoh(*iph);
    pos += iph->ihl * 4;
    if (iph->ver != 4 || iph->ihl < 5 || iph->ip_proto != 1) {
        return make_ready_future<>();
    }
    auto ip_len = iph->len;
    auto icmph = reinterpret_cast<icmp_hdr*>(f.base + pos);
    if (icmph->type != 8) {
        return make_ready_future<>();
    }
    auto icmp_len = ip_len - iph->ihl * 4;
    std::swap(eh->src_mac, eh->dst_mac);
    eh->src_mac = { 0x12, 0x23, 0x45, 0x56, 0x67, 0x68 };
    auto x = iph->src_ip;
    iph->src_ip = ipv4_address("192.168.122.2");
    iph->dst_ip = x;
    icmph->type = 0;
    icmph->csum = 0;
    hton(*iph);
    hton(*eh);
    icmph->csum = ip_checksum(icmph, icmp_len);
    iph->csum = 0;
    iph->csum = ip_checksum(iph, iph->ihl * 4);
    return netif.send(std::move(p));
}

int main(int ac, char** av) {
    auto vnet = create_virtio_net_device("tap0");
    subscription<packet> rx = vnet->receive([netif = vnet.get(), &rx] (packet p) {
        dump_packet(p);
        return echo_packet(*netif, std::move(p));
    });
    engine.run();
    return 0;
}


