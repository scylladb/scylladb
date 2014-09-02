/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "ip.hh"
#include "core/print.hh"

namespace net {

std::ostream& operator<<(std::ostream& os, ipv4_address a) {
    auto ip = a.ip;
    return fprint(os, "%d.%d.%d.%d",
            (ip >> 24) & 0xff,
            (ip >> 16) & 0xff,
            (ip >> 8) & 0xff,
            (ip >> 0) & 0xff);
}

uint16_t ip_checksum(const void* data, size_t len) {
    uint64_t csum = 0;
    auto p64 = reinterpret_cast<const packed<uint64_t>*>(data);
    while (len >= 8) {
        auto old = csum;
        csum += ntohq(*p64++);
        csum += (csum < old);
        len -= 8;
    }
    auto p16 = reinterpret_cast<const packed<uint16_t>*>(p64);
    while (len >= 2) {
        auto old = csum;
        csum += ntohs(*p16++);
        csum += (csum < old);
        len -= 2;
    }
    auto p8 = reinterpret_cast<const uint8_t*>(p16);
    if (len) {
        auto old = csum;
        csum += *p8++ << 8;
        csum += (csum < old);
        len -= 1;
    }
    csum = (csum & 0xffff) + ((csum >> 16) & 0xffff) + ((csum >> 32) & 0xffff) + (csum >> 48);
    csum += csum >> 16;
    return htons(~csum);
}

ipv4::ipv4(interface* netif)
    : _netif(netif)
    , _global_arp(netif)
    , _arp(_global_arp)
    , _l3(netif, 0x0800)
    , _tcp(*this)
    , _l4({ { 6, &_tcp } }) {
    run();
}

bool ipv4::in_my_netmask(ipv4_address a) const {
    return !((a.ip ^ _host_address.ip) & _netmask.ip);
}


void ipv4::run() {
    _l3.receive().then([this] (packet p, ethernet_address from) {
        handle_received_packet(std::move(p), from);
        run();
    });
}

void ipv4::handle_received_packet(packet p, ethernet_address from) {
    auto iph = p.get_header<ip_hdr>(0);
    if (!iph) {
        return;
    }
    ntoh(*iph);
    // FIXME: process options
    if (in_my_netmask(iph->src_ip) && iph->src_ip != _host_address) {
        _arp.learn(from, iph->src_ip);
    }
    if (iph->frag & 0x3fff) {
        // FIXME: defragment
        return;
    }
    if (iph->dst_ip != _host_address) {
        // FIXME: forward
        return;
    }
    auto l4 = _l4[iph->ip_proto];
    if (l4) {
        p.trim_front(iph->ihl * 4);
        l4->received(std::move(p), iph->src_ip, iph->dst_ip);
    }
}

void ipv4::send(ipv4_address to, uint8_t proto_num, packet p) {
    // FIXME: fragment
    ip_hdr iph;
    iph.ihl = sizeof(iph) / 4;
    iph.ver = 4;
    iph.dscp = 0;
    iph.ecn = 0;
    iph.len = sizeof(iph) + p.len;
    iph.id = 0;
    iph.frag = 0;
    iph.ttl = 64;
    iph.ip_proto = proto_num;
    iph.csum = 0;
    iph.src_ip = _host_address;
    // FIXME: routing
    auto gw = to;
    iph.dst_ip = to;
    hton(iph);
    checksummer csum;
    csum.sum(reinterpret_cast<char*>(&iph), sizeof(iph));
    iph.csum = csum.get();
    auto q = packet(fragment{reinterpret_cast<char*>(&iph), sizeof(iph)}, std::move(p));
    _arp.lookup(gw).then([this, p = std::move(q)] (ethernet_address e_dst) mutable {
        _send_sem.wait().then([this, e_dst, p = std::move(p)] () mutable {
            return _l3.send(e_dst, std::move(p));
        }).then([this] {
            _send_sem.signal();
        });
    });
}

void ipv4::set_host_address(ipv4_address ip) {
    _host_address = ip;
    _arp.set_self_addr(ip);
}

void checksummer::sum(const char* data, size_t len) {
    auto orig_len = len;
    if (odd) {
        partial += uint8_t(*data++);
        --len;
    }
    partial += ip_checksum(data, len);
    odd ^= orig_len & 1;
}

void checksummer::sum(const packet& p) {
    for (auto&& f : p.fragments) {
        sum(f.base, f.size);
    }
}

uint16_t checksummer::get() const {
    return partial + (partial >> 16);
}

}
