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
    , _arp(_global_arp) {
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
