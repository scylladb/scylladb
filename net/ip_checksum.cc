/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "ip_checksum.hh"
#include "net.hh"
#include <arpa/inet.h>

namespace net {

void checksummer::sum(const char* data, size_t len) {
    auto orig_len = len;
    if (odd) {
        csum += uint8_t(*data++);
        --len;
    }
    auto p64 = reinterpret_cast<const packed<uint64_t>*>(data);
    while (len >= 8) {
        csum += ntohq(*p64++);
        len -= 8;
    }
    auto p16 = reinterpret_cast<const packed<uint16_t>*>(p64);
    while (len >= 2) {
        csum += ntohs(*p16++);
        len -= 2;
    }
    auto p8 = reinterpret_cast<const uint8_t*>(p16);
    if (len) {
        csum += *p8++ << 8;
        len -= 1;
    }
    odd ^= orig_len & 1;
}

uint16_t checksummer::get() const {
    __int128 csum1 = (csum & 0xffff'ffff'ffff'ffff) + (csum >> 64);
    uint64_t csum = (csum1 & 0xffff'ffff'ffff'ffff) + (csum1 >> 64);
    csum = (csum & 0xffff) + ((csum >> 16) & 0xffff) + ((csum >> 32) & 0xffff) + (csum >> 48);
    csum = (csum & 0xffff) + (csum >> 16);
    csum = (csum & 0xffff) + (csum >> 16);
    return htons(~csum);
}

void checksummer::sum(const packet& p) {
    for (auto&& f : p.fragments()) {
        sum(f.base, f.size);
    }
}

uint16_t ip_checksum(const void* data, size_t len) {
    checksummer cksum;
    cksum.sum(reinterpret_cast<const char*>(data), len);
    return cksum.get();
}


}
