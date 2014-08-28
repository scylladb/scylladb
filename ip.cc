/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "ip.hh"

namespace net {

uint16_t ip_checksum(void* data, size_t len) {
    uint64_t csum = 0;
    auto p64 = reinterpret_cast<uint64_t*>(data);
    while (len >= 8) {
        auto old = csum;
        csum += ntohq(*p64++);
        csum += (csum < old);
        len -= 8;
    }
    auto p16 = reinterpret_cast<uint16_t*>(p64);
    while (len >= 2) {
        auto old = csum;
        csum += ntohs(*p16++);
        csum += (csum < old);
        len -= 2;
    }
    auto p8 = reinterpret_cast<uint8_t*>(p16);
    if (len) {
        auto old = csum;
        csum += *p8++;
        csum += (csum < old);
        len -= 1;
    }
    csum = (csum & 0xffff) + ((csum >> 16) & 0xffff) + ((csum >> 32) & 0xffff) + (csum >> 48);
    csum += csum >> 16;
    return htons(~csum);
}

}
