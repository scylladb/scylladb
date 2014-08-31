/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#ifndef IP_HH_
#define IP_HH_

#include <arpa/inet.h>
#include <cstdint>
#include <array>
#include "byteorder.hh"

namespace net {

uint16_t ip_checksum(void* data, size_t len);

struct ip_hdr {
    uint8_t ihl : 4;
    uint8_t ver : 4;
    uint8_t dscp : 6;
    uint8_t ecn : 2;
    packed<uint16_t> len;
    packed<uint16_t> id;
    packed<uint16_t> frag;
    uint8_t ttl;
    uint8_t ip_proto;
    packed<uint16_t> csum;
    packed<uint32_t> src_ip;
    packed<uint32_t> dst_ip;
    uint8_t options[0];
    template <typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(len, id, frag, csum, src_ip, dst_ip);
    }
} __attribute__((packed));

struct icmp_hdr {
    uint8_t type;
    uint8_t code;
    packed<uint16_t> csum;
    packed<uint32_t> rest;
    template <typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(csum);
    }
} __attribute__((packed));

}

#endif /* IP_HH_ */
