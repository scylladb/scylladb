/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef ETHERNET_HH_
#define ETHERNET_HH_

#include <array>
#include "byteorder.hh"

namespace net {

using ethernet_address = std::array<uint8_t, 6>;

struct eth_hdr {
    ethernet_address dst_mac;
    ethernet_address src_mac;
    packed<uint16_t> eth_proto;
    template <typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(eth_proto);
    }
} __attribute__((packed));

}

#endif /* ETHERNET_HH_ */
