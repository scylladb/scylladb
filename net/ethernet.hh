/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef ETHERNET_HH_
#define ETHERNET_HH_

#include <array>
#include "byteorder.hh"
#include "core/print.hh"

namespace net {

struct ethernet_address {
    std::array<uint8_t, 6> mac;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) {}
} __attribute__((packed));

std::ostream& operator<<(std::ostream& os, ethernet_address ea);

struct ethernet {
    using address = ethernet_address;
    static address broadcast_address() {
        return {{{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }}};
    }
    static constexpr uint16_t arp_hardware_type() { return 1; }
};

struct eth_hdr {
    ethernet_address dst_mac;
    ethernet_address src_mac;
    packed<uint16_t> eth_proto;
    template <typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(eth_proto);
    }
} __attribute__((packed));

ethernet_address parse_ethernet_address(std::string addr);
}

#endif /* ETHERNET_HH_ */
