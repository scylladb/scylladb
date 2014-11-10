/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CONST_HH_
#define CONST_HH_
namespace net {

enum class ip_protocol_num : uint8_t {
    icmp = 1, tcp = 6, udp = 17, unused = 255
};

enum class eth_protocol_num : uint16_t {
    ipv4 = 0x0800, arp = 0x0806, ipv6 = 0x86dd
};

const uint8_t eth_hdr_len = 14;
const uint8_t tcp_hdr_len_min = 20;
const uint8_t ipv4_hdr_len_min = 20;
const uint8_t ipv6_hdr_len_min = 40;
const uint16_t ip_packet_len_max = 65535;

}
#endif
