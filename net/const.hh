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

}
#endif
