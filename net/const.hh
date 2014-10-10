/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CONST_HH_
#define CONST_HH_
namespace net {

enum class ip_protocol_num : uint8_t {
    icmp = 1, tcp = 6, udp = 17, unused = 255
};

}
#endif
