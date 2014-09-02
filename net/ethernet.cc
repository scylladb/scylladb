/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "ethernet.hh"

namespace net {

std::ostream& operator<<(std::ostream& os, ethernet_address ea) {
    auto& m = ea.mac;
    using u = uint32_t;
    return fprint(os, "%02x:%02x:%02x:%02x:%02x:%02x",
            u(m[0]), u(m[1]), u(m[2]), u(m[3]), u(m[4]), u(m[5]));
}

}



