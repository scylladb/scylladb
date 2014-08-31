/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "ethernet.hh"

namespace net {

std::ostream& operator<<(std::ostream& os, ethernet_address ea) {
    auto& m = ea.mac;
    return fprint(os, "%02x:%02x:%02x:%02x:%02x:%02x",
            m[0], m[1], m[2], m[3], m[4], m[5]);
}

}



