/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "ethernet.hh"
#include <boost/algorithm/string.hpp>
#include <string>

namespace net {

std::ostream& operator<<(std::ostream& os, ethernet_address ea) {
    auto& m = ea.mac;
    using u = uint32_t;
    return fprint(os, "%02x:%02x:%02x:%02x:%02x:%02x",
            u(m[0]), u(m[1]), u(m[2]), u(m[3]), u(m[4]), u(m[5]));
}

ethernet_address parse_ethernet_address(std::string addr)
{
    std::vector<std::string> v;
    boost::split(v, addr , boost::algorithm::is_any_of(":"));

    if (v.size() != 6) {
        throw std::runtime_error("invalid mac address\n");
    }

    ethernet_address a;
    unsigned i = 0;
    for (auto &x: v) {
        a.mac[i++] = std::stoi(x, nullptr,16);
    }
    return a;
}
}



