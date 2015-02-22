/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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



