/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "inet_address.hh"
#include "serializer.hh"

namespace ser {

/**
 * Manual definition of inet_address serialization.
 * Because inet_address was initially hardcoded to
 * ipv4, its wire format is not very forward compatible.
 *
 * Since we potentially need to communicate with older
 * version nodes, we manually define the new serial format
 * for inet_address to be:
 *
 * ipv4: 4  bytes address
 * ipv6: 4  bytes marker 0xffffffff (invalid address)
 *       16 bytes data -> address
 *
 * As long as we are restricted to ipv4 (config/user responsibility)
 * we will be able to swap gossip with older nodes. Once the
 * cluster is fully updated, user can enable ipv6 and we will be
 * happy and addressing all the molecules.
 *
 */
template<>
struct serializer<gms::inet_address> {
    template<typename Input>
    static gms::inet_address read(Input& in) {
        auto sz = deserialize(in, boost::type<uint32_t>());
        if (sz == std::numeric_limits<uint32_t>::max()) {
            seastar::net::ipv6_address addr(deserialize(in, boost::type<net::ipv6_address::ipv6_bytes>()));
            return gms::inet_address(addr);
        }
        return gms::inet_address(sz);
    }
    template<typename Output>
    static void write(Output& out, gms::inet_address v) {
        auto& addr = v.addr();
        if (addr.is_ipv6()) {
            serialize(out, std::numeric_limits<uint32_t>::max());
            auto bv = v.bytes();
            out.write(reinterpret_cast<const char *>(bv.data()), bv.size());
        } else {
            uint32_t ip = addr.as_ipv4_address().ip;
            // must write this little (or rather host) endian
            serialize(out, ip);
        }
    }
    template<typename Input>
    static void skip(Input& v) {
        read(v);
    }
};


}
