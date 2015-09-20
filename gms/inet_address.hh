/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "types.hh"
#include "net/ip.hh"
#include "utils/serialization.hh"
#include <sstream>

namespace gms {

class inet_address {
private:
    // FIXME: ipv6
    net::ipv4_address _addr;
public:
    inet_address() = default;
    inet_address(int32_t ip)
        : _addr(uint32_t(ip)) {
    }
    inet_address(net::ipv4_address&& addr) : _addr(std::move(addr)) {}

    const net::ipv4_address& addr() const {
        return _addr;
    }

    inet_address(const sstring& addr) {
        // FIXME: We need a real DNS resolver
        if (addr == "localhost") {
            _addr = net::ipv4_address("127.0.0.1");
        } else {
            _addr = net::ipv4_address(addr);
        }
    }
    uint32_t raw_addr() const {
        return _addr.ip;
    }
    bool is_broadcast_address() {
        return _addr == net::ipv4::broadcast_address();
    }
    void serialize(bytes::iterator& out) const {
        int8_t inet_address_size = sizeof(inet_address);
        serialize_int8(out, inet_address_size);
        serialize_int32(out, _addr.ip);
    }
    static inet_address deserialize(bytes_view& v) {
        int8_t inet_address_size = read_simple<int8_t>(v);
        assert(inet_address_size == sizeof(inet_address));
        return inet_address(read_simple<int32_t>(v));
    }
    size_t serialized_size() const {
        return serialize_int8_size + serialize_int32_size;
    }
    friend inline bool operator==(const inet_address& x, const inet_address& y) {
        return x._addr == y._addr;
    }
    friend inline bool operator!=(const inet_address& x, const inet_address& y) {
        return x._addr != y._addr;
    }
    friend inline bool operator<(const inet_address& x, const inet_address& y) {
        return x._addr.ip < y._addr.ip;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const inet_address& x) {
        return os << x._addr;
    }
    friend struct std::hash<inet_address>;
};

}

namespace std {
template<>
struct hash<gms::inet_address> {
    size_t operator()(gms::inet_address a) const { return std::hash<net::ipv4_address>()(a._addr); }
};
}
