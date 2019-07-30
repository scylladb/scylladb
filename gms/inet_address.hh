/*
 * Copyright (C) 2015 ScyllaDB
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

#include <utility>

#include <seastar/core/print.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include "utils/serialization.hh"
#include <sstream>
#include <optional>

namespace gms {

class inet_address {
private:
    net::inet_address _addr;
public:
    inet_address() = default;
    inet_address(int32_t ip)
        : inet_address(uint32_t(ip)) {
    }
    explicit inet_address(uint32_t ip)
        : _addr(net::ipv4_address(ip)) {
    }
    inet_address(const net::inet_address& addr) : _addr(addr) {}
    inet_address(const socket_address& sa)
        : inet_address(sa.addr())
    {}
    const net::inet_address& addr() const {
        return _addr;
    }

    inet_address(const inet_address&) = default;

    operator const seastar::net::inet_address&() const {
        return _addr;
    }

    inet_address(const sstring& addr) {
        // FIXME: We need a real DNS resolver
        if (addr == "localhost") {
            _addr = net::ipv4_address("127.0.0.1");
        } else {
            _addr = net::inet_address(addr);
        }
    }
    bytes_view bytes() const {
        return bytes_view(reinterpret_cast<const int8_t*>(_addr.data()), _addr.size());
    }
    // TODO remove
    uint32_t raw_addr() const {
        return addr().as_ipv4_address().ip;
    }
    sstring to_sstring() const {
        return format("{}", *this);
    }
    friend inline bool operator==(const inet_address& x, const inet_address& y) {
        return x._addr == y._addr;
    }
    friend inline bool operator!=(const inet_address& x, const inet_address& y) {
        using namespace std::rel_ops;
        return x._addr != y._addr;
    }
    friend inline bool operator<(const inet_address& x, const inet_address& y) {
        return x.bytes() < y.bytes();
    }
    friend inline std::ostream& operator<<(std::ostream& os, const inet_address& x) {
        return os << x._addr;
    }
    friend struct std::hash<inet_address>;

    using opt_family = std::optional<net::inet_address::family>;

    static future<inet_address> lookup(sstring, opt_family family = {}, opt_family preferred = {});
};

std::ostream& operator<<(std::ostream& os, const inet_address& x);

}

namespace std {
template<>
struct hash<gms::inet_address> {
    size_t operator()(gms::inet_address a) const { return std::hash<net::inet_address>()(a._addr); }
};
}
