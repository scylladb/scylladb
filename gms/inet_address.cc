/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <iomanip>
#include <boost/io/ios_state.hpp>
#include <seastar/net/inet_address.hh>
#include <seastar/net/dns.hh>
#include <seastar/core/print.hh>
#include <seastar/core/future.hh>
#include "inet_address.hh"
#include "utils/to_string.hh"

using namespace seastar;

static_assert(std::is_nothrow_default_constructible_v<gms::inet_address>);
static_assert(std::is_nothrow_copy_constructible_v<gms::inet_address>);
static_assert(std::is_nothrow_move_constructible_v<gms::inet_address>);

future<gms::inet_address> gms::inet_address::lookup(sstring name, opt_family family, opt_family preferred) {
    return seastar::net::dns::get_host_by_name(name, family).then([preferred](seastar::net::hostent&& h) {
        for (auto& addr : h.addr_list) {
            if (!preferred || addr.in_family() == preferred) {
                return gms::inet_address(addr);
            }
        }
        return gms::inet_address(h.addr_list.front());
    });
}

std::ostream& gms::operator<<(std::ostream& os, const inet_address& x) {
    if (x.addr().is_ipv4()) {
        return os << x.addr();
    }

    boost::io::ios_flags_saver fs(os);

    os << std::hex;
    auto&& bytes = x.bytes();
    auto i = 0u;
    auto acc = 0u;
    // extra paranoid sign extension evasion - #5808
    for (uint8_t b : bytes) {
        acc <<= 8;
        acc |= b;
        if ((++i & 1) == 0) {
            os << (std::exchange(acc, 0u) & 0xffff);
            if (i != bytes.size()) {
                os << ":";
            }
        }
    }    
    os << std::dec;
    if (x.addr().scope() != seastar::net::inet_address::invalid_scope) {
        os << '%' << x.addr().scope();
    }
    return os;
}

sstring gms::inet_address::to_sstring() const {
    return format("{}", *this);
}
