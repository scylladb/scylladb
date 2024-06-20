/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/io/ios_state.hpp>
#include <seastar/net/inet_address.hh>
#include <seastar/net/dns.hh>
#include <seastar/core/print.hh>
#include <seastar/core/future.hh>
#include "inet_address.hh"

using namespace seastar;

static_assert(std::is_nothrow_default_constructible_v<gms::inet_address>);
static_assert(std::is_nothrow_copy_constructible_v<gms::inet_address>);
static_assert(std::is_nothrow_move_constructible_v<gms::inet_address>);

future<gms::inet_address> gms::inet_address::lookup(sstring name, opt_family family, opt_family preferred) {
    return seastar::net::dns::get_host_by_name(std::move(name), family).then([preferred](seastar::net::hostent&& h) {
        for (auto& addr : h.addr_list) {
            if (!preferred || addr.in_family() == preferred) {
                return gms::inet_address(addr);
            }
        }
        return gms::inet_address(h.addr_list.front());
    });
}
