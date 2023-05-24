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
#include <fmt/core.h>
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

template <typename FormatContext>
auto fmt::formatter<gms::inet_address>::format(const gms::inet_address& x,
                                               FormatContext& ctx) const -> decltype(ctx.out()) {
    if (x.addr().is_ipv4()) {
        return fmt::format_to(ctx.out(), "{}", x.addr());
    }
    // print 2 bytes in a group, and use ':' as the delimeter
    fmt::format_to(ctx.out(), "{:2:}", fmt_hex(x.bytes()));
    if (x.addr().scope() != seastar::net::inet_address::invalid_scope) {
        return fmt::format_to(ctx.out(), "%{}", x.addr().scope());
    }
    return ctx.out();
}

template
auto fmt::formatter<gms::inet_address>::format<fmt::format_context>(
    const gms::inet_address&,
    fmt::format_context& ctx) const -> decltype(ctx.out());
template
auto fmt::formatter<gms::inet_address>::format<fmt::basic_format_context<std::back_insert_iterator<std::string>, char>>(
    const gms::inet_address&,
    fmt::basic_format_context<std::back_insert_iterator<std::string>, char>& ctx) const
    -> decltype(ctx.out());

std::ostream& gms::operator<<(std::ostream& os, const inet_address& x) {
    fmt::print(os, "{}", x);
    return os;
}

sstring gms::inet_address::to_sstring() const {
    return format("{}", *this);
}
