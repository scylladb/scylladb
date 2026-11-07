/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/cidr.hh"

#include <charconv>
#include <cstring>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <seastar/core/reactor.hh>
#include <seastar/net/api.hh>

#include <stdexcept>

namespace utils {

bool is_cidr(std::string_view text) {
    return text.find('/') != std::string_view::npos;
}

std::optional<cidr> cidr::parse(std::string_view text) {
    auto slash = text.find('/');
    if (slash == std::string_view::npos) {
        return std::nullopt;
    }
    auto address_text = text.substr(0, slash);
    auto length_text = text.substr(slash + 1);
    if (address_text.empty() || length_text.empty()) {
        return std::nullopt;
    }

    // parse_numerical() rather than a lookup: a prefix names a range of addresses, so
    // there is nothing to resolve, and resolving would turn a typo into a DNS query.
    auto network = seastar::net::inet_address::parse_numerical(seastar::sstring(address_text));
    if (!network) {
        return std::nullopt;
    }

    unsigned length = 0;
    auto [end, ec] = std::from_chars(length_text.data(), length_text.data() + length_text.size(), length);
    if (ec != std::errc{} || end != length_text.data() + length_text.size()) {
        return std::nullopt;
    }
    if (length > network->size() * 8) {
        return std::nullopt;
    }
    return cidr(*network, static_cast<uint8_t>(length));
}

bool cidr::contains(const seastar::net::inet_address& address) const {
    if (address.in_family() != _network.in_family()) {
        return false;
    }
    const auto* lhs = static_cast<const uint8_t*>(address.data());
    const auto* rhs = static_cast<const uint8_t*>(_network.data());

    // The whole bytes of the prefix have to match exactly, and then the leftover bits of
    // the byte they stop in the middle of.
    const size_t whole_bytes = _prefix_length / 8;
    if (std::memcmp(lhs, rhs, whole_bytes) != 0) {
        return false;
    }
    const unsigned leftover_bits = _prefix_length % 8;
    if (leftover_bits == 0) {
        return true;
    }
    const uint8_t mask = static_cast<uint8_t>(0xff << (8 - leftover_bits));
    return (lhs[whole_bytes] & mask) == (rhs[whole_bytes] & mask);
}

seastar::net::inet_address select_address_in(const cidr& prefix,
        const std::vector<seastar::net::inet_address>& candidates) {
    std::vector<seastar::net::inet_address> matches;
    for (const auto& candidate : candidates) {
        if (prefix.contains(candidate)) {
            matches.push_back(candidate);
        }
    }
    if (matches.empty()) {
        throw std::runtime_error(fmt::format(
                "No address of this node is in {}. Its addresses are {}.", prefix, candidates));
    }
    if (matches.size() > 1) {
        throw std::runtime_error(fmt::format(
                "More than one address of this node is in {}: {}. Narrow the network, or name an address.",
                prefix, matches));
    }
    return matches.front();
}

std::vector<seastar::net::inet_address> local_addresses(bool include_loopback) {
    std::vector<seastar::net::inet_address> addresses;
    for (const auto& interface : seastar::engine().net().network_interfaces()) {
        if (!interface.is_up()) {
            continue;
        }
        if (interface.is_loopback() && !include_loopback) {
            continue;
        }
        for (const auto& address : interface.addresses()) {
            addresses.push_back(address);
        }
    }
    return addresses;
}

seastar::net::inet_address select_local_address(const cidr& prefix) {
    // Loopback only when that is what was asked for: a prefix inside 127.0.0.0/8 or ::1
    // is one a single-machine setup writes on purpose, while any other prefix landing on
    // a loopback address would be a node nobody outside the machine can reach.
    static const auto loopback_v4 = cidr::parse("127.0.0.0/8");
    static const auto loopback_v6 = cidr::parse("::1/128");
    const bool wants_loopback = loopback_v4->contains(prefix.network())
            || loopback_v6->contains(prefix.network());

    auto candidates = local_addresses(wants_loopback);
    if (candidates.empty()) {
        throw std::runtime_error(fmt::format(
                "Cannot listen on {}: this node cannot enumerate its own network interfaces. "
                "Name an address instead.", prefix));
    }
    return select_address_in(prefix, candidates);
}

}

auto fmt::formatter<utils::cidr>::format(const utils::cidr& c, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}/{}", c.network(), c.prefix_length());
}
