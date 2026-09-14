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

}

auto fmt::formatter<utils::cidr>::format(const utils::cidr& c, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}/{}", c.network(), c.prefix_length());
}
