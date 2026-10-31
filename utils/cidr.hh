/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include <seastar/net/inet_address.hh>

#include "seastarx.hh"

namespace utils {

/// A range of addresses, written the way everyone writes one: an address and the number
/// of leading bits that matter, as in "10.0.0.0/8" or "fd00::/8".
///
/// A prefix is of one family. An IPv4 address is never inside an IPv6 prefix, and the
/// other way round, even when one of them is the mapped form of the other - a
/// configuration that says "the 10.0.0.0/8 network" means the v4 one, and silently
/// matching a v6 address against it would be a surprise rather than a convenience.
class cidr {
public:
    /// Parse "address/length". Returns nullopt when the text is not a prefix at all: no
    /// "/", an address that does not parse, a length that is not a number or does not fit
    /// the family. The address is parsed numerically, so this never resolves a name and
    /// never blocks.
    static std::optional<cidr> parse(std::string_view text);

    /// True when `address` is inside this prefix. False for an address of another family.
    bool contains(const seastar::net::inet_address& address) const;

    const seastar::net::inet_address& network() const noexcept { return _network; }
    uint8_t prefix_length() const noexcept { return _prefix_length; }

    bool operator==(const cidr&) const = default;

private:
    cidr(seastar::net::inet_address network, uint8_t prefix_length) noexcept
        : _network(network), _prefix_length(prefix_length) {}

    seastar::net::inet_address _network;
    uint8_t _prefix_length;
};

/// Whether `text` looks like a prefix rather than a plain address or a host name. Cheap
/// enough to ask before parsing, for a caller that accepts either.
bool is_cidr(std::string_view text);

/// The one address in `candidates` that is inside `prefix`.
///
/// Throws when none of them is, and when more than one is. Both are configuration
/// mistakes that are worse to paper over than to refuse: a node that silently listened on
/// whichever address happened to be enumerated first would be reachable on an address
/// nobody meant, and finding out why means reading the interface list of that one machine.
seastar::net::inet_address select_address_in(const cidr& prefix,
        const std::vector<seastar::net::inet_address>& candidates);

/// The addresses of this host's usable interfaces: those that are up, and not loopback
/// unless `include_loopback`. Empty on a network stack that does not enumerate interfaces
/// (the native one does not), which a caller has to be ready for.
std::vector<seastar::net::inet_address> local_addresses(bool include_loopback);

/// The local address inside `prefix`, for a listener that was configured with a network
/// rather than an address. Loopback is considered only when the prefix is itself within
/// loopback, so that "listen on 10.0.0.0/8" cannot land on 127.0.0.1 while
/// "listen on 127.0.0.0/8" still works for a single-machine setup.
seastar::net::inet_address select_local_address(const cidr& prefix);

}

template <>
struct fmt::formatter<utils::cidr> : fmt::formatter<string_view> {
    auto format(const utils::cidr&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
