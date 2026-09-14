/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>
#include <unordered_map>

#include <fmt/core.h>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace db {

class config;

/// Description of a single client-facing listening socket.
///
/// A listener is derived from the per-protocol configuration options
/// (native_transport_port and friends).
struct listener_config {
    enum class protocol_type {
        cql,
    };

    protocol_type protocol = protocol_type::cql;
    /// Address to listen on. Empty means the protocol's default address,
    /// rpc_address.
    sstring address;
    uint16_t port = 0;
    /// Serve the shard-aware variant of the protocol. CQL only.
    bool shard_aware = false;
    /// The name of the shard-aware listener that a client connected to this
    /// listener is to be handed over to, advertised in the CQL SUPPORTED
    /// message. Empty means the clients of this listener aren't offered a
    /// shard-aware port at all. CQL only.
    sstring shard_aware_listener;
    /// Expect a PROXY protocol header on incoming connections.
    bool proxy_protocol = false;
    /// Encrypt the connections with TLS. When set, but tls_options is empty,
    /// the protocol's default encryption options are used,
    /// client_encryption_options.
    bool tls = false;
    std::unordered_map<sstring, sstring> tls_options;
    /// Enable TCP keepalive. Unset means the rpc_keepalive default.
    std::optional<bool> keepalive;

    bool operator==(const listener_config&) const = default;
};

/// A set of listeners, keyed by the name that they are known - and referred
/// to - by.
using listener_configs = std::unordered_map<sstring, listener_config>;

/// The listeners the given protocol is to serve, as derived from the
/// per-protocol configuration options.
listener_configs get_listeners(const config&, listener_config::protocol_type);

std::string_view to_string(listener_config::protocol_type);

}

template <>
struct fmt::formatter<db::listener_config::protocol_type> : fmt::formatter<string_view> {
    auto format(db::listener_config::protocol_type, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<db::listener_config> : fmt::formatter<string_view> {
    auto format(const db::listener_config&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
