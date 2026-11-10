/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <iosfwd>
#include <optional>
#include <unordered_map>

#include <fmt/core.h>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace YAML {
class Node;
}

namespace db {

class config;

/// Description of a single client-facing listening socket.
///
/// A listener is either spelled out by the user in the "listeners"
/// configuration option, or derived from the legacy per-protocol options
/// (native_transport_port, alternator_https_port, and friends).
struct listener_config {
    enum class protocol_type {
        cql,
        alternator,
    };

    protocol_type protocol = protocol_type::cql;
    /// Address to listen on. Empty means the protocol's default address:
    /// rpc_address for CQL, alternator_address for Alternator.
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
    /// the protocol's default encryption options are used:
    /// client_encryption_options for CQL, alternator_encryption_options for
    /// Alternator.
    bool tls = false;
    std::unordered_map<sstring, sstring> tls_options;
    /// Enable TCP keepalive. Unset means the rpc_keepalive default.
    std::optional<bool> keepalive;

    bool operator==(const listener_config&) const = default;

    static listener_config decode(const YAML::Node&);
};

/// A set of listeners, keyed by the name that they are known - and referred
/// to - by.
using listener_configs = std::unordered_map<sstring, listener_config>;

/// The listeners the given protocol is to serve.
///
/// `configured` is the set of listeners the "listeners" cluster config option resolved to
/// for this node. When it is non-empty it is the sole source of truth and the per-protocol
/// options in scylla.yaml are ignored; when it is empty the listeners are derived from
/// those options instead, so a cluster that never set the option keeps the listeners it
/// has always had.
listener_configs get_listeners(const config&, const listener_configs& configured, listener_config::protocol_type);

/// Parse a set of listeners out of its stored cluster-config form: a YAML mapping of
/// listener name to listener, in flow style, as canonicalize_listeners() writes it.
listener_configs decode_listeners(std::string_view);

/// The cluster-config codec of the "listeners" option. Declared here, next to what they
/// describe, and handed to the registry by db/cluster_config_registry.cc.
///
/// The stored form is flow-style YAML - which is to say it is also JSON - so that a value
/// stays on the one line a cluster config value is allowed, and so that the listeners a
/// user writes and the listeners a node reads back go through one parser.
std::optional<sstring> validate_listeners(std::string_view);
sstring canonicalize_listeners(std::string_view);

std::string_view to_string(listener_config::protocol_type);

std::istream& operator>>(std::istream&, listener_config&);

}

template <>
struct fmt::formatter<db::listener_config::protocol_type> : fmt::formatter<string_view> {
    auto format(db::listener_config::protocol_type, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<db::listener_config> : fmt::formatter<string_view> {
    auto format(const db::listener_config&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
