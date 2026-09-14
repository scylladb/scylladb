/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/listener_config.hh"

#include "db/config.hh"
#include "utils/config_file.hh"

namespace db {

std::string_view to_string(listener_config::protocol_type p) {
    switch (p) {
    case listener_config::protocol_type::cql:
        return "cql";
    }
    return "unknown";
}

// Listeners as spelled out by the legacy, per-protocol configuration options.
// TLS options are left empty, meaning the protocol's defaults - the
// controllers resolve those.
static listener_configs legacy_cql_listeners(const config& cfg) {
    using protocol_type = listener_config::protocol_type;

    listener_configs listeners;
    auto name_of = [] (bool shard_aware, bool tls, bool proxy_protocol) {
        return fmt::format("cql{}{}{}", shard_aware ? "_shard_aware" : "", tls ? "_ssl" : "",
                proxy_protocol ? "_proxy_protocol" : "");
    };
    auto add = [&] (uint16_t port, bool shard_aware, bool tls, bool proxy_protocol) {
        listeners.emplace(name_of(shard_aware, tls, proxy_protocol), listener_config{
            .protocol = protocol_type::cql,
            .address = cfg.rpc_address(),
            .port = port,
            .shard_aware = shard_aware,
            .proxy_protocol = proxy_protocol,
            .tls = tls,
        });
    };

    bool native_port = false, native_shard_aware_port = false;

    if (cfg.native_transport_port.is_set() ||
            (!cfg.native_transport_port_ssl.is_set() && !cfg.native_transport_port.is_set())) {
        // Non-SSL port is specified || neither SSL nor non-SSL ports are specified
        add(cfg.native_transport_port(), false, false, false);
        native_port = true;
    }
    if (cfg.native_shard_aware_transport_port.is_set() ||
            (!cfg.native_shard_aware_transport_port_ssl.is_set() && !cfg.native_shard_aware_transport_port.is_set())) {
        add(cfg.native_shard_aware_transport_port(), true, false, false);
        native_shard_aware_port = true;
    }

    // Turning an already added listener into an encrypted one renames it, as
    // the name spells out what the listener is.
    auto encrypt = [&] (bool shard_aware) {
        auto node = listeners.extract(name_of(shard_aware, false, false));
        node.key() = name_of(shard_aware, true, false);
        node.mapped().tls = true;
        listeners.insert(std::move(node));
    };

    const bool tls_enabled = utils::is_true(utils::get_or_default(cfg.client_encryption_options(), "enabled", "false"));
    if (tls_enabled) {
        if (cfg.native_transport_port_ssl.is_set() &&
                (!cfg.native_transport_port.is_set() ||
                cfg.native_transport_port_ssl() != cfg.native_transport_port())) {
            // SSL port is specified && non-SSL port is either left out or set to a different value
            add(cfg.native_transport_port_ssl(), false, true, false);
        } else if (native_port) {
            encrypt(false);
        }
        if (cfg.native_shard_aware_transport_port_ssl.is_set() &&
                (!cfg.native_shard_aware_transport_port.is_set() ||
                cfg.native_shard_aware_transport_port_ssl() != cfg.native_shard_aware_transport_port())) {
            add(cfg.native_shard_aware_transport_port_ssl(), true, true, false);
        } else if (native_shard_aware_port) {
            encrypt(true);
        }
    }

    // Proxy protocol ports (disabled by default, port 0 means disabled)
    if (cfg.native_transport_port_proxy_protocol()) {
        add(cfg.native_transport_port_proxy_protocol(), false, false, true);
    }
    if (cfg.native_shard_aware_transport_port_proxy_protocol()) {
        add(cfg.native_shard_aware_transport_port_proxy_protocol(), true, false, true);
    }
    if (cfg.native_transport_port_ssl_proxy_protocol() && tls_enabled) {
        add(cfg.native_transport_port_ssl_proxy_protocol(), false, true, true);
    }
    if (cfg.native_shard_aware_transport_port_ssl_proxy_protocol() && tls_enabled) {
        add(cfg.native_shard_aware_transport_port_ssl_proxy_protocol(), true, true, true);
    }

    // Hand the clients of every listener over to the shard-aware listener they
    // can actually connect to - the one that agrees on encryption and on the
    // proxy protocol.
    for (auto& [name, listener] : listeners) {
        if (listener.shard_aware) {
            continue;
        }
        auto sibling = name_of(true, listener.tls, listener.proxy_protocol);
        if (listeners.contains(sibling)) {
            listener.shard_aware_listener = sibling;
        }
    }

    return listeners;
}

listener_configs get_listeners(const config& cfg, listener_config::protocol_type protocol) {
    switch (protocol) {
    case listener_config::protocol_type::cql:
        return legacy_cql_listeners(cfg);
    }
    return {};
}

}

auto fmt::formatter<db::listener_config::protocol_type>::format(db::listener_config::protocol_type p, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", db::to_string(p));
}

auto fmt::formatter<db::listener_config>::format(const db::listener_config& lc, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = fmt::format_to(ctx.out(), "{{protocol: {}, address: {}, port: {}", lc.protocol,
            lc.address.empty() ? sstring("default") : lc.address, lc.port);
    if (lc.shard_aware) {
        out = fmt::format_to(out, ", shard-aware");
    }
    if (lc.proxy_protocol) {
        out = fmt::format_to(out, ", proxy-protocol");
    }
    out = fmt::format_to(out, ", {}", lc.tls ? "encrypted" : "unencrypted");
    return fmt::format_to(out, "}}");
}
