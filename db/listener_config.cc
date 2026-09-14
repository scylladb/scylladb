/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/listener_config.hh"

#include <istream>
#include <algorithm>
#include <ranges>
#include <map>
#include <tuple>
#include <unordered_set>
#include <stdexcept>

#include <yaml-cpp/yaml.h>

#include "db/config.hh"
#include "utils/config_file.hh"

namespace db {

std::string_view to_string(listener_config::protocol_type p) {
    switch (p) {
    case listener_config::protocol_type::cql:
        return "cql";
    case listener_config::protocol_type::alternator:
        return "alternator";
    }
    return "unknown";
}

static listener_config::protocol_type parse_protocol(const sstring& s) {
    if (s == "cql") {
        return listener_config::protocol_type::cql;
    }
    if (s == "alternator") {
        return listener_config::protocol_type::alternator;
    }
    throw std::invalid_argument(fmt::format("Bad listener configuration: unknown protocol '{}', expected 'cql' or 'alternator'", s));
}

listener_config listener_config::decode(const YAML::Node& node) {
    if (!node.IsMap()) {
        throw std::invalid_argument("Bad listener configuration: a listener must be a YAML map");
    }
    listener_config lc;

    static const std::unordered_set<std::string> known_keys = {
        "protocol", "address", "port", "shard_aware", "shard_aware_listener",
        "proxy_protocol", "tls", "keepalive",
    };
    // The options utils::configure_tls_creds_builder() reads, spelled as it spells them
    // ("certficate_revocation_list" included), plus "enabled", which it doesn't read but
    // client_encryption_options carries, so that it gets an error of its own below.
    static const std::unordered_set<std::string> known_tls_options = {
        "enabled", "certificate", "keyfile", "truststore", "certficate_revocation_list",
        "priority_string", "require_client_auth", "enable_session_tickets",
    };
    for (const auto& e : node) {
        auto key = e.first.as<std::string>();
        if (!known_keys.contains(key)) {
            throw std::invalid_argument(fmt::format("Bad listener configuration: unknown key '{}'", key));
        }
    }

    auto get = [&] (const char* name) -> YAML::Node {
        return node[name];
    };

    if (auto n = get("protocol")) {
        lc.protocol = parse_protocol(n.as<std::string>());
    } else {
        throw std::invalid_argument("Bad listener configuration: mandatory 'protocol' is missing");
    }
    if (auto n = get("address")) {
        lc.address = n.as<std::string>();
    }
    if (auto n = get("port")) {
        lc.port = n.as<uint16_t>();
    } else {
        throw std::invalid_argument("Bad listener configuration: mandatory 'port' is missing");
    }
    if (auto n = get("shard_aware")) {
        lc.shard_aware = n.as<bool>();
    }
    if (auto n = get("shard_aware_listener")) {
        lc.shard_aware_listener = n.as<std::string>();
    }
    if (auto n = get("proxy_protocol")) {
        lc.proxy_protocol = n.as<bool>();
    }
    if (auto n = get("keepalive")) {
        lc.keepalive = n.as<bool>();
    }
    // "tls" is either a boolean, selecting the protocol's default encryption
    // options, or a map spelling the encryption options out.
    if (auto n = get("tls")) {
        if (n.IsMap()) {
            lc.tls = true;
            for (const auto& e : n) {
                auto key = e.first.as<std::string>();
                // A misspelled option would otherwise be silently ignored, and a
                // misspelled require_client_auth would leave client certificates unchecked.
                if (!known_tls_options.contains(key)) {
                    throw std::invalid_argument(fmt::format("Bad listener configuration: unknown tls option '{}'", key));
                }
                // "enabled" says nothing here: spelling the options out already
                // means encrypted. Ignoring it would still be wrong, since even a map
                // holding nothing else replaces the protocol's default options.
                if (key == "enabled") {
                    throw std::invalid_argument("Bad listener configuration: 'enabled' has no meaning in a listener's tls options; "
                            "use 'tls: true' for the protocol's default encryption options");
                }
                lc.tls_options[key] = e.second.as<std::string>();
            }
        } else {
            lc.tls = n.as<bool>();
        }
    }

    if (lc.port == 0) {
        throw std::invalid_argument("Bad listener configuration: 'port' must not be zero");
    }
    // "keepalive" joins the CQL-only list because the Alternator listeners are served by
    // seastar's httpd, whose keepalive setting is a property of the whole http_server rather
    // than of an individual listener, so a per-listener value cannot be honoured there. Reject
    // it rather than accept it and quietly ignore it. See scylladb/scylladb#27405.
    for (auto& [field, unsupported] : {std::pair("shard_aware", lc.shard_aware), std::pair("shard_aware_listener", !lc.shard_aware_listener.empty()), std::pair("keepalive", lc.keepalive.has_value())}) {
        if (unsupported && lc.protocol != protocol_type::cql) {
            throw std::invalid_argument(fmt::format("Bad listener configuration: '{}' is not supported by the {} protocol", field, to_string(lc.protocol)));
        }
    }

    return lc;
}

listener_configs decode_listeners(std::string_view value) {
    auto node = YAML::Load(std::string(value));
    if (!node.IsMap()) {
        throw std::invalid_argument("Bad listeners configuration: expected a map of listener name to listener");
    }
    listener_configs listeners;
    for (const auto& e : node) {
        auto name = e.first.as<std::string>();
        if (name.empty()) {
            throw std::invalid_argument("Bad listeners configuration: a listener name must not be empty");
        }
        auto [it, inserted] = listeners.emplace(name, listener_config::decode(e.second));
        if (!inserted) {
            throw std::invalid_argument(fmt::format("Bad listeners configuration: duplicate listener '{}'", name));
        }
    }

    // Two listeners on the same address and port could never both be opened. Catch it
    // here, when the option is set, rather than when a node fails to bind the second one.
    // Only listeners that spell their address the same way can be compared: what an
    // address resolves to, or which address a default stands for, is up to each node.
    std::map<std::tuple<std::string_view, listener_config::protocol_type, uint16_t>, std::string_view> taken;
    for (const auto& [name, l] : listeners) {
        // An empty address is a different default for each protocol; a spelled-out one is
        // the same address whatever the protocol.
        auto protocol = l.address.empty() ? l.protocol : listener_config::protocol_type::cql;
        auto [it, inserted] = taken.emplace(std::tuple(std::string_view(l.address), protocol, l.port), std::string_view(name));
        if (!inserted) {
            // The initializer_list overload returns the values; the two-argument one would
            // return references, one of them to the temporary string_view of `name`.
            auto [first, second] = std::minmax({it->second, std::string_view(name)});
            throw std::invalid_argument(fmt::format("Bad listeners configuration: listeners '{}' and '{}' are both on {}{}port {}",
                    first, second, l.address, l.address.empty() ? "" : " ", l.port));
        }
    }
    // A listener can only hand its clients over to a shard-aware CQL listener that
    // agrees with it about the PROXY header, since a client reconnects the way it
    // connected. Catch a reference that can never work here, so that it is never
    // stored to fail the node's next start. Whether the two are on the same address
    // is up to each node, and is checked when the listeners are opened.
    for (const auto& [name, l] : listeners) {
        if (l.shard_aware_listener.empty()) {
            continue;
        }
        auto sibling = listeners.find(l.shard_aware_listener);
        if (sibling == listeners.end()) {
            throw std::invalid_argument(fmt::format("Bad listeners configuration: listener '{}' names shard_aware_listener '{}', which doesn't exist",
                    name, l.shard_aware_listener));
        }
        if (sibling->second.protocol != listener_config::protocol_type::cql || !sibling->second.shard_aware) {
            throw std::invalid_argument(fmt::format("Bad listeners configuration: listener '{}' names shard_aware_listener '{}', which isn't a shard-aware CQL listener",
                    name, l.shard_aware_listener));
        }
        if (sibling->second.proxy_protocol != l.proxy_protocol) {
            throw std::invalid_argument(fmt::format("Bad listeners configuration: listener '{}' names shard_aware_listener '{}', which {} a PROXY protocol header",
                    name, l.shard_aware_listener, sibling->second.proxy_protocol ? "expects" : "doesn't accept"));
        }
    }
    return listeners;
}

std::optional<sstring> validate_listeners(std::string_view value) {
    try {
        decode_listeners(value);
    } catch (const std::exception& e) {
        // Both the YAML parser and listener_config::decode() report through exceptions, and
        // the registry wants a reason rather than a throw.
        return sstring(e.what());
    }
    return std::nullopt;
}

sstring canonicalize_listeners(std::string_view value) {
    // Only ever reached for a value validate_listeners() accepted, so the parse cannot fail.
    // Re-emit it rather than store what the user wrote: flow style keeps the value on one
    // line, a fixed key order and the omission of defaulted keys make two spellings of the
    // same set of listeners compare equal, and a value that round-trips through
    // decode_listeners() is one a node can be sure it will be able to read back.
    auto listeners = decode_listeners(value);

    // std::unordered_map has no order of its own, and a stored value that reshuffled itself
    // would look like a change to every consumer watching it.
    std::vector<std::pair<sstring, const listener_config*>> sorted;
    sorted.reserve(listeners.size());
    for (const auto& [name, listener] : listeners) {
        sorted.emplace_back(name, &listener);
    }
    std::ranges::sort(sorted, std::less<>{}, [] (const auto& e) { return std::string_view(e.first); });

    YAML::Emitter out;
    out << YAML::Flow << YAML::BeginMap;
    for (const auto& [name, listener] : sorted) {
        out << YAML::Key << std::string(name) << YAML::Value << YAML::BeginMap;
        out << YAML::Key << "protocol" << YAML::Value << std::string(to_string(listener->protocol));
        out << YAML::Key << "port" << YAML::Value << listener->port;
        if (!listener->address.empty()) {
            out << YAML::Key << "address" << YAML::Value << std::string(listener->address);
        }
        if (listener->shard_aware) {
            out << YAML::Key << "shard_aware" << YAML::Value << true;
        }
        if (!listener->shard_aware_listener.empty()) {
            out << YAML::Key << "shard_aware_listener" << YAML::Value << std::string(listener->shard_aware_listener);
        }
        if (listener->proxy_protocol) {
            out << YAML::Key << "proxy_protocol" << YAML::Value << true;
        }
        if (listener->tls) {
            out << YAML::Key << "tls" << YAML::Value;
            if (listener->tls_options.empty()) {
                out << true;
            } else {
                // Sorted for the same reason the listeners themselves are.
                std::vector<std::pair<sstring, sstring>> opts(listener->tls_options.begin(), listener->tls_options.end());
                std::ranges::sort(opts, std::less<>{}, [] (const auto& e) { return std::string_view(e.first); });
                out << YAML::BeginMap;
                for (const auto& [k, v] : opts) {
                    out << YAML::Key << std::string(k) << YAML::Value << std::string(v);
                }
                out << YAML::EndMap;
            }
        }
        if (listener->keepalive) {
            out << YAML::Key << "keepalive" << YAML::Value << *listener->keepalive;
        }
        out << YAML::EndMap;
    }
    out << YAML::EndMap;
    return sstring(out.c_str());
}

std::istream& operator>>(std::istream& is, listener_config& lc) {
    std::string s{std::istreambuf_iterator<char>(is), std::istreambuf_iterator<char>()};
    lc = listener_config::decode(YAML::Load(s));
    return is;
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

static listener_configs legacy_alternator_listeners(const config& cfg) {
    using protocol_type = listener_config::protocol_type;

    listener_configs listeners;
    auto add = [&] (uint16_t port, bool tls, bool proxy_protocol) {
        if (!port) {
            return;
        }
        auto name = fmt::format("alternator{}{}", tls ? "_https" : "", proxy_protocol ? "_proxy_protocol" : "");
        listeners.emplace(std::move(name), listener_config{
            .protocol = protocol_type::alternator,
            .address = cfg.alternator_address(),
            .port = port,
            .proxy_protocol = proxy_protocol,
            .tls = tls,
        });
    };

    add(cfg.alternator_port(), false, false);
    add(cfg.alternator_port_proxy_protocol(), false, true);
    add(cfg.alternator_https_port(), true, false);
    add(cfg.alternator_https_port_proxy_protocol(), true, true);

    return listeners;
}

listener_configs get_listeners(const config& cfg, const listener_configs& configured, listener_config::protocol_type protocol) {
    // A non-empty "listeners" option is the sole source of truth, and overrides
    // all of the per-protocol options.
    if (!configured.empty()) {
        auto listeners = configured;
        std::erase_if(listeners, [protocol] (const auto& e) { return e.second.protocol != protocol; });
        return listeners;
    }

    switch (protocol) {
    case listener_config::protocol_type::cql:
        return legacy_cql_listeners(cfg);
    case listener_config::protocol_type::alternator:
        return legacy_alternator_listeners(cfg);
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
