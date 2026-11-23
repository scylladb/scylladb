/*
 * Copyright (C) 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/native_transport_config.hh"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <memory>
#include <istream>
#include <ostream>
#include <set>
#include <system_error>
#include <ifaddrs.h>
#include <net/if.h>
#include <yaml-cpp/yaml.h>
#include <seastar/json/formatter.hh>
#include <seastar/net/api.hh>
#include "utils/log.hh"

namespace db {

namespace {
logging::logger logger("unified_port");
using seastar::net::inet_address;
using seastar::sstring;

void check_fields(const YAML::Node& node, std::initializer_list<std::string_view> allowed) {
    if (!node.IsMap()) {
        throw std::invalid_argument("unified listener configuration must be a mapping");
    }
    std::set<std::string> seen;
    for (const auto& entry : node) {
        auto name = entry.first.as<std::string>();
        if (std::ranges::find(allowed, name) == allowed.end() || !seen.insert(name).second) {
            throw std::invalid_argument(fmt::format("unknown or duplicate unified listener field: {}", name));
        }
    }
}

std::vector<sstring> string_list(const YAML::Node& node) {
    if (!node.IsSequence()) {
        throw std::invalid_argument("proxy trust entries must be lists of strings");
    }
    std::vector<sstring> result;
    for (const auto& entry : node) {
        result.emplace_back(entry.as<std::string>());
    }
    return result;
}

inet_address unmap_ipv4(inet_address address) {
    if (address.is_ipv6()) {
        auto ip6 = static_cast<in6_addr>(address);
        if (IN6_IS_ADDR_V4MAPPED(&ip6)) {
            in_addr ip4;
            std::memcpy(&ip4, ip6.s6_addr + 12, sizeof(ip4));
            return inet_address(ip4);
        }
    }
    return address;
}

struct proxy_subnet {
    inet_address address;
    unsigned prefix;

    proxy_subnet(inet_address network, unsigned mask_bits) : address(network), prefix(mask_bits) {
        if (prefix >= 96) {
            auto normalized = unmap_ipv4(address);
            if (normalized.is_ipv4()) {
                address = normalized;
                prefix -= 96;
            }
        }
    }

    bool contains(inet_address peer) const {
        // Dual-stack listeners can report IPv4 peers as IPv4-mapped IPv6 addresses.
        peer = unmap_ipv4(peer);
        if (peer.in_family() != address.in_family()) {
            return false;
        }
        auto* a = static_cast<const uint8_t*>(address.data());
        auto* p = static_cast<const uint8_t*>(peer.data());
        const auto bytes = prefix / 8;
        const auto bits = prefix % 8;
        return std::memcmp(a, p, bytes) == 0 && (!bits || ((a[bytes] ^ p[bytes]) & (0xff << (8 - bits))) == 0);
    }
};

proxy_subnet parse_subnet(const sstring& text) {
    auto slash = text.find('/');
    auto address = inet_address::parse_numerical(text.substr(0, slash));
    if (!address) {
        throw std::invalid_argument(fmt::format("invalid trusted proxy address: {}", text));
    }
    const unsigned width = address->is_ipv4() ? 32 : 128;
    unsigned prefix = width;
    if (slash != sstring::npos) {
        const auto* begin = text.data() + slash + 1;
        const auto* end = text.data() + text.size();
        auto [last, ec] = std::from_chars(begin, end, prefix);
        if (ec != std::errc() || last != end || prefix > width) {
            throw std::invalid_argument(fmt::format("invalid trusted proxy CIDR: {}", text));
        }
    }
    return {*address, prefix};
}

inet_address interface_address(const sockaddr* sa) {
    if (sa->sa_family == AF_INET) {
        return inet_address(reinterpret_cast<const sockaddr_in*>(sa)->sin_addr);
    }
    return inet_address(reinterpret_cast<const sockaddr_in6*>(sa)->sin6_addr);
}

unsigned prefix_length(const inet_address& mask) {
    auto* bytes = static_cast<const uint8_t*>(mask.data());
    unsigned prefix = 0;
    bool zero = false;
    for (size_t i = 0; i < mask.size(); ++i) {
        for (int bit = 7; bit >= 0; --bit) {
            if (bytes[i] & (1u << bit)) {
                if (zero) {
                    throw std::invalid_argument("non-contiguous interface netmask");
                }
                ++prefix;
            } else {
                zero = true;
            }
        }
    }
    return prefix;
}
}

native_transport_unified_port_config native_transport_unified_port_config::decode(const YAML::Node& node) {
    check_fields(node, {"port", "encryption", "shard_routing"});
    native_transport_unified_port_config result;
    if (auto value = node["port"]) {
        auto port = value.as<int64_t>();
        if (port < 0 || port > 65535) {
            throw std::invalid_argument("unified listener port must be between 0 and 65535");
        }
        result.port = port;
    }
    if (auto value = node["encryption"]) {
        result.encryption = value.as<std::string>();
    }
    if (auto routing = node["shard_routing"]) {
        check_fields(routing, {"default", "ppv2"});
        if (auto value = routing["default"]) {
            result.default_routing = value.as<std::string>();
        }
        if (auto proxy = routing["ppv2"]) {
            check_fields(proxy, {"mode", "trusted_proxies"});
            if (auto value = proxy["mode"]) {
                result.ppv2 = value.as<std::string>();
            }
            if (auto trust = proxy["trusted_proxies"]) {
                check_fields(trust, {"interface_subnets", "addresses"});
                if (auto value = trust["interface_subnets"]) {
                    result.interface_subnets = string_list(value);
                }
                if (auto value = trust["addresses"]) {
                    result.addresses = string_list(value);
                }
            }
        }
    }
    result.validate();
    return result;
}

void native_transport_unified_port_config::validate() const {
    if ((port || !encryption.empty()) && encryption != "plain-only" && encryption != "tls-only" && encryption != "both") {
        throw std::invalid_argument("unified listener encryption must be plain-only, tls-only, or both");
    }
    if (default_routing != "balanced" && default_routing != "source-port") {
        throw std::invalid_argument("shard_routing.default must be balanced or source-port");
    }
    if (ppv2 != "disabled" && ppv2 != "optional" && ppv2 != "required") {
        throw std::invalid_argument("ppv2.mode must be disabled, optional, or required");
    }
    for (const auto& address : addresses) {
        if (address != "*") {
            parse_subnet(address);
        }
    }
    for (const auto& name : interface_subnets) {
        if (name.empty()) {
            throw std::invalid_argument("trusted proxy interface name must not be empty");
        }
    }
}

seastar::unified_port_options native_transport_unified_port_config::make_listen_options() const {
    validate();
    seastar::unified_port_options result;
    using options = seastar::unified_port_options;
    result.encryption = encryption == "tls-only" ? options::encryption_mode::tls_only
        : encryption == "plain-only" ? options::encryption_mode::plain_only : options::encryption_mode::both;
    result.proxy_protocol = ppv2 == "required" ? options::proxy_protocol_mode::required
        : ppv2 == "optional" ? options::proxy_protocol_mode::optional : options::proxy_protocol_mode::disabled;
    result.source_port_routing = default_routing == "source-port";

    std::vector<proxy_subnet> subnets;
    for (const auto& address : addresses) {
        if (address == "*") {
            subnets.push_back(parse_subnet("0.0.0.0/0"));
            subnets.push_back(parse_subnet("::/0"));
        } else {
            subnets.push_back(parse_subnet(address));
        }
    }
    if (!interface_subnets.empty()) {
        ifaddrs* raw = nullptr;
        if (getifaddrs(&raw) < 0) {
            throw std::system_error(errno, std::system_category(), "enumerate trusted proxy interface subnets");
        }
        auto entries = std::unique_ptr<ifaddrs, decltype(&freeifaddrs)>(raw, freeifaddrs);
        const bool all = std::ranges::find(interface_subnets, "*") != interface_subnets.end();
        std::set<sstring> resolved;
        for (auto* entry = entries.get(); entry; entry = entry->ifa_next) {
            const bool named = std::ranges::find(interface_subnets, entry->ifa_name) != interface_subnets.end();
            if (!named && !(all && (entry->ifa_flags & IFF_UP) && !(entry->ifa_flags & IFF_LOOPBACK))) {
                continue;
            }
            if (!entry->ifa_addr || !entry->ifa_netmask
                    || (entry->ifa_addr->sa_family != AF_INET && entry->ifa_addr->sa_family != AF_INET6)) {
                continue;
            }
            subnets.push_back({interface_address(entry->ifa_addr), prefix_length(interface_address(entry->ifa_netmask))});
            resolved.emplace(entry->ifa_name);
        }
        for (const auto& name : interface_subnets) {
            if (name != "*" && !resolved.contains(name)) {
                throw std::invalid_argument(fmt::format("trusted proxy interface {} has no usable IP subnet", name));
            }
        }
    }
    for (const auto& subnet : subnets) {
        logger.info("Trusting proxy subnet {}/{}", subnet.address, subnet.prefix);
    }
    result.trusted_proxy = [subnets = std::move(subnets)] (const inet_address& peer) {
        return std::ranges::any_of(subnets, [&peer] (const proxy_subnet& subnet) { return subnet.contains(peer); });
    };
    return result;
}

std::istream& operator>>(std::istream& in, native_transport_unified_port_config& config) {
    std::string text(std::istreambuf_iterator<char>(in), {});
    config = native_transport_unified_port_config::decode(YAML::Load(text));
    return in;
}

std::ostream& operator<<(std::ostream& out, const native_transport_unified_port_config& config) {
    using seastar::json::formatter;
    return out << fmt::format(
        R"json({{"port":{},"encryption":{},"shard_routing":{{"default":{},)json"
        R"json("ppv2":{{"mode":{},"trusted_proxies":{{"interface_subnets":{},"addresses":{}}}}}}}}})json",
        config.port, formatter::to_json(config.encryption), formatter::to_json(config.default_routing),
        formatter::to_json(config.ppv2), formatter::to_json(config.interface_subnets), formatter::to_json(config.addresses));
}

}
