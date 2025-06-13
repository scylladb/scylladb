/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include <charconv>
#include <regex>

// NOLINTNEXTLINE(misc-use-internal-linkage,cppcoreguidelines-avoid-non-const-global-variables)
logging::logger vslogger("vector_store");

namespace {

using configuration_exception = exceptions::configuration_exception;
using host_name = service::vector_store::host_name;
using port_number = service::vector_store::port_number;

auto parse_port(std::string const& port_txt) -> std::optional<port_number> {
    auto port = port_number{};
    auto [ptr, ec] = std::from_chars(&*port_txt.begin(), &*port_txt.end(), port);
    if (*ptr != '\0' || ec != std::errc{}) {
        return std::nullopt;
    }
    return port;
}

auto parse_service_uri(std::string_view uri) -> std::optional<std::tuple<host_name, port_number>> {
    constexpr auto URI_REGEX = R"(^http:\/\/([a-z0-9._-]+):([0-9]+)$)";
    auto const uri_regex = std::regex(URI_REGEX);
    auto uri_match = std::smatch{};
    auto uri_txt = std::string(uri);
    if (!std::regex_match(uri_txt, uri_match, uri_regex) || uri_match.size() != 3) {
        return {};
    }
    auto host = uri_match[1].str();
    auto port = parse_port(uri_match[2].str());
    if (!port) {
        return {};
    }
    return {{host, *port}};
}

/// Stop and remove old clients if they are not used anymore.
auto cleanup_old_clients(std::vector<lw_shared_ptr<client>>& clients) -> future<> {
    for (auto& client : clients) {
        if (client && client.owned()) {
            co_await client->close();
            client = nullptr;
        }
    }
    std::erase_if(clients, [](auto const& client) {
        return !client;
    });
}

} // namespace

namespace service {

vector_store::vector_store(config const& cfg) {
    auto config_uri = cfg.vector_store_uri();
    if (config_uri.empty()) {
        return;
    }

    auto parsed_uri = parse_service_uri(config_uri);
    if (!parsed_uri) {
        throw configuration_exception(format("Invalid Vector Store service URI: {}", config_uri));
    }

    std::tie(_host, _port) = *parsed_uri;
}

vector_store::~vector_store() = default;

auto vector_store::stop() -> future<> {
    if (_client) {
        co_await _client->close();
    }
    _client = nullptr;
    for (auto& client : _old_clients) {
        if (!client) {
            continue;
        }
        co_await client->close();
    }
    _old_clients.clear();
}

auto vector_store::refresh_service_addr() -> future<bool> {
    if (is_disabled()) {
        co_return false;
    }

    // Do not refresh the service address too often
    if (_client) {
        // Minimum period between dns name refreshes
        constexpr auto REFRESH_PERIOD = std::chrono::seconds(5);
        if (lowres_system_clock::now() - _last_dns_refresh < REFRESH_PERIOD) {
            co_return true;
        }
    }

    _last_dns_refresh = lowres_system_clock::now();
    auto addr = inet_address{};
    try {
        addr = co_await net::dns::resolve_name(_host);
    } catch (std::system_error const&) {
        // addr should be empty if the resolution failed
    }
    if (addr.is_addr_any()) {
        if (_client) {
            // If we have a client, save it for later cleanup
            _old_clients.emplace_back(std::move(_client));
        }

        co_await cleanup_old_clients(_old_clients);
        co_return false;
    }

    // Check if the new address is the same as the current one
    if (_client) {
        if (addr == _addr) {
            co_return true; // Address was refreshed but remains the same
        }

        auto old_client = std::move(_client);
        if (old_client.owned()) {
            // Close the old client if it is not in use
            co_await old_client->close();
        } else {
            // Save the old client for later cleanup
            _old_clients.emplace_back(std::move(old_client));
        }
    }

    _addr = addr;
    _client = make_lw_shared<client>(socket_address(_addr, _port));

    co_await cleanup_old_clients(_old_clients);
    co_return true;
}

} // namespace service

