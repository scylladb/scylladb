/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store_client.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include <charconv>
#include <regex>

namespace {

using configuration_exception = exceptions::configuration_exception;
using host_name = service::vector_store_client::host_name;
using port_number = service::vector_store_client::port_number;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
logging::logger vslogger("vector_store_client");

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

} // namespace

namespace service {

struct vector_store_client::impl {
    host_name host;
    port_number port{};
    gate tasks_gate;

    impl(host_name host_, port_number port_)
        : host(std::move(host_))
        , port(port_) {
    }
};

vector_store_client::vector_store_client(config const& cfg) {
    auto config_uri = cfg.vector_store_uri();
    if (config_uri.empty()) {
        vslogger.info("Vector Store service URI is not configured.");
        return;
    }

    auto parsed_uri = parse_service_uri(config_uri);
    if (!parsed_uri) {
        throw configuration_exception(format("Invalid Vector Store service URI: {}", config_uri));
    }

    auto [host, port] = *parsed_uri;
    _impl = std::make_unique<impl>(std::move(host), port);
    vslogger.info("Vector Store service uri = {}:{}.", _impl->host, _impl->port);
}

vector_store_client::~vector_store_client() = default;

void vector_store_client::start_background_tasks() {
    if (is_disabled()) {
        return;
    }
}

auto vector_store_client::stop() -> future<> {
    if (is_disabled()) {
        co_return;
    }
    co_await _impl->tasks_gate.close();
}

auto vector_store_client::host() const -> std::expected<host_name, disabled> {
    if (is_disabled()) {
        return std::unexpected{disabled{}};
    }
    return {_impl->host};
}

auto vector_store_client::port() const -> std::expected<port_number, disabled> {
    if (is_disabled()) {
        return std::unexpected{disabled{}};
    }
    return {_impl->port};
}

} // namespace service

