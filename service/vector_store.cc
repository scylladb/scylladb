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
    co_return;
}

} // namespace service

