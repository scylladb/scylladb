/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/net/inet_address.hh>
#include <seastar/http/client.hh>
#include <seastar/http/common.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#include "request.hh"
#include "exceptions.hh"
#include "managed_identity_credentials.hh"

namespace azure {

static constexpr char REDACTED_VALUE[] = "[REDACTED]";

static body_filter make_response_filter() {
    return [](std::string_view body) -> std::optional<std::string> {
        if (!body.empty()) {
            auto j = rjson::parse(body);
            auto val = rjson::find(j, "access_token");
            if (val) {
                val->SetString(REDACTED_VALUE);
                return rjson::print(j);
            }
        }
        return std::nullopt;
    };
}

managed_identity_credentials::managed_identity_credentials(const sstring& logctx)
    : credentials(logctx)
{}

access_token managed_identity_credentials::make_token(const rjson::value& json, const resource_type& resource_uri) {
    auto token = rjson::get<std::string>(json, "access_token");
    auto expires_in_str = rjson::get<std::string>(json, "expires_in");
    if (auto expires_in_int = std::atoi(expires_in_str.c_str())) {
        return { token, timeout_clock::now() + std::chrono::seconds(expires_in_int), resource_uri };
    }
    throw std::runtime_error(seastar::format("Invalid expires_in value: {}", expires_in_str));
}

// Token request from IMDS.
// https://docs.azure.cn/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http
future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    log_debug("Refreshing token");

    const auto op = httpd::operation_type::GET;
    const auto host = IMDS_HOST;
    const auto port = 80;
    const auto path = seastar::format(IMDS_TOKEN_PATH_TEMPLATE, IMDS_API_VERSION, resource_uri);

    auto req = http::request::make(op, host, path);
    req._version = "1.1";
    req._headers["Metadata"] = "true";

    if (az_creds_logger.is_enabled(log_level::trace)) {
        log_trace("Sending request: {}", format_request(req));
    }

    auto addr = seastar::net::inet_address(host);
    auto factory = std::make_unique<seastar::http::experimental::basic_connection_factory>(socket_address(addr, port));
    http::experimental::client http_client{std::move(factory)};

    sstring resp;
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
        auto lin = std::move(in);
        resp = co_await util::read_entire_stream_contiguous(lin);
        if (rep._status == http::reply::status_type::ok) {
            if (az_creds_logger.is_enabled(log_level::trace)) {
                log_trace("Got response: {}", format_reply(rep, resp, make_response_filter()));
            }
        } else {
            if (az_creds_logger.is_enabled(log_level::trace)) {
                log_trace("Got unexpected response: {}", format_reply(rep, resp));
            }
            throw creds_auth_error::make_error(rep._status, resp);
        }
    }).finally([&] -> future<> { co_await http_client.close(); });
    token = make_token(rjson::parse(resp), resource_uri);
}

}