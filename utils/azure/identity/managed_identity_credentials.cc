/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/regex.hpp>

#include <seastar/net/dns.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>

#include "utils/rest/client.hh"
#include "exceptions.hh"
#include "managed_identity_credentials.hh"

namespace azure {

class mi_log_filter : public rest::nop_log_filter {
public:
    string_opt filter_body(body_type type, std::string_view body) const override {
        if (type == body_type::response && !body.empty()) {
            auto j = rjson::parse(body);
            auto val = rjson::find(j, "access_token");
            if (val) {
                val->SetString(REDACTED_VALUE);
                return rjson::print(j);
            }
        }
        return std::nullopt;
    }
};

managed_identity_credentials::managed_identity_credentials(const sstring& endpoint, const sstring& logctx)
    : credentials(logctx)
{
    if (endpoint.empty()) {
        return;
    }
    // Regex for the IMDS endpoint.
    // Expected format: [http://]<host>[:port]
    static const boost::regex uri_pattern(R"((?:http://)?([^/:]+)(?::(\d+))?)");
    boost::match_results<std::string_view::const_iterator> match;
    std::string_view endpoint_view{endpoint};
    if (boost::regex_match(endpoint_view.begin(), endpoint_view.end(), match, uri_pattern)) {
        _host = match[1].str();
        if (match[2].matched) {
            _port = std::stoi(match[2].str());
        }
    } else {
        throw std::invalid_argument(fmt::format("Invalid IMDS endpoint '{}'. Expected format: [http://]<host>[:port]", endpoint));
    }
}

access_token managed_identity_credentials::make_token(const rjson::value& json, const resource_type& resource_uri) {
    auto token = rjson::get<std::string>(json, "access_token");
    auto expires_in_str = rjson::get<std::string>(json, "expires_in");
    if (auto expires_in_int = std::atoi(expires_in_str.c_str())) {
        return { token, timeout_clock::now() + std::chrono::seconds(expires_in_int), resource_uri };
    }
    throw std::runtime_error(seastar::format("Invalid expires_in value: {}", expires_in_str));
}

/**
 * @brief Retries for transient errors.
 *
 * Retries are performed for 404, 429, and 5xx errors, using an exponential backoff strategy.
 * Three retries are attempted in total.
 * The latencies between retries are 0, 100, and 300 milliseconds.
 *
 * Based on the official Azure docs, but with a reduced number of retries and backoff delay
 * to prioritize responsiveness (longer transient errors should be handled by upper layers):
 * https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#retry-guidance
 *
 * @param func A callable that returns a future<>, representing the asynchronous operation to retry.
 * @return A future<> that resolves to the result of the operation if successful, or propagates the error if all retries fail.
 */
future<> managed_identity_credentials::with_retries(std::function<future<>()> func) {
    constexpr int MAX_RETRIES = 3;
    constexpr std::chrono::milliseconds DELTA_BACKOFF {100};
    std::chrono::milliseconds backoff;

    int retries = 0;
    while (true) {
        try {
            co_return co_await func();
        } catch (const creds_auth_error& e) {
            auto status = e.status();
            bool should_retry =
                    status == http::reply::status_type::not_found ||
                    status == http::reply::status_type::too_many_requests ||
                    http::reply::classify_status(status) == http::reply::status_class::server_error;

            if (retries >= MAX_RETRIES || !should_retry) {
                throw;
            }

            backoff = DELTA_BACKOFF * ((1 << retries) - 1);
            az_creds_logger.debug("[{}] Token request failed with status {}. Reason: {}. Retrying in {} ms...",
                    *this, static_cast<int>(status), e.what(), backoff.count());

            retries++;
        }
        co_await seastar::sleep(backoff);
    }
}

/**
 * @brief Check connectivity to the IMDS endpoint
 *
 * Azure offers no node-local indicator to determine if the node is an Azure VM.
 * The only way to check is to try to connect to the IMDS endpoint.
 *
 * This method attempts to establish a TCP connection to the IMDS endpoint and
 * assumes the service is unreachable after 3 seconds.
 *
 * Ideally, we should be able to do that via the rest HTTP client, but it
 * offers no timeout or cancellation API, so we would have to wait for the
 * system timeout (connect(2) ETIMEDOUT), which can take several minutes.
 *
 * @throws timed_out_error if the connection attempt times out.
 */
future<> managed_identity_credentials::check_connectivity() {
    az_creds_logger.debug("[{}] Checking connectivity to IMDS endpoint", *this);

    const auto timeout = std::chrono::seconds(3);
    auto sock = make_socket();
    auto addr = co_await net::dns::resolve_name(_host, net::inet_address::family::INET);
    gate g;
    std::exception_ptr ex;

    try {
        co_await with_timeout(timer<>::clock::now() + timeout, with_gate(g, [&sock, addr, this]() {
            return sock.connect(socket_address(addr, static_cast<uint16_t>(_port)));
        }));
    } catch (const timed_out_error&) {
        ex = std::current_exception();
    } catch (const std::system_error&) {
        ex = std::current_exception();
    }

    sock.shutdown();
    co_await g.close();

    if (ex) {
        std::rethrow_exception(ex);
    }
}

/**
 * Token request from IMDS.
 * https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http
 */
future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    if (!_connectivity_checked) {
        co_await check_connectivity();
        _connectivity_checked = true;
    }

    az_creds_logger.debug("[{}] Refreshing token", *this);

    co_await with_retries([this, &resource_uri] () -> future<> {
        static const mi_log_filter filter{};

        const auto op = httpd::operation_type::GET;
        const auto path = seastar::format(IMDS_TOKEN_PATH_TEMPLATE, IMDS_API_VERSION, resource_uri);

        rest::httpclient client{_host, _port, nullptr, std::nullopt};
        client.target(path);
        client.method(op);
        client.add_header("Metadata", "true");

        if (az_creds_logger.is_enabled(log_level::trace)) {
            az_creds_logger.trace("[{}] Sending request: {}", *this, rest::redacted_request_type{ client.request(), filter });
        }

        auto res = co_await client.send();
        if (res.result() == http::reply::status_type::ok) {
            if (az_creds_logger.is_enabled(log_level::trace)) {
                az_creds_logger.trace("[{}] Got response: {}", *this, rest::redacted_result_type{ res, filter });
            }
        } else {
            if (az_creds_logger.is_enabled(log_level::trace)) {
                az_creds_logger.trace("[{}] Got unexpected response: {}", *this, rest::redacted_result_type{ res, filter });
            }
            throw creds_auth_error::make_error(res.result(), res.body());
        }
        _token = make_token(rjson::parse(res.body()), resource_uri);
    });
}

}