/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/regex.hpp>

#include "db/config.hh"
#include "utils/rest/client.hh"
#include "exceptions.hh"
#include "service_principal_credentials.hh"

namespace azure {

class sp_log_filter : public rest::nop_log_filter {
public:
    string_opt filter_body(body_type type, std::string_view body) const override {
        if (type == body_type::request) {
            for (const auto& param : {"client_secret="}) {
                size_t start_pos = body.find(param);
                if (start_pos == std::string_view::npos) {
                    continue;
                }
                size_t value_start_pos = start_pos + std::string(param).length();
                size_t end_pos = body.find('&', value_start_pos);
                if (end_pos == std::string_view::npos) {
                    end_pos = body.length();
                }
                return fmt::format("{}{}{}",
                        body.substr(0, value_start_pos),
                        REDACTED_VALUE,
                        body.substr(end_pos));
            }
        } else if (type == body_type::response) {
            if (!body.empty()) {
                auto j = rjson::parse(body);
                auto val = rjson::find(j, "access_token");
                if (val) {
                    val->SetString(REDACTED_VALUE);
                    return rjson::print(j);
                }
            }
        }
        return std::nullopt;
    }
};

service_principal_credentials::service_principal_credentials(const sstring& tenant_id,
        const sstring& client_id, const sstring& client_secret, const sstring& client_cert,
        const sstring& authority, const sstring& truststore, const sstring& priority_string,
        const sstring& logctx)
    : credentials(logctx)
    , _tenant_id(tenant_id)
    , _client_id(client_id)
    , _client_secret(client_secret)
    , _client_cert(client_cert)
    , _truststore(truststore)
    , _priority_string(priority_string)
{
    if (authority.empty()) {
        return;
    }
    // Regex for the authentication authority URL.
    // Expected format: [http(s)://]<host>[:port]
    static const boost::regex uri_pattern(R"((?:(https?)://)?([^/:]+)(?::(\d+))?)");
    boost::match_results<std::string_view::const_iterator> match;
    std::string_view authority_view{authority};
    if (boost::regex_match(authority_view.begin(), authority_view.end(), match, uri_pattern)) {
        if (match[1].matched) {
            _is_secured = (match[1].str() == "https");
        }
        _host = match[2].str();
        if (match[3].matched) {
            _port = std::stoi(match[3].str());
        }
    } else {
        throw std::invalid_argument(fmt::format("Invalid authentication authority URL '{}'. Expected format: [http(s)://]<host>[:port]", authority));
    }
}

static future<::shared_ptr<tls::certificate_credentials>> make_creds(const sstring& truststore, const sstring& priority_string) {
    auto creds = seastar::make_shared<tls::certificate_credentials>();
    if (!priority_string.empty()) {
        creds->set_priority_string(priority_string);
    } else {
        creds->set_priority_string(db::config::default_tls_priority);
    }
    if (!truststore.empty()) {
        co_await creds->set_x509_trust_file(truststore, seastar::tls::x509_crt_format::PEM);
    } else {
        co_await creds->set_system_trust();
    }
    co_return creds;
}

/**
 * @brief Retries for transient errors.
 *
 * Retries are performed for 408, 429, 500, 502, 503, and 504 errors, using an exponential backoff strategy.
 * Three retries are attempted in total.
 * The latencies between retries are 0, 100, and 300 milliseconds.
 *
 * The error codes are based on the generic retry policy of the Azure C++ SDK:
 * https://github.com/Azure/azure-sdk-for-cpp/blob/126452efd30860263398a152f11f337007f529f4/sdk/core/azure-core/inc/azure/core/http/policies/policy.hpp#L133
 *
 * The exponential backoff strategy follows the one for the `managed_identity_credentials`.
 *
 * @param func The asynchronous function to execute and retry on failure.
 * @return The result of the function if successful, or throws if all retries fail or a non-retriable error occurs.
 */
future<sstring> service_principal_credentials::with_retries(std::function<future<sstring>()> func) {
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
                    status == http::reply::status_type::request_timeout ||
                    status == http::reply::status_type::too_many_requests ||
                    status == http::reply::status_type::internal_server_error ||
                    status == http::reply::status_type::bad_gateway ||
                    status == http::reply::status_type::service_unavailable ||
                    status == http::reply::status_type::gateway_timeout;

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

future<sstring> service_principal_credentials::post(const sstring& body) {
    static const sp_log_filter filter{};
    const auto op = httpd::operation_type::POST;
    const auto path = seastar::format(AZURE_ENTRA_ID_TOKEN_PATH_TEMPLATE, _tenant_id);
    const auto mime_type = MIME_TYPE;

    shared_ptr<tls::certificate_credentials> creds;
    std::optional<seastar::tls::tls_options> options;

    if (_is_secured) {
        creds = co_await make_creds(_truststore, _priority_string);
        options = { .wait_for_eof_on_shutdown = false, .server_name = _host };
    }

    rest::httpclient client{_host, _port, std::move(creds), options};
    client.target(path);
    client.method(op);
    client.add_header("Content-Type", mime_type);
    client.content(std::move(body));

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
    co_return res.body();
}

access_token service_principal_credentials::make_token(const rjson::value& json, const resource_type& resource_uri) {
    auto token = rjson::get<std::string>(json, "access_token");
    auto expires_in = timeout_clock::now() + std::chrono::seconds(rjson::get<int>(json, "expires_in"));
    return { token, expires_in, resource_uri };
}

future<> service_principal_credentials::refresh(const resource_type& resource_uri) {
    az_creds_logger.debug("[{}] Refreshing token for principal: {}", *this, _client_id);
    if (_client_secret != "") {
        co_await refresh_with_secret(resource_uri);
    } else {
        co_await refresh_with_certificate(resource_uri);
    }
}

/**
 * Token request with secret.
 * Based on: https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#first-case-access-token-request-with-a-shared-secret
 */
future<> service_principal_credentials::refresh_with_secret(const resource_type& resource_uri) {
    // Scopes for the client credentials flow must contain only one resource
    // identifier and only the .default scope.
    auto scope = seastar::format("{}/.default", resource_uri);
    sstring grant_type = "client_credentials";
    sstring body = seastar::format(
            "client_id={}&scope={}&client_secret={}&grant_type={}",
            _client_id,
            seastar::http::internal::url_encode(scope),
            seastar::http::internal::url_encode(_client_secret),
            grant_type);
    auto resp = co_await with_retries([&] { return post(body); });
    _token = make_token(rjson::parse(resp), resource_uri);
}

future<> service_principal_credentials::refresh_with_certificate(const resource_type& resource_uri) {
     throw std::logic_error("Not implemented");
}

}