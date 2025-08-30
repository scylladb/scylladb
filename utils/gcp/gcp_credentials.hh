/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <variant>
#include <string>
#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/net/tls.hh>

#include "utils/rjson.hh"

namespace rest {
    class http_log_filter;
}

namespace utils::gcp {

    using timeout_clock = std::chrono::system_clock;
    using timestamp_type = typename timeout_clock::time_point;
    using scopes_type = std::string; // space separated. avoids some transforms. makes other easy.

    struct access_token;
    struct user_credentials;
    struct service_account_credentials;
    struct impersonated_service_account_credentials;
    struct compute_engine_credentials{};

    struct google_credentials;

    extern const char AUTHORIZATION[];

    struct access_token {
        access_token() = default;
        access_token(const rjson::value&);

        std::string token;
        timestamp_type expiry;
        scopes_type scopes;

        bool empty() const;
        bool expired() const;
    };

    struct user_credentials {
        user_credentials(const rjson::value&);

        std::string client_id;
        std::string client_secret;
        std::string refresh_token;
        std::string access_token;
        std::string quota_project_id;
    };

    struct service_account_credentials {
        service_account_credentials(const rjson::value&);

        std::string client_id;
        std::string client_email;
        std::string private_key_id;
        std::string private_key_pkcs8;
        std::string token_server_uri;
        std::string project_id;
        std::string quota_project_id;
    };

    struct impersonated_service_account_credentials {
        impersonated_service_account_credentials(std::string principal, google_credentials&&);
        impersonated_service_account_credentials(const rjson::value&);

        std::vector<std::string> delegates;
        std::vector<std::string> scopes;
        std::string quota_project_id;
        std::string iam_endpoint_override;
        std::string target_principal;

        std::unique_ptr<google_credentials> source_credentials;
        access_token token;
    };

    using credentials_variant = std::variant<
        user_credentials,
        service_account_credentials,
        impersonated_service_account_credentials,
        compute_engine_credentials
    >;

    using scope_implies_other_scope_pred = std::function<bool(const scopes_type&, const scopes_type&)>;

    bool default_scopes_implies_other_scope(const scopes_type&, const scopes_type&);
    bool scopes_contains_scope(const scopes_type&, std::string_view);

    struct google_credentials {
        google_credentials(google_credentials&&) = default;
        google_credentials(credentials_variant&& c)
            : credentials(std::move(c))
        {}
        google_credentials& operator=(google_credentials&&) = default;

        credentials_variant credentials;
        access_token token;

        future<> refresh(const scopes_type&, shared_ptr<tls::certificate_credentials> = {});
        future<> refresh(const scopes_type&, scope_implies_other_scope_pred, shared_ptr<tls::certificate_credentials> = {});

        static google_credentials from_data(std::string_view);
        static google_credentials from_data(const temporary_buffer<char>&);
        static future<google_credentials> from_file(const std::string& path);

        static future<google_credentials> get_default_credentials();
    };

    class bad_configuration : public std::runtime_error {
    public:
        bad_configuration(const std::string&);
    };

    std::string format_bearer(const access_token&);

    const rest::http_log_filter& bearer_filter();
}
