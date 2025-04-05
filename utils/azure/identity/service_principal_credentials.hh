/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "credentials.hh"

namespace azure {

class service_principal_credentials : public credentials {
    static constexpr char NAME[] = "ServicePrincipalCredentials";
    static constexpr char AZURE_ENTRA_ID_HOST[] = "login.microsoftonline.com";
    sstring _tenant_id;
    sstring _client_id;
    sstring _client_secret;
    sstring _client_cert;
    // TLS options
    sstring _truststore;
    sstring _priority_string;
    // Endpoint
    sstring _host{AZURE_ENTRA_ID_HOST};
    unsigned _port{443};
    bool _is_secured{true};

    std::string_view get_name() const override { return NAME; };
    future<> refresh(const resource_type& resource_uri) override;
public:
    service_principal_credentials(const sstring& tenant_id, const sstring& client_id,
            const sstring& client_secret, const sstring& client_cert,
            const sstring& authority,
            const sstring& truststore = "", const sstring& priority_string = "",
            const sstring& logctx = "");
};

}

template <>
struct fmt::formatter<azure::service_principal_credentials> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const azure::service_principal_credentials& creds, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{}", *dynamic_cast<const azure::credentials*>(&creds));
    }
};