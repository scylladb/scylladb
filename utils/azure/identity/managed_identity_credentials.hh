/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/rjson.hh"
#include "credentials.hh"

namespace azure {

class managed_identity_credentials : public credentials {
    static constexpr char NAME[] = "ManagedIdentityCredentials";
    static constexpr char IMDS_HOST[] = "169.254.169.254";
    static constexpr char IMDS_API_VERSION[] = "2018-02-01";
    static constexpr char IMDS_TOKEN_PATH_TEMPLATE[] = "/metadata/identity/oauth2/token?api-version={}&resource={}";

    sstring _host{IMDS_HOST};
    unsigned _port{80};

    std::string_view get_name() const override { return NAME; };
    future<> with_retries(std::function<future<>()>);
    future<> refresh(const resource_type& resource_uri) override;
    access_token make_token(const rjson::value&, const resource_type&);
public:
    managed_identity_credentials(const sstring& endpoint = "", credential_logger logger = {});
};

}