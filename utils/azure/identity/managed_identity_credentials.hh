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

class managed_identity_credentials : public credentials {
    static constexpr char NAME[] = "ManagedIdentityCredentials";
    static constexpr char IMDS_HOST[] = "169.254.169.254";

    sstring _host{IMDS_HOST};
    unsigned _port{80};

    std::string_view get_name() const override { return NAME; };
    future<> refresh(const resource_type& resource_uri) override;
public:
    managed_identity_credentials(const sstring& endpoint = "", credential_logger logger = {});
};

}