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

    const char* get_name() const override { return NAME; };
    future<> refresh(const resource_type& resource_uri) override;
public:
    managed_identity_credentials(const sstring& logctx = "");
};

}