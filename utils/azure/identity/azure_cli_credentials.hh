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

class azure_cli_credentials : public credentials {
    static constexpr char NAME[] = "AzureCliCredentials";

    std::string_view get_name() const override { return NAME; };
    static std::vector<sstring> make_env();
    future<> refresh(const resource_type& resource_uri) override;
    future<> do_refresh(const resource_type& resource_uri);
    access_token make_token(const rjson::value&, const resource_type&);
public:
    azure_cli_credentials(credential_logger = {});
};

}