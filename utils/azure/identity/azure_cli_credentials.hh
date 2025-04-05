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

class azure_cli_credentials : public credentials {
    static constexpr char NAME[] = "AzureCliCredentials";

    std::string_view get_name() const override { return NAME; };
    future<> refresh(const resource_type& resource_uri) override;
public:
    azure_cli_credentials(const sstring& logctx = "");
};

}

template <>
struct fmt::formatter<azure::azure_cli_credentials> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const azure::azure_cli_credentials& creds, fmt::format_context& ctxt) const {
        return fmt::format_to(ctxt.out(), "{}", *dynamic_cast<const azure::credentials*>(&creds));
    }
};