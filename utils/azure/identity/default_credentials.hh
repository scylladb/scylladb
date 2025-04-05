/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "enum_set.hh"
#include "credentials.hh"

namespace azure {

class default_credentials : public credentials {
public:
    enum class source : uint8_t {
        Env,
        AzureCli,
        Imds,
    };
    using source_set = enum_set<super_enum<source,
        source::Env,
        source::AzureCli,
        source::Imds>>;
    static constexpr source_set all_sources = source_set::full();

    default_credentials(const source_set& sources = all_sources,
            const sstring& truststore = "",
            const sstring& priority_string = "",
            const sstring& logctx = "");
private:
    static constexpr char NAME[] = "DefaultCredentials";
    source_set _sources;
    std::unique_ptr<credentials> _creds;
    // TLS options.
    sstring _truststore;
    sstring _priority_string;

    const char* get_name() const override { return NAME; };
    future<> refresh(const resource_type& resource_uri) override;
    future<> detect(const resource_type& resource_uri);

    using credentials_opt = std::optional<std::unique_ptr<credentials>>;
    future<credentials_opt> get_credentials_from_env(const resource_type& resource_uri);
    future<credentials_opt> get_credentials_from_azure_cli(const resource_type& resource_uri);
    future<credentials_opt> get_credentials_from_imds(const resource_type& resource_uri);
};

}