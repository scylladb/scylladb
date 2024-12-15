/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "aws_credentials_provider_chain.hh"
#include "utils/log.hh"
#include <seastar/core/coroutine.hh>

namespace aws {
static logging::logger cpc_logger("cred_provider_chain");

seastar::future<s3::aws_credentials> aws_credentials_provider_chain::get_aws_credentials() {
    for (const auto& provider : providers) {
        try {
            auto creds = co_await provider->get_aws_credentials();
            if (creds) {
                cpc_logger.info("AWS credentials were successfully retrieved by {}.", provider->get_name());
                co_return creds;
            }
            cpc_logger.debug("Retrieving AWS credentials by credentials provider {} failed.", provider->get_name());
        } catch (...) {
            cpc_logger.debug("Retrieving AWS credentials by credentials provider {} failed. Reason: {}", provider->get_name(), std::current_exception());
        }
    }
    cpc_logger.error("Failed to retrieve AWS credentials from any provider.");
    co_return s3::aws_credentials{};
}

void aws_credentials_provider_chain::invalidate_credentials() {
    for (const auto& provider : providers) {
        provider->invalidate_credentials();
    }
}

aws_credentials_provider_chain& aws_credentials_provider_chain::add_credentials_provider(std::unique_ptr<aws_credentials_provider>&& provider) {
    providers.emplace_back(std::move(provider));
    return *this;
}

} // namespace aws
