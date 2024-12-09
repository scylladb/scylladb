/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "aws_credentials_provider_chain.hh"
#include <seastar/core/coroutine.hh>
#include <utils/log.hh>

namespace aws {
static logging::logger cpc_logger("cred_provider_chain");

seastar::future<s3::endpoint_config::aws_credentials> aws_credentials_provider_chain::get_aws_credentials() {
    for (const auto& provider : providers) {
        try {
            auto creds = co_await provider->get_aws_credentials();
            if (creds) {
                co_return creds;
            }
        } catch (...) {
            cpc_logger.warn("Retrieving AWS credentials by credentials provider {} failed. Reason: {}", provider->get_name(), std::current_exception());
        }
    }
    co_return s3::endpoint_config::aws_credentials{};
}

aws_credentials_provider_chain& aws_credentials_provider_chain::add_credentials_provider(std::unique_ptr<aws_credentials_provider>&& provider) {
    providers.emplace_back(std::move(provider));
    return *this;
}


} // namespace aws
