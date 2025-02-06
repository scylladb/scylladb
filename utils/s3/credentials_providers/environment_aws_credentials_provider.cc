/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "environment_aws_credentials_provider.hh"
#include <seastar/core/coroutine.hh>

namespace aws {
environment_aws_credentials_provider::environment_aws_credentials_provider() {
    creds = {
        .access_key_id = std::getenv("AWS_ACCESS_KEY_ID") ?: "",
        .secret_access_key = std::getenv("AWS_SECRET_ACCESS_KEY") ?: "",
        .session_token = std::getenv("AWS_SESSION_TOKEN") ?: "",
        .expires_at = seastar::lowres_clock::time_point::max(),
    };
}
seastar::future<> environment_aws_credentials_provider::reload() {
    if (!creds) [[unlikely]] {
        creds = {
            .access_key_id = std::getenv("AWS_ACCESS_KEY_ID") ?: "",
            .secret_access_key = std::getenv("AWS_SECRET_ACCESS_KEY") ?: "",
            .session_token = std::getenv("AWS_SESSION_TOKEN") ?: "",
            .expires_at = seastar::lowres_clock::time_point::max(),
        };
    }
    co_return;
}
} // namespace aws
