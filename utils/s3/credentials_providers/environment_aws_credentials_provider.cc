/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "environment_aws_credentials_provider.hh"
#include <seastar/core/coroutine.hh>

namespace aws {
environment_aws_credentials_provider::environment_aws_credentials_provider() {
    creds = {
        .access_key_id = std::getenv("AWS_ACCESS_KEY_ID") ?: "",
        .secret_access_key = std::getenv("AWS_SECRET_ACCESS_KEY") ?: "",
        .session_token = std::getenv("AWS_SESSION_TOKEN") ?: "",
        .expires_at = std::chrono::system_clock::time_point::max(),
    };
}
seastar::future<s3::aws_credentials> environment_aws_credentials_provider::get_aws_credentials() {
    co_return creds;
}
} // namespace aws
