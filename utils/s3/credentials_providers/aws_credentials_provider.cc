/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "aws_credentials_provider.hh"
#include <seastar/core/coroutine.hh>

namespace aws {

seastar::future<s3::aws_credentials> aws_credentials_provider::get_aws_credentials() {
    co_await reload();
    co_return creds;
}

void aws_credentials_provider::invalidate_credentials() {
    creds = {};
}

} // namespace aws
