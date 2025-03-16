/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "retry_strategy.hh"

namespace aws {

class aws_error;

class s3_retry_strategy : public default_retry_strategy {
public:
    using credentials_refresher = std::function<seastar::future<>()>;
    s3_retry_strategy(credentials_refresher creds_refresher, unsigned max_retries = 10, unsigned scale_factor = 25);

    seastar::future<bool> should_retry(const aws_error& error, unsigned attempted_retries) const override;

private:
    credentials_refresher _creds_refresher;
};

} // namespace aws
