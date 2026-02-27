/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/http/retry_strategy.hh>

namespace aws {

class aws_error;

class default_aws_retry_strategy : public seastar::http::experimental::retry_strategy {
protected:
    unsigned _max_retries;

public:
    explicit default_aws_retry_strategy(unsigned max_retries = 10);

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

} // namespace aws
