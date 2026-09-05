/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/core/abort_source.hh>
#include <seastar/http/retry_strategy.hh>

namespace aws {

class aws_error;

class default_aws_retry_strategy : public seastar::http::retry_strategy {
protected:
    unsigned _max_retries;
    seastar::abort_source* _as;

public:
    explicit default_aws_retry_strategy(unsigned max_retries = 10, seastar::abort_source* as = nullptr);

    unsigned max_retries() const { return _max_retries; }

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

} // namespace aws
