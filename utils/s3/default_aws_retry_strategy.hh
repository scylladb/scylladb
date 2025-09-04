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

class default_aws_retry_strategy : public seastar::http::experimental::default_retry_strategy {
protected:
    [[nodiscard]] unsigned get_max_retries() const noexcept override { return _max_retries; }

    unsigned _max_retries;
    unsigned _scale_factor;

public:
    explicit default_aws_retry_strategy(unsigned max_retries = 10, unsigned scale_factor = 25);

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

} // namespace aws
