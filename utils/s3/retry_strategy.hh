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

class retry_strategy : public seastar::http::experimental::retry_strategy<aws_error> {
protected:
    unsigned _max_retries;
    unsigned _scale_factor;

public:
    explicit retry_strategy(unsigned max_retries = 10, unsigned scale_factor = 25);

    seastar::future<bool> should_retry(const aws_error& error, unsigned attempted_retries) const override;

    [[nodiscard]] std::chrono::milliseconds delay_before_retry(const aws_error& error, unsigned attempted_retries) const override;

    [[nodiscard]] unsigned get_max_retries() const override { return _max_retries; }
};

} // namespace aws
