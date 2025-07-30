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
    unsigned _max_retries;
    unsigned _scale_factor;

public:
    explicit default_aws_retry_strategy(unsigned max_retries = 10, unsigned scale_factor = 25);

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
    seastar::future<seastar::input_stream<char>>
    analyze_reply(std::optional<seastar::http::reply::status_type> expected, const seastar::http::reply& rep, seastar::input_stream<char>&& in) const override;

    [[nodiscard]] unsigned get_max_retries() const noexcept override { return _max_retries; }
};

} // namespace aws
