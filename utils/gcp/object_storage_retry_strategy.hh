/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "utils/exponential_backoff_retry.hh"
#include "utils/http_client_error_processing.hh"

#include <seastar/http/retry_strategy.hh>

namespace utils::gcp::storage {

class object_storage_retry_strategy : public seastar::http::experimental::retry_strategy {
protected:
    unsigned _max_retries;
    mutable exponential_backoff_retry _exr;
    abort_source* _as;

public:
    object_storage_retry_strategy(unsigned max_retries = 10,
                                  std::chrono::milliseconds base_sleep_time = std::chrono::milliseconds(10),
                                  std::chrono::milliseconds max_sleep_time = std::chrono::milliseconds(10000),
                                  abort_source* as = nullptr);

    seastar::future<bool> should_retry(std::exception_ptr error, unsigned attempted_retries) const override;
};

class object_storage_error : public std::exception {
    http::retryable _retryable;
    std::exception_ptr _underlying_error;

public:
    object_storage_error(http::retryable retryable, std::exception_ptr original_error);
    explicit object_storage_error(std::exception_ptr original_error);

    [[nodiscard]] bool is_retryable() const noexcept;
};

} // namespace utils::gcp::storage
