/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "utils/exponential_backoff_retry.hh"
#include <seastar/http/retry_strategy.hh>

namespace utils::gcp::storage {

// GCP object storage retry strategy
// General guidelines https://docs.cloud.google.com/storage/docs/retry-strategy

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
    static bool from_exception_ptr(std::exception_ptr exception);
};

} // namespace utils::gcp::storage
