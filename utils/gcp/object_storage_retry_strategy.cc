/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "object_storage_retry_strategy.hh"
#include "utils/exceptions.hh"
#include "utils/http_client_error_processing.hh"

#include <seastar/core/sleep.hh>
#include <seastar/http/exception.hh>

static logger rs_logger("gcp_retry_strategy");

namespace utils::gcp::storage {

object_storage_retry_strategy::object_storage_retry_strategy(unsigned max_retries,
                                                             std::chrono::milliseconds base_sleep_time,
                                                             std::chrono::milliseconds max_sleep_time,
                                                             abort_source* as)
    : _max_retries(max_retries), _exr(base_sleep_time, max_sleep_time), _as(as) {
}

future<bool> object_storage_retry_strategy::should_retry(std::exception_ptr error, unsigned attempted_retries) const {
    if (attempted_retries >= _max_retries) {
        rs_logger.warn("Retries exhausted. Retry# {}", attempted_retries);
        co_return false;
    }
    auto retryable = from_exception_ptr(error);
    if (retryable) {
        rs_logger.debug("GCP client request failed. Reason: {}. Retry# {}", error, attempted_retries);
        co_await (_as ? _exr.retry(*_as) : _exr.retry());
    } else {
        rs_logger.warn("GCP client encountered non-retryable error. Reason: {}. Retry# {}", error, attempted_retries);
    }
    co_return retryable;
}

bool object_storage_retry_strategy::from_exception_ptr(std::exception_ptr exception) {
    return dispatch_exception<bool>(
        std::move(exception),
        [](std::exception_ptr, std::string&&) { return false; },
        [](const seastar::httpd::unexpected_status_error& ex) {
            return http::from_http_code(ex.status()) == http::retryable::yes || ex.status() == seastar::http::reply::status_type::unauthorized;
        },
        [](const std::system_error& ex) { return http::from_system_error(ex) == http::retryable::yes; });
}

} // namespace utils::gcp::storage
