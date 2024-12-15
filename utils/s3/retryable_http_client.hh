/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "retry_strategy.hh"
#include <seastar/http/client.hh>

namespace aws {

class retryable_http_client {
public:
    using error_handler = std::function<void(std::exception_ptr)>;

    retryable_http_client(std::unique_ptr<seastar::http::experimental::connection_factory>&& factory,
                          unsigned max_conn,
                          error_handler error_func,
                          seastar::http::experimental::client::retry_requests should_retry,
                          const aws::retry_strategy& retry_strategy);
    seastar::future<> make_request(seastar::http::request req,
                                   seastar::http::experimental::client::reply_handler handle,
                                   std::optional<seastar::http::reply::status_type> expected = std::nullopt,
                                   seastar::abort_source* = nullptr);
    seastar::future<> close();
    [[nodiscard]] const seastar::http::experimental::client& get_http_client() const { return http; };
    static void ignore_exception(std::exception_ptr) {}

private:
    seastar::future<>
    do_retryable_request(seastar::http::request req, seastar::http::experimental::client::reply_handler handler, seastar::abort_source* as = nullptr);

    seastar::http::experimental::client http;
    const aws::retry_strategy& _retry_strategy;
    error_handler _error_handler;
};

} // namespace aws
