/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
                          const retry_strategy& retry_strategy);
    seastar::future<> make_request(seastar::http::request& req,
                                   seastar::http::experimental::client::reply_handler& handler,
                                   std::optional<seastar::http::reply::status_type> expected = std::nullopt,
                                   seastar::abort_source* as = nullptr);
    seastar::future<> make_request(seastar::http::request&& req,
                                   seastar::http::experimental::client::reply_handler&& handler,
                                   std::optional<seastar::http::reply::status_type> expected = std::nullopt,
                                   seastar::abort_source* as = nullptr);
    seastar::future<>
    make_request(seastar::http::request& req, std::optional<seastar::http::reply::status_type> expected = std::nullopt, seastar::abort_source* as = nullptr);
    seastar::future<> close();
    [[nodiscard]] const seastar::http::experimental::client& get_http_client() const { return http; };

private:
    seastar::future<>
    do_retryable_request(seastar::http::request& req, seastar::http::experimental::client::reply_handler handler, seastar::abort_source* as = nullptr);

    seastar::http::experimental::client http;
    const retry_strategy& _retry_strategy;
    error_handler _error_handler;
};

} // namespace aws
