/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "retryable_http_client.hh"
#include "aws_error.hh"
#include "client.hh"
#include "utils/error_injection.hh"

#include <seastar/core/sleep.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

namespace aws {

using namespace seastar;
retryable_http_client::retryable_http_client(std::unique_ptr<http::experimental::connection_factory>&& factory,
                                             unsigned max_conn,
                                             error_handler error_func,
                                             http::experimental::client::retry_requests should_retry,
                                             const aws::retry_strategy& retry_strategy)
    : http(std::move(factory), max_conn, should_retry), _retry_strategy(retry_strategy), _error_handler(std::move(error_func)) {
    assert(_error_handler);
}

future<> retryable_http_client::do_retryable_request(http::request req, http::experimental::client::reply_handler handler, seastar::abort_source* as) {
    // TODO: the http client does not check abort status on entry, and if
    // we're already aborted when we get here we will paradoxally not be
    // interrupted, because no registration etc will be done. So do a quick
    // preemptive check already.
    if (as && as->abort_requested()) {
        co_await coroutine::return_exception_ptr(as->abort_requested_exception_ptr());
    }
    uint32_t retries = 0;
    std::exception_ptr e;
    aws::aws_exception request_ex{aws::aws_error{aws::aws_error_type::OK, aws::retryable::yes}};
    while (true) {
        try {
            // We need to be able to simulate a retry in s3 tests
            if (utils::get_local_injector().enter("s3_client_fail_authorization")) {
                throw aws::aws_exception(aws::aws_error{aws::aws_error_type::HTTP_UNAUTHORIZED,
                    "EACCESS fault injected to simulate authorization failure", aws::retryable::no});
            }
            e = {};
            co_return co_await http.make_request(req, handler, std::nullopt, as);
        } catch (...) {
            e = std::current_exception();
            request_ex = aws_exception(aws_error::from_exception_ptr(e));
        }

        if (!co_await _retry_strategy.should_retry(request_ex.error(), retries)) {
            break;
        }
        co_await seastar::sleep(_retry_strategy.delay_before_retry(request_ex.error(), retries));
        ++retries;
    }

    if (e) {
        _error_handler(e);
    }
}

future<> retryable_http_client::make_request(http::request req,
                                             http::experimental::client::reply_handler handle,
                                             std::optional<http::reply::status_type> expected,
                                             seastar::abort_source* as) {
    co_await do_retryable_request(
        std::move(req),
        [handler = std::move(handle), expected](const http::reply& rep, input_stream<char>&& in) mutable -> future<> {
            auto payload = std::move(in);
            auto status_class = http::reply::classify_status(rep._status);

            if (status_class != http::reply::status_class::informational && status_class != http::reply::status_class::success) {
                std::optional<aws::aws_error> possible_error = aws::aws_error::parse(co_await util::read_entire_stream_contiguous(payload));
                if (possible_error) {
                    co_await coroutine::return_exception(aws::aws_exception(std::move(possible_error.value())));
                }
                co_await coroutine::return_exception(aws::aws_exception(aws::aws_error::from_http_code(rep._status)));
            }

            if (expected.has_value() && rep._status != *expected) {
                co_await coroutine::return_exception(httpd::unexpected_status_error(rep._status));
            }
            co_await handler(rep, std::move(payload));
        },
        as);
}

future<> retryable_http_client::close() {
    return http.close();
}

} // namespace aws
