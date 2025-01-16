/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "retryable_http_client.hh"
#include "aws_error.hh"
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
                                             const retry_strategy& retry_strategy)
    : http(std::move(factory), max_conn, should_retry), _retry_strategy(retry_strategy), _error_handler(std::move(error_func)) {
}

future<> retryable_http_client::make_request(http::request& req,
                                             http::experimental::client::reply_handler& handler,
                                             std::optional<http::reply::status_type> expected,
                                             abort_source* as) {
    co_await do_retryable_request(
        req,
        [&handler, expected = expected.value_or(http::reply::status_type::ok)](const http::reply& rep, input_stream<char>&& in) -> future<> {
            auto payload = std::move(in);
            auto status_class = http::reply::classify_status(rep._status);

            if (status_class != http::reply::status_class::informational && status_class != http::reply::status_class::success) {
                std::optional<aws_error> possible_error = aws_error::parse(co_await util::read_entire_stream_contiguous(payload));
                if (possible_error) {
                    co_await coroutine::return_exception(aws_exception(std::move(possible_error.value())));
                }
                co_await coroutine::return_exception(aws_exception(aws_error::from_http_code(rep._status)));
            }

            if (rep._status != expected) {
                co_await coroutine::return_exception(httpd::unexpected_status_error(rep._status));
            }
            co_await handler(rep, std::move(payload));
        },
        as);
}
future<> retryable_http_client::make_request(http::request&& req,
                                             http::experimental::client::reply_handler&& handler,
                                             std::optional<http::reply::status_type> expected,
                                             abort_source* as) {
    auto request = std::move(req);
    auto reply_handler = std::move(handler);
    co_await make_request(request, reply_handler, expected, as);
}

future<> retryable_http_client::make_request(http::request& req, std::optional<http::reply::status_type> expected, abort_source* as) {
    co_await make_request(
        std::move(req),
        [](const http::reply& rep, input_stream<char>&& in_) -> future<> {
            auto in = std::move(in_);
            co_await util::skip_entire_stream(in);
        },
        expected,
        as);
}

future<> retryable_http_client::close() {
    return http.close();
}

future<> retryable_http_client::do_retryable_request(http::request& req, http::experimental::client::reply_handler handler, abort_source* as) {
    // TODO: the http client does not check abort status on entry, and if
    // we're already aborted when we get here we will paradoxally not be
    // interrupted, because no registration etc will be done. So do a quick
    // preemptive check already.
    if (as && as->abort_requested()) {
        co_await coroutine::return_exception_ptr(as->abort_requested_exception_ptr());
    }
    uint32_t retries = 0;
    std::exception_ptr e;
    aws_exception request_ex{aws_error{aws_error_type::OK, retryable::yes}};
    while (true) {
        try {
            e = {};
            co_return co_await (as ? http.make_request(req, handler, *as, std::nullopt) : http.make_request(req, handler, std::nullopt));
        } catch (const aws_exception& ex) {
            e = std::current_exception();
            request_ex = ex;
        } catch (const std::system_error& ex) {
            e = std::current_exception();
            request_ex = aws_exception(aws_error::from_system_error(ex));
        } catch (...) {
            e = std::current_exception();
            request_ex = aws_exception(aws_error{aws_error_type::UNKNOWN, format("{}", e), retryable::no});
        }

        if (!_retry_strategy.should_retry(request_ex.error(), retries)) {
            break;
        }
        co_await sleep(_retry_strategy.delay_before_retry(request_ex.error(), retries));
        ++retries;
    }

    if (e) {
        _error_handler(e);
    }
}


} // namespace aws
