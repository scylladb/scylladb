/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "retryable_http_client.hh"
#include "aws_error.hh"
#include <seastar/core/sleep.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>
#include <utils/exceptions.hh>

using namespace seastar;
namespace aws {

// EWZ TODO: This function just got moved from client.cc to make this version of code to work properly, later on it will moved back where it belongs
storage_io_error map_s3_client_exception(std::exception_ptr ex) {
    seastar::memory::scoped_critical_alloc_section alloc;

    try {
        std::rethrow_exception(std::move(ex));
    } catch (const aws::aws_exception& e) {
        int error_code;
        switch (e.error().get_error_type()) {
        case aws::aws_error_type::HTTP_NOT_FOUND:
        case aws::aws_error_type::RESOURCE_NOT_FOUND:
        case aws::aws_error_type::NO_SUCH_BUCKET:
        case aws::aws_error_type::NO_SUCH_KEY:
        case aws::aws_error_type::NO_SUCH_UPLOAD:
            error_code = ENOENT;
            break;
        case aws::aws_error_type::HTTP_FORBIDDEN:
        case aws::aws_error_type::HTTP_UNAUTHORIZED:
        case aws::aws_error_type::ACCESS_DENIED:
            error_code = EACCES;
            break;
        default:
            error_code = EIO;
        }
        return {error_code, format("S3 request failed. Code: {}. Reason: {}", e.error().get_error_type(), e.what())};
    } catch (const httpd::unexpected_status_error& e) {
        auto status = e.status();

        if (http::reply::classify_status(status) == http::reply::status_class::redirection || status == http::reply::status_type::not_found) {
            return {ENOENT, format("S3 object doesn't exist ({})", status)};
        }
        if (status == http::reply::status_type::forbidden || status == http::reply::status_type::unauthorized) {
            return {EACCES, format("S3 access denied ({})", status)};
        }

        return {EIO, format("S3 request failed with ({})", status)};
    } catch (...) {
        auto e = std::current_exception();
        return {EIO, format("S3 error ({})", e)};
    }
}

retryable_http_client::retryable_http_client(std::unique_ptr<http::experimental::connection_factory>&& factory,
                                             unsigned max_conn,
                                             http::experimental::client::retry_requests should_retry,
                                             const aws::retry_strategy& retry_strategy)
    : http(std::move(factory), max_conn, should_retry), _retry_strategy(retry_strategy) {
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
            e = {};
            co_return co_await (as ? http.make_request(req, handler, *as, std::nullopt) : http.make_request(req, handler, std::nullopt));
        } catch (const aws::aws_exception& ex) {
            e = std::current_exception();
            request_ex = ex;
        }

        if (!_retry_strategy.should_retry(request_ex.error(), retries)) {
            break;
        }
        co_await seastar::sleep(_retry_strategy.delay_before_retry(request_ex.error(), retries));
        ++retries;
    }

    if (e) {
        throw map_s3_client_exception(e);
    }
}

future<> retryable_http_client::make_request(http::request req,
                                             http::experimental::client::reply_handler handle,
                                             std::optional<http::reply::status_type> expected,
                                             seastar::abort_source* as) {
    co_await do_retryable_request(
        std::move(req),
        [handler = std::move(handle), expected = expected.value_or(http::reply::status_type::ok)](const http::reply& rep,
                                                                                                  input_stream<char>&& in) mutable -> future<> {
            auto payload = std::move(in);
            auto status_class = http::reply::classify_status(rep._status);

            if (status_class != http::reply::status_class::informational && status_class != http::reply::status_class::success) {
                std::optional<aws::aws_error> possible_error = aws::aws_error::parse(co_await util::read_entire_stream_contiguous(payload));
                if (possible_error) {
                    co_await coroutine::return_exception(aws::aws_exception(std::move(possible_error.value())));
                }
                co_await coroutine::return_exception(aws::aws_exception(aws::aws_error::from_http_code(rep._status)));
            }

            if (rep._status != expected) {
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
