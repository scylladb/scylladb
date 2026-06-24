/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/http/reply.hh>
#include <seastar/core/sleep.hh>
#include <system_error>

#include "utils/http_client_error_processing.hh"
#include "utils/exponential_backoff_retry.hh"

using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_http_error_classification_http_status_codes) {
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::service_unavailable), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::internal_server_error), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::too_many_requests), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::request_timeout), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::gateway_timeout), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::bandwidth_limit_exceeded), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::network_connect_timeout), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::network_read_timeout), utils::http::retryable::yes);

    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::unauthorized), utils::http::retryable::no);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::forbidden), utils::http::retryable::no);
    BOOST_REQUIRE_EQUAL(utils::http::from_http_code(seastar::http::reply::status_type::not_found), utils::http::retryable::no);

    co_return;
}

SEASTAR_TEST_CASE(test_http_error_classification_system_errors) {
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ECONNRESET, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ECONNREFUSED, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ENETUNREACH, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(EHOSTUNREACH, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ETIMEDOUT, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(EAGAIN, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ECONNABORTED, std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(static_cast<int>(std::errc::broken_pipe), std::system_category())), utils::http::retryable::yes);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ENETDOWN, std::system_category())), utils::http::retryable::yes);

    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(EACCES, std::system_category())), utils::http::retryable::no);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(EPERM, std::system_category())), utils::http::retryable::no);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(ENOENT, std::system_category())), utils::http::retryable::no);
    BOOST_REQUIRE_EQUAL(utils::http::from_system_error(std::system_error(EADDRINUSE, std::system_category())), utils::http::retryable::no);

    co_return;
}

SEASTAR_TEST_CASE(test_exponential_backoff_retry) {
    exponential_backoff_retry exr(10ms, 1000ms);

    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 10);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 20);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 40);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 80);

    co_await exr.retry();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 160);

    exr.reset();
    BOOST_REQUIRE_EQUAL(exr.sleep_time().count(), 10);
}
