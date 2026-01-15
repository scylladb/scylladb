/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "http_client_error_processing.hh"
#include <seastar/http/exception.hh>
#include <gnutls/gnutls.h>

namespace utils::http {

retryable from_http_code(seastar::http::reply::status_type http_code) {
    switch (http_code) {
    case seastar::http::reply::status_type::unauthorized:
    case seastar::http::reply::status_type::forbidden:
    case seastar::http::reply::status_type::not_found:
        return retryable::no;
    case seastar::http::reply::status_type::too_many_requests:
    case seastar::http::reply::status_type::internal_server_error:
    case seastar::http::reply::status_type::bandwidth_limit_exceeded:
    case seastar::http::reply::status_type::service_unavailable:
    case seastar::http::reply::status_type::request_timeout:
    case seastar::http::reply::status_type::page_expired:
    case seastar::http::reply::status_type::login_timeout:
    case seastar::http::reply::status_type::gateway_timeout:
    case seastar::http::reply::status_type::network_connect_timeout:
    case seastar::http::reply::status_type::network_read_timeout:
        return retryable::yes;
    default:
        return retryable{seastar::http::reply::classify_status(http_code) == seastar::http::reply::status_class::server_error};
    }
}

retryable from_system_error(const std::system_error& system_error) {
    switch (system_error.code().value()) {
    case static_cast<int>(std::errc::interrupted):
    case static_cast<int>(std::errc::resource_unavailable_try_again):
    case static_cast<int>(std::errc::timed_out):
    case static_cast<int>(std::errc::connection_aborted):
    case static_cast<int>(std::errc::connection_reset):
    case static_cast<int>(std::errc::connection_refused):
    case static_cast<int>(std::errc::broken_pipe):
    case static_cast<int>(std::errc::network_unreachable):
    case static_cast<int>(std::errc::host_unreachable):
    case static_cast<int>(std::errc::network_down):
    case static_cast<int>(std::errc::network_reset):
    case static_cast<int>(std::errc::no_buffer_space):
    // GNU TLS section. Since we pack gnutls error codes in std::system_error and rethrow it as std::nested_exception we have to handle them here.
    case GNUTLS_E_PREMATURE_TERMINATION:
    case GNUTLS_E_AGAIN:
    case GNUTLS_E_INTERRUPTED:
    case GNUTLS_E_PUSH_ERROR:
    case GNUTLS_E_PULL_ERROR:
    case GNUTLS_E_TIMEDOUT:
    case GNUTLS_E_SESSION_EOF:
    case GNUTLS_E_BAD_COOKIE: // as per RFC6347 section-4.2.1 client should retry
        return retryable::yes;
    default:
        return retryable::no;
    }
}

} // namespace utils::http
