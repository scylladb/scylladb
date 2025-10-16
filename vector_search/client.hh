/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/http/client.hh>
#include <seastar/http/common.hh>
#include <exception>

namespace vector_search {

class client {

public:
    class is_down_exception : public std::exception {
    public:
        const char* what() const noexcept override {
            return "Client is down";
        }
    };

    struct response {
        seastar::http::reply::status_type status;
        std::vector<seastar::temporary_buffer<char>> content;
    };

    struct endpoint_type {
        seastar::sstring host;
        std::uint16_t port;
        seastar::net::inet_address ip;
    };

    explicit client(endpoint_type endpoint_);

    seastar::future<response> request(seastar::httpd::operation_type method, seastar::sstring path, seastar::sstring content, seastar::abort_source& as);

    seastar::future<> close();

    const endpoint_type& endpoint() const {
        return _endpoint;
    }

private:
    seastar::future<response> request_impl(seastar::httpd::operation_type method, seastar::sstring path, seastar::sstring content,
            std::optional<seastar::http::reply::status_type>&& expected, seastar::abort_source& as);

    seastar::future<> check_status();
    seastar::future<> handle_request_failed();
    seastar::future<> start_checking_status();
    // seastar::future<> stop_checking_status();
    bool is_checking_status_in_progress() const;
    seastar::future<> restart_pinging();
    seastar::future<> start_pinging();
    seastar::future<> stop_pinging();
    seastar::future<> ping();

    endpoint_type _endpoint;
    seastar::http::experimental::client _http_client;
    seastar::future<> _checking_status = seastar::make_ready_future<>();
    seastar::abort_source _as;
    // bool _is_up{true};
    // seastar::future<> _pinging = seastar::make_ready_future<>();
};

} // namespace vector_search
