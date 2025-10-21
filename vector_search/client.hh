/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "error.hh"
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/http/client.hh>
#include <seastar/http/common.hh>
#include <optional>
#include <expected>
#include <variant>
#include <seastar/core/gate.hh>

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

    using request_error = std::variant<aborted_error, service_unavailable_error>;
    using request_result = std::expected<response, request_error>;

    explicit client(endpoint_type endpoint_);

    seastar::future<request_result> request(
            seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as);

    seastar::future<> close();

    const endpoint_type& endpoint() const {
        return endpoint_;
    }

private:
    seastar::future<response> request_impl(seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content,
            std::optional<seastar::http::reply::status_type>&& expected, seastar::abort_source& as);

    seastar::future<bool> check_status();
    seastar::future<> handle_server_unavailable();
    seastar::future<> run_checking_status();
    bool is_checking_status_in_progress() const;

    endpoint_type endpoint_;
    seastar::http::experimental::client http_client_;
    seastar::gate gate_;
    seastar::abort_source as_;
};

} // namespace vector_search
