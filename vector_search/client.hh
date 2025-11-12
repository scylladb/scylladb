/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "error.hh"
#include "utils/log.hh"
#include "utils/updateable_value.hh"
#include <chrono>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/http/client.hh>
#include <seastar/http/common.hh>
#include <optional>
#include <expected>
#include <variant>

namespace vector_search {

class client {
public:
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

    explicit client(logging::logger& logger, endpoint_type endpoint_, utils::updateable_value<uint32_t> request_timeout_in_ms);

    seastar::future<request_result> request(
            seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as);

    seastar::future<> close();

    const endpoint_type& endpoint() const {
        return _endpoint;
    }

    bool is_up() const {
        return !is_checking_status_in_progress();
    }

private:
    seastar::future<response> request_impl(seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content,
            std::optional<seastar::http::reply::status_type>&& expected, seastar::abort_source& as);
    seastar::future<bool> check_status();
    void handle_server_unavailable();
    seastar::future<> run_checking_status();
    bool is_checking_status_in_progress() const;
    std::chrono::milliseconds backoff_retry_max() const;

    endpoint_type _endpoint;
    seastar::http::experimental::client _http_client;
    seastar::future<> _checking_status_future = seastar::make_ready_future();
    seastar::abort_source _as;
    logging::logger& _logger;
    utils::updateable_value<uint32_t> _request_timeout;
};

} // namespace vector_search
