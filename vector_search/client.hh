/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "error.hh"
#include "utils/log.hh"
#include "utils/updateable_value.hh"
#include <array>
#include <chrono>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/http/client.hh>
#include <seastar/http/common.hh>
#include <seastar/net/tls.hh>
#include <optional>
#include <string_view>
#include <expected>
#include <variant>

namespace vector_search {

/// Outcome of a request to the Vector Store, exported as the `status` label of the
/// requests and endpoint_requests metrics. Each value says who has to act.
enum class request_status {
    ok,          ///< 2xx: nobody.
    rejected,    ///< 400, 404 or 422, the query or the index is wrong: the user.
    not_ready,   ///< 503, the index is still building: nobody.
    unavailable, ///< No URI configured, no address resolved, or no endpoint answered: ops.
    error,       ///< Any other status (403, 5xx), a reply that cannot be parsed, or any other exception: Vector Store, or a bug.
    aborted,     ///< The request was aborted.
    num_statuses
};

std::string_view to_name(request_status status);

/// The status of an HTTP reply of the Vector Store.
request_status to_request_status(seastar::http::reply::status_type status);

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

    /// The number of attempts to the address of this client, by request_status.
    using attempt_counters = std::array<uint64_t, size_t(request_status::num_statuses)>;

    explicit client(logging::logger& logger, endpoint_type endpoint_, utils::updateable_value<uint32_t> unreachable_node_detection_time_in_ms,
            ::shared_ptr<seastar::tls::certificate_credentials> credentials);

    seastar::future<request_result> request(
            seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as);

    seastar::future<> close();

    const endpoint_type& endpoint() const {
        return _endpoint;
    }

    bool is_up() const {
        return !is_checking_status_in_progress();
    }

    /// Where record_attempt() counts. Set by `clients`, which exports the counters.
    void set_attempt_counters(seastar::lw_shared_ptr<attempt_counters> counters) {
        _attempts = std::move(counters);
    }

    void record_attempt(request_status status) {
        if (_attempts) {
            ++(*_attempts)[size_t(status)];
        }
    }

private:
    seastar::future<response> request_impl(seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content,
            std::optional<seastar::http::reply::status_type>&& expected, seastar::abort_source& as);
    seastar::future<bool> check_status();
    void handle_server_unavailable(const seastar::sstring& reason);
    seastar::future<> run_checking_status();
    bool is_checking_status_in_progress() const;
    std::chrono::milliseconds backoff_retry_max() const;

    endpoint_type _endpoint;
    seastar::http::client _http_client;
    seastar::future<> _checking_status_future = seastar::make_ready_future();
    seastar::abort_source _as;
    logging::logger& _logger;
    utils::updateable_value<uint32_t> _unreachable_node_detection_time_in_ms;
    seastar::lw_shared_ptr<attempt_counters> _attempts;
};

} // namespace vector_search
