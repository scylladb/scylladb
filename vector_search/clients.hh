/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "client.hh"
#include "dns.hh"
#include "truststore.hh"
#include <seastar/core/future.hh>
#include "uri.hh"
#include "utils/sequential_producer.hh"
#include "vector_search/error.hh"
#include "utils/log.hh"
#include "utils/updateable_value.hh"
#include <expected>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/metrics_registration.hh>
#include <array>
#include <map>
#include <string_view>
#include <utility>
#include <memory>
#include <vector>

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

class clients {
public:
    using refresh_trigger_callback = std::function<void()>;

    using request_error = std::variant<aborted_error, addr_unavailable_error, service_unavailable_error>;
    using request_result = std::expected<client::response, request_error>;

    using clients_vec = std::vector<seastar::lw_shared_ptr<client>>;
    using get_clients_error = std::variant<aborted_error, addr_unavailable_error>;
    using get_clients_result = std::expected<clients_vec, get_clients_error>;

    /// `role` ("primary" or "secondary") labels the per-endpoint metrics of these clients.
    explicit clients(logging::logger& logger, seastar::sstring role, refresh_trigger_callback trigger_refresh,
            utils::updateable_value<uint32_t> unreachable_node_detection_time_in_ms, truststore& truststore);

    seastar::future<request_result> request(
            seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as);

    seastar::future<> handle_changed(const std::vector<uri>& uris, const dns::host_address_map& addrs);

    /// Keeps one address gauge for each configured URI: adds the series of
    /// new URIs and removes those of URIs, and their addresses, no longer configured.
    void set_endpoints(const std::vector<uri>& uris);

    seastar::future<> stop();

    void clear();

    seastar::future<get_clients_result> get_clients(seastar::abort_source& as);

    /// The currently resolved per-endpoint clients. Empty when DNS has not
    /// resolved any address yet. Does not trigger a refresh and never blocks.
    ///
    /// This is a reference to the live vector, which handle_changed() replaces
    /// on every DNS refresh: the caller must not preempt while iterating it.
    const clients_vec& current() const {
        return _clients;
    }

    void timeout(std::chrono::milliseconds timeout) {
        _timeout = timeout;
    }

private:
    /// The number of addresses resolved for one configured URI (host:port).
    struct endpoint_metrics {
        uint64_t addresses = 0;
        seastar::metrics::metric_groups metrics;
    };

    /// The outcome of each attempt to one resolved address of a configured URI.
    struct address_metrics {
        std::array<uint64_t, size_t(request_status::num_statuses)> requests = {};
        seastar::metrics::metric_groups metrics;
    };

    /// Identifies one address of a configured URI: host:port and the IP.
    using address_key = std::pair<seastar::sstring, seastar::sstring>;

    void register_endpoint_metrics(const seastar::sstring& endpoint, endpoint_metrics& m);
    void register_address_metrics(const address_key& key, address_metrics& m);
    void record_attempt(const client::endpoint_type& endpoint, request_status status);

    seastar::future<> close_clients();
    seastar::future<> close_old_clients();
    seastar::future<seastar::lw_shared_ptr<client>> make_client(const uri& uri_, const seastar::net::inet_address& addr_);

    clients_vec _clients;
    sequential_producer<clients_vec> _producer;
    refresh_trigger_callback _trigger_refresh;
    seastar::gate _gate;
    seastar::condition_variable _refresh_cv;
    std::chrono::milliseconds _timeout;
    clients_vec _old_clients;
    logging::logger& _logger;
    utils::updateable_value<uint32_t> _unreachable_node_detection_time_in_ms;
    truststore& _truststore;
    seastar::sstring _role;
    std::map<seastar::sstring, std::unique_ptr<endpoint_metrics>> _endpoint_metrics;
    std::map<address_key, std::unique_ptr<address_metrics>> _address_metrics;
};

} // namespace vector_search
