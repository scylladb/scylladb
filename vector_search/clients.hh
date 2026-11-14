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
#include <map>
#include <memory>
#include <vector>

namespace vector_search {

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

    /// Keeps one set of per-endpoint metrics for each configured URI: adds the
    /// series of new URIs and removes those of URIs no longer configured.
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
    /// Per-endpoint metrics, one set per configured URI (host:port).
    struct endpoint_metrics {
        uint64_t ok = 0;
        uint64_t unavailable = 0;
        uint64_t addresses = 0;
        seastar::metrics::metric_groups metrics;
    };

    void register_endpoint_metrics(const seastar::sstring& endpoint, endpoint_metrics& m);
    endpoint_metrics* find_endpoint_metrics(const client::endpoint_type& endpoint);

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
};

} // namespace vector_search
