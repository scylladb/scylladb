/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "client.hh"
#include "dns.hh"
#include "uri.hh"
#include "utils/sequential_producer.hh"
#include "vector_search/error.hh"
#include "utils/log.hh"
#include <expected>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/condition-variable.hh>
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

    explicit clients(logging::logger& logger, refresh_trigger_callback trigger_refresh);

    seastar::future<request_result> request(
            seastar::httpd::operation_type method, seastar::sstring path, std::optional<seastar::sstring> content, seastar::abort_source& as);

    seastar::future<> handle_changed(const std::vector<uri>& uris, const dns::host_address_map& addrs);

    seastar::future<> stop();

    void clear();

    seastar::future<get_clients_result> get_clients(seastar::abort_source& as);

    void timeout(std::chrono::milliseconds timeout) {
        _timeout = timeout;
    }

private:
    seastar::future<> close_clients();
    seastar::future<> close_old_clients();

    clients_vec _clients;
    sequential_producer<clients_vec> _producer;
    refresh_trigger_callback _trigger_refresh;
    seastar::gate _gate;
    seastar::condition_variable _refresh_cv;
    std::chrono::milliseconds _timeout;
    clients_vec _old_clients;
    logging::logger& _logger;
};

} // namespace vector_search
