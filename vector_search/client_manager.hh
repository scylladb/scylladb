/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "node.hh"
#include "dns.hh"
#include <chrono>
#include <vector>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/abort_source.hh>
#include "utils/sequential_producer.hh"

namespace vector_search {

class client_manager {
public:
    struct uri {
        seastar::sstring host;
        std::uint16_t port;
    };
    using clients_type = std::vector<seastar::lw_shared_ptr<node>>;

    client_manager(std::vector<uri> primary, std::vector<uri> secondary, logging::logger& logger, uint64_t& refreshes_counter);

    void update_uris(std::vector<uri> primary, std::vector<uri> secondary);
    /// Timeout for waiting for a new client to be available
    void timeout(std::chrono::milliseconds timeout) {
        _timeout = timeout;
    }

    auto get_primary_clients(seastar::abort_source& as) -> seastar::future<clients_type>;
    auto get_secondary_clients(seastar::abort_source& as) -> seastar::future<clients_type>;

    void start_background_tasks();
    auto stop() -> seastar::future<>;

    bool has_primay() const;
    bool has_secondary() const;
    bool empty() const;

    void trigger_refresh();
    void clear();

    dns& get_dns() {
        return _dns;
    }

private:
    auto get_or_refresh_if_empty(const clients_type& clients, seastar::abort_source& as) -> seastar::future<clients_type>;
    auto cleanup_clients() -> seastar::future<>;
    auto cleanup_old_clients() -> seastar::future<>;
    auto handle_addresses_changed(const dns::host_address_map& addrs) -> seastar::future<>;

    clients_type _primary_clients;
    clients_type _secondary_clients;
    clients_type _old_clients;
    std::vector<uri> _primary_uris;
    std::vector<uri> _secondary_uris;
    seastar::gate _client_producer_gate;
    seastar::condition_variable _refresh_client_cv;
    std::chrono::milliseconds _timeout = std::chrono::seconds(5);
    sequential_producer<void> _refresh_clients;
    dns _dns;
};

} // namespace vector_search
