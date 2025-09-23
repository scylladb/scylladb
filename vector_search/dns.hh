/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sstring.hh>
#include "utils/log.hh"
#include <chrono>
#include <map>
#include <vector>
#include <functional>
#include <seastar/net/inet_address.hh>

namespace vector_search {

class dns {
public:
    using address_type = std::vector<seastar::net::inet_address>;
    using resolver_type = std::function<seastar::future<address_type>(seastar::sstring const&)>;
    using host_address_map = std::unordered_map<seastar::sstring, address_type>;
    using listener_type = std::function<seastar::future<>(host_address_map const&)>;

    explicit dns(logging::logger& logger, std::vector<seastar::sstring> hosts, listener_type listener);

    void start_background_tasks();

    void refresh_interval(std::chrono::milliseconds interval) {
        _refresh_interval = interval;
    }

    void hosts(std::vector<seastar::sstring> hosts) {
        _addresses.clear();
        _hosts = std::move(hosts);
        trigger_refresh();
    }

    void resolver(resolver_type r) {
        _resolver = std::move(r);
    }

    void trigger_refresh() {
        refresh_cv.signal();
    }

    seastar::future<> refresh_addr_task();

    seastar::future<> stop() {
        abort_refresh.request_abort();
        refresh_cv.signal();
        return tasks_gate.close();
    }

private:
    seastar::future<> refresh_addr();

    seastar::gate tasks_gate;
    logging::logger& vslogger;
    seastar::abort_source abort_refresh;
    seastar::lowres_clock::time_point last_refresh;
    std::chrono::milliseconds _refresh_interval;
    seastar::condition_variable refresh_cv;
    resolver_type _resolver;
    std::vector<seastar::sstring> _hosts;
    host_address_map _addresses;
    listener_type _listener;
};

} // namespace vector_search
