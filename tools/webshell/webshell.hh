/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/units.hh>
#include <seastar/http/httpd.hh>
#include "cql3/query_processor.hh"
#include "timeout_config.hh"

namespace tools::webshell {

struct config {
    sstring cluster_name;
    scheduling_group scheduling_group;
    ::updateable_timeout_config timeout_config;
    db_clock::duration session_ttl = std::chrono::minutes(10);
};

class session_manager;

class server : public peering_sharded_service<server> {
    static constexpr size_t content_length_limit = 16*MB;

private:
    config _cfg;
    httpd::http_server _http_server;
    named_gate _pending_requests;
    std::unique_ptr<session_manager> _session_manager;

private:
    void set_routes(seastar::httpd::routes& r);

public:
    server(config cfg, cql3::query_processor& qp, auth::service& auth_service, qos::service_level_controller& sl_controller);
    ~server();

    future<> init(net::inet_address addr, uint16_t port);
    future<> stop();
};

} // namespace webshell
