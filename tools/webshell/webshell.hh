/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include <seastar/core/sharded.hh>

#include "db_clock.hh"
#include "timeout_config.hh"
#include "transport/protocol_server.hh"

namespace cql3 {
class query_processor;
}

namespace auth {
class service;
}

namespace qos {
class service_level_controller;
}

namespace db {
class config;
}

namespace tools::webshell {

struct config {
    sstring cluster_name;
    scheduling_group scheduling_group;

    bool listen_interface_prefer_ipv6;
    bool enable_ipv6_dns_lookup;
    ::updateable_timeout_config timeout_config;

    sstring webshell_http_address;
    uint16_t webshell_http_port;
    sstring webshell_https_address;
    uint16_t webshell_https_port;
    std::unordered_map<sstring, sstring> webshell_https_encryption_options;
    std::filesystem::path webshell_resource_manifest_path;

    db_clock::duration session_ttl = std::chrono::minutes(10);
    uint64_t max_sessions = 32;
    uint64_t max_concurrent_requests = 16;
    uint64_t max_waiting_requests = 16;
};

config make_config(const db::config& db_cfg, std::string_view cluster_name, scheduling_group sg);

class server;

class controller : public protocol_server {
    sharded<cql3::query_processor>& _qp;
    sharded<auth::service>& _auth_service;
    sharded<qos::service_level_controller>& _sl_controller;
    config _config;

    std::vector<socket_address> _listen_addresses;
    sharded<server> _server;

public:
    controller(sharded<cql3::query_processor>& qp, sharded<auth::service>& auth_service, sharded<qos::service_level_controller>& sl_controller, config cfg);

    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
    virtual future<::utils::chunked_vector<client_data>> get_client_data() override;
};

} // namespace tools::webshell
