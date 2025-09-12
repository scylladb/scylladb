/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>

#include "protocol_server.hh"


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

class server;

class controller : public protocol_server {
    sharded<cql3::query_processor>& _qp;
    sharded<auth::service>& _auth_service;
    sharded<qos::service_level_controller>& _sl_controller;
    const db::config& _config;
    sstring _cluster_name;

    std::vector<socket_address> _listen_addresses;
    sharded<server> _server;

public:
    controller(sharded<cql3::query_processor>& qp, sharded<auth::service>& auth_service, sharded<qos::service_level_controller>& sl_controller,
        const db::config& config, sstring cluster_name, seastar::scheduling_group sg);

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
