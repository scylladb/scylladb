/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>

#include "db/config.hh"
#include "protocol_server.hh"
#include "service/maintenance_mode.hh"

using namespace seastar;

namespace auth { class service; }
namespace service {
    class migration_notifier;
    class endpoint_lifecycle_notifier;
    class memory_limiter;
}
namespace gms { class gossiper; }
namespace cql3 { class query_processor; }
namespace qos { class service_level_controller; }
namespace db { class config; }
struct client_data;

namespace cql_transport {

class cql_server;
struct connection_service_level_params;
class controller : public protocol_server {
    std::vector<socket_address> _listen_addresses;
    std::unique_ptr<sharded<cql_server>> _server;
    semaphore _ops_sem; /* protects start/stop operations on _server */
    gate _bg_stops;
    bool _stopped = false;

    sharded<auth::service>& _auth_service;
    sharded<service::migration_notifier>& _mnotifier;
    sharded<service::endpoint_lifecycle_notifier>& _lifecycle_notifier;
    sharded<gms::gossiper>& _gossiper;
    sharded<cql3::query_processor>& _qp;
    sharded<service::memory_limiter>& _mem_limiter;
    sharded<qos::service_level_controller>& _sl_controller;
    const db::config& _config;
    scheduling_group_key _cql_opcode_stats_key;


    future<> set_cql_ready(bool ready);
    future<> do_start_server();
    future<> do_stop_server();

    future<> subscribe_server(sharded<cql_server>& server);
    future<> unsubscribe_server(sharded<cql_server>& server);

    future<> start_listening_on_tcp_sockets(sharded<cql_server>& cserver);
    future<> start_listening_on_maintenance_socket(sharded<cql_server>& cserver);

    maintenance_socket_enabled _used_by_maintenance_socket;

public:
    controller(sharded<auth::service>&, sharded<service::migration_notifier>&, sharded<gms::gossiper>&,
            sharded<cql3::query_processor>&, sharded<service::memory_limiter>&,
            sharded<qos::service_level_controller>&, sharded<service::endpoint_lifecycle_notifier>&,
            const db::config& cfg, scheduling_group_key cql_opcode_stats_key, maintenance_socket_enabled used_by_maintenance_socket,
            seastar::scheduling_group sg);
    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
    virtual future<utils::chunked_vector<client_data>> get_client_data() override;

    future<std::vector<connection_service_level_params>> get_connections_service_level_params();
};

} // namespace cql_transport
