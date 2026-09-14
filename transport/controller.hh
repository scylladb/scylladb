/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

#include <unordered_map>

#include "protocol_server.hh"
#include "db/listener_config.hh"
#include "transport/cql_listener_config.hh"
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
namespace netw { class messaging_service; }
namespace db { class config; }
struct client_data;
class updateable_timeout_config;

namespace cql_transport {

class cql_server;
struct connection_service_level_params;
class controller : public protocol_server {
    // Everything needed to open one listening socket, worked out from a listener's
    // configuration: its address resolved, its credentials built, and the shard-aware
    // sibling it advertises looked up.
    struct listen_cfg {
        sstring name;
        socket_address addr;
        cql_listener_config lcfg;
        bool is_shard_aware;
        std::shared_ptr<seastar::tls::credentials_builder> cred;
        // The options `cred` was built out of. Kept so that two resolutions of the same
        // listener compare equal: resolving builds a fresh credentials_builder every
        // time, so comparing the pointers would call every encrypted listener changed,
        // and tear down its connections, whenever anything at all was reconfigured.
        std::unordered_map<sstring, sstring> tls_options;
        bool proxy_protocol;
        bool keepalive;

        // Whether the two would open the same socket. Everything but `lcfg` is part of
        // the socket; `lcfg` is only what its connections are told, and can be changed
        // under a socket that stays open.
        bool same_socket(const listen_cfg& other) const {
            return name == other.name && addr == other.addr
                    && is_shard_aware == other.is_shard_aware
                    && bool(cred) == bool(other.cred) && tls_options == other.tls_options
                    && proxy_protocol == other.proxy_protocol && keepalive == other.keepalive;
        }
    };

    db::listener_configs _listeners;
    // What is actually being served, by name, as it was resolved when it was opened.
    // apply_listeners() compares against this rather than against _listeners, so that a
    // listener is reopened when anything its socket was opened with changed.
    std::unordered_map<sstring, listen_cfg> _served;
    std::vector<socket_address> _listen_addresses;
    std::unique_ptr<sharded<cql_server>> _server;
    semaphore _ops_sem; /* protects start/stop operations on _server */
    named_gate _bg_stops;
    bool _stopped = false;
    // Whether the server is meant to run: start_server() was called, and
    // request_stop_server() hasn't been since. It runs only while it also has
    // listeners, so a server that is wanted but has none starts with the first
    // listener apply_listeners() gives it, and stops when it takes the last away.
    bool _wanted = false;
    // Whether the server was started before. Only the first start, with the node, is
    // strict about every listener opening; a later one - the server stopped and started
    // again - is best effort, like a change to the listeners, which may have left
    // listeners in the set that could not be opened.
    bool _started_before = false;

    sharded<auth::service>& _auth_service;
    sharded<service::migration_notifier>& _mnotifier;
    sharded<service::endpoint_lifecycle_notifier>& _lifecycle_notifier;
    sharded<gms::gossiper>& _gossiper;
    sharded<cql3::query_processor>& _qp;
    sharded<service::memory_limiter>& _mem_limiter;
    sharded<qos::service_level_controller>& _sl_controller;
    sharded<netw::messaging_service>& _messaging;
    sharded<updateable_timeout_config>& _timeout_config;
    const db::config& _config;
    scheduling_group_key _cql_opcode_stats_key;


    future<> set_cql_ready(bool ready);
    // With best_effort, a listener that cannot be opened is reported and left out, and
    // the server starts with the others, failing only if there are none. Without it,
    // any listener that cannot be opened fails the start.
    future<> do_start_server(bool best_effort = false);
    future<> do_stop_server();

    future<> subscribe_server(sharded<cql_server>& server);
    future<> unsubscribe_server(sharded<cql_server>& server);

    cql_listener_config shard_aware_sibling(const sstring& name, const db::listener_config&,
            const db::listener_configs& listeners,
            const std::unordered_map<sstring, net::inet_address>& resolved_addresses) const;
    // Resolve a set of listeners into what it takes to serve them. Does everything that
    // can fail - name resolution, reading certificates, checking that a shard-aware
    // sibling exists and is reachable - so that a set which cannot be served is rejected
    // before any socket is touched.
    future<std::vector<listen_cfg>> resolve_listeners(const db::listener_configs& listeners) const;
    future<> start_listening_on_tcp_sockets(sharded<cql_server>& cserver, bool best_effort);
    future<bool> try_listen(sharded<cql_server>& cserver, const listen_cfg& cfg);
    void update_listen_addresses();
public:
    // Serve `listeners` from now on instead of whatever is being served: open the ones
    // that are new, stop the ones that are gone, and reopen the ones whose socket
    // changed. Connections on a listener that goes away are drained, so a client keeps
    // the answer to the request it had in flight; connections on a listener that stays
    // are left alone, even when the shard-aware listener it advertises changes - that
    // is updated in place.
    //
    // The whole set is resolved before anything is touched, so a set that cannot be
    // served - a name that does not resolve, a certificate that cannot be read, a
    // shard-aware sibling that is missing - is rejected with the server still serving
    // what it was.
    future<> apply_listeners(db::listener_configs listeners);
private:
    future<> start_listening_on_maintenance_socket(sharded<cql_server>& cserver);

    maintenance_socket_enabled _used_by_maintenance_socket;

public:
    controller(sharded<auth::service>&, sharded<service::migration_notifier>&, sharded<gms::gossiper>&,
            sharded<cql3::query_processor>&, sharded<service::memory_limiter>&,
            sharded<qos::service_level_controller>&, sharded<service::endpoint_lifecycle_notifier>&,
            sharded<netw::messaging_service>&, sharded<updateable_timeout_config>& timeout_config,
            const db::config& cfg, db::listener_configs listeners, scheduling_group_key cql_opcode_stats_key, maintenance_socket_enabled used_by_maintenance_socket,
            seastar::scheduling_group sg);
    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
    virtual future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> get_client_data() override;
    future<> update_connections_scheduling_group();

    future<std::vector<connection_service_level_params>> get_connections_service_level_params();
};

} // namespace cql_transport
