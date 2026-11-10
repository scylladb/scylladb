/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

#include "db/listener_config.hh"
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

namespace gms {
class gossiper;
}

class updateable_timeout_config;

namespace tools::webshell {

class server;

/// Controls the Web Shell servers of a node: one HTTP server per shard, serving
/// the "protocol: webshell" listeners of the "listeners" option.
///
/// The set of listeners can be changed at runtime, see apply_listeners().
/// The server runs only while it is wanted (start_server() was called) and has
/// listeners to serve.
class controller : public protocol_server {
    sharded<cql3::query_processor>& _qp;
    sharded<auth::service>& _auth_service;
    sharded<qos::service_level_controller>& _sl_controller;
    sharded<updateable_timeout_config>& _timeout_config;
    sharded<gms::gossiper>& _gossiper;
    const db::config& _config;
    db::listener_configs _listeners;

    std::vector<socket_address> _listen_addresses;
    sharded<server> _server;
    // Whether _server is started, and so whether there are listeners to
    // reconcile against, rather than simply to remember.
    bool _serving = false;
    // Whether the server is meant to run: start_server() was called, and no
    // stop since.
    bool _wanted = false;
    // Serializes starting, stopping and reconfiguring the server.
    semaphore _ops_sem{1};

    future<> do_start_server();
    future<> do_stop_server();
    // Have every shard serve `listeners`, as far as it can. Returns the
    // addresses that could not be listened on.
    future<std::vector<socket_address>> reconcile(const db::listener_configs& listeners);

public:
    controller(sharded<cql3::query_processor>& qp, sharded<auth::service>& auth_service,
            sharded<qos::service_level_controller>& sl_controller, sharded<updateable_timeout_config>& timeout_config,
            sharded<gms::gossiper>& gossiper, const db::config& config, db::listener_configs listeners, scheduling_group sg);

    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
    // Called (on each shard separately) when system.clients is read: the
    // requests of this shard, waiting or being processed.
    virtual future<::utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> get_client_data() override;

    /// Serve `listeners` from now on, instead of whatever is being served.
    /// Listeners which didn't change are left alone, the ones that are gone
    /// are stopped, the new ones are opened. A listener that cannot be opened
    /// is reported and left out.
    future<> apply_listeners(db::listener_configs listeners);
};

} // namespace tools::webshell
