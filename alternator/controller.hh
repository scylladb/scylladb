/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include "alternator/server.hh"
#include "db/listener_config.hh"
#include "transport/protocol_server.hh"

namespace service {
class storage_proxy;
class storage_service;
class migration_manager;
class memory_limiter;
}

namespace db {
class system_distributed_keyspace;
class system_keyspace;
}

namespace cdc {
class generation_service;
}

namespace gms {

class gossiper;

}

namespace auth {
class service;
}

namespace qos {
class service_level_controller;
}

namespace vector_search {
class vector_store_client;
}

class updateable_timeout_config;

namespace alternator {

// This is the official DynamoDB API version.
// It represents the last major reorganization of that API, and all the features
// that were added since did NOT increment this version string.
constexpr const char* version = "2012-08-10";

using namespace seastar;

class executor;
class server;

class controller : public protocol_server {
    sharded<gms::gossiper>& _gossiper;
    sharded<service::storage_proxy>& _proxy;
    sharded<service::storage_service>& _ss;
    sharded<service::migration_manager>& _mm;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<db::system_keyspace>& _sys_ks;
    sharded<cdc::generation_service>& _cdc_gen_svc;
    sharded<service::memory_limiter>& _memory_limiter;
    sharded<auth::service>& _auth_service;
    sharded<qos::service_level_controller>& _sl_controller;
    sharded<vector_search::vector_store_client>& _vsc;
    sharded<updateable_timeout_config>& _timeout_config;
    const db::config& _config;
    db::listener_configs _listeners;

    std::vector<socket_address> _listen_addresses;
    sharded<executor> _executor;
    sharded<server> _server;
    // Whether _server has been started, and so whether there are listeners to reconcile
    // against rather than simply to remember.
    bool _serving = false;
    std::optional<smp_service_group> _ssg;

    std::unordered_map<sstring, sstring> listener_tls_options(const db::listener_config&) const;

    // A set of listeners worked out into what it takes to serve them.
    struct resolved_listeners {
        std::vector<server::listener> listeners;
        std::vector<seastar::tls::credentials_builder> credentials;
        // What each entry of `credentials` was built from, so that the server can tell
        // credentials it already holds from ones it has to build.
        std::vector<std::unordered_map<sstring, sstring>> tls_options;
    };
    resolved_listeners resolve_listeners(const db::listener_configs&) const;

public:
    controller(
        sharded<gms::gossiper>& gossiper,
        sharded<service::storage_proxy>& proxy,
        sharded<service::storage_service>& ss,
        sharded<service::migration_manager>& mm,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::system_keyspace>& sys_ks,
        sharded<cdc::generation_service>& cdc_gen_svc,
        sharded<service::memory_limiter>& memory_limiter,
        sharded<auth::service>& auth_service,
        sharded<qos::service_level_controller>& sl_controller,
        sharded<vector_search::vector_store_client>& vsc,
        sharded<updateable_timeout_config>& timeout_config,
        const db::config& config,
        db::listener_configs listeners,
        seastar::scheduling_group sg);

    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    // The listeners this controller was given, so that a change to them can be noticed.
    const db::listener_configs& listeners() const noexcept { return _listeners; }

    // Serve `listeners` from now on instead of whatever is being served, draining the
    // connections of any listener that goes away. See cql_transport::controller.
    future<> apply_listeners(db::listener_configs listeners);

    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
    // This virtual function is called (on each shard separately) when the
    // virtual table "system.clients" is read. It is expected to generate a
    // list of clients connected to this server (on this shard).
    virtual future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> get_client_data() override;
};

}
