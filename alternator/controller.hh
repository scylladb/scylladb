/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include "protocol_server.hh"

namespace service {
class storage_proxy;
class migration_manager;
class memory_limiter;
}

namespace db {
class system_distributed_keyspace;
class config;
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
    sharded<service::migration_manager>& _mm;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<cdc::generation_service>& _cdc_gen_svc;
    sharded<service::memory_limiter>& _memory_limiter;
    sharded<auth::service>& _auth_service;
    sharded<qos::service_level_controller>& _sl_controller;
    const db::config& _config;

    std::vector<socket_address> _listen_addresses;
    sharded<executor> _executor;
    sharded<server> _server;
    std::optional<smp_service_group> _ssg;

public:
    controller(
        sharded<gms::gossiper>& gossiper,
        sharded<service::storage_proxy>& proxy,
        sharded<service::migration_manager>& mm,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<cdc::generation_service>& cdc_gen_svc,
        sharded<service::memory_limiter>& memory_limiter,
        sharded<auth::service>& auth_service,
        sharded<qos::service_level_controller>& sl_controller,
        const db::config& config,
        seastar::scheduling_group sg);

    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
};

}
