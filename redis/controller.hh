/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "protocol_server.hh"
#include "data_dictionary/data_dictionary.hh"

namespace db {
class config;
};

namespace redis {
class query_processor;
}

namespace redis_transport {
class redis_server;
}

namespace auth {
class service;
}

namespace service {
class storage_proxy;
class migration_manager;
}

namespace gms {
class gossiper;
}

namespace redis {

// As defined in: https://redis.io/topics/protocol
// "The RESP protocol was introduced in Redis 1.2, but it became the standard way
// for talking with the Redis server in Redis 2.0. This is the protocol you
// should implement in your Redis client."
// The protocol itself doesn't seem to have a version, but it was stabilized in
// Redis 2.0 according to the above quite so that is the version we are going to use.
constexpr const char* version = "2.0";

class controller : public protocol_server {
    seastar::sharded<redis::query_processor> _query_processor;
    seastar::shared_ptr<seastar::sharded<redis_transport::redis_server>> _server;
    seastar::sharded<service::storage_proxy>& _proxy;
    data_dictionary::database _db;
    seastar::sharded<auth::service>& _auth_service;
    seastar::sharded<service::migration_manager>& _mm;
    db::config& _cfg;
    seastar::sharded<gms::gossiper>& _gossiper;
    std::vector<socket_address> _listen_addresses;
private:
    seastar::future<> listen(seastar::sharded<auth::service>& auth_service, db::config& cfg);
public:
    controller(seastar::sharded<service::storage_proxy>& proxy, seastar::sharded<auth::service>& auth_service,
            seastar::sharded<service::migration_manager>& mm, db::config& cfg, seastar::sharded<gms::gossiper>& gossiper,
            seastar::scheduling_group sg);
    ~controller();
    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
};

}
