/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include "service/memory_limiter.hh"
#include "protocol_server.hh"

using namespace seastar;

class thrift_server;

namespace replica {
class database;
}

namespace auth { class service; }
namespace cql3 { class query_processor; }
namespace service {
class storage_service;
class storage_proxy;
}

class thrift_controller : public protocol_server {
    std::unique_ptr<distributed<thrift_server>> _server;
    std::optional<socket_address> _addr;
    semaphore _ops_sem; /* protects start/stop operations on _server */
    bool _stopped = false;

    distributed<replica::database>& _db;
    sharded<auth::service>& _auth_service;
    sharded<cql3::query_processor>& _qp;
    sharded<service::memory_limiter>& _mem_limiter;
    sharded<service::storage_service>& _ss;
    sharded<service::storage_proxy>& _proxy;

    future<> do_start_server();
    future<> do_stop_server();

public:
    thrift_controller(distributed<replica::database>&, sharded<auth::service>&, sharded<cql3::query_processor>&, sharded<service::memory_limiter>&, sharded<service::storage_service>& ss, sharded<service::storage_proxy>& proxy);
    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
};
