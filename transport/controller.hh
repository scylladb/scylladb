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

using namespace seastar;

namespace cql_transport { class cql_server; }
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

namespace cql_transport {

class controller {
    std::unique_ptr<distributed<cql_transport::cql_server>> _server;
    semaphore _ops_sem; /* protects start/stop operations on _server */
    bool _stopped = false;

    sharded<auth::service>& _auth_service;
    sharded<service::migration_notifier>& _mnotifier;
    sharded<service::endpoint_lifecycle_notifier>& _lifecycle_notifier;
    gms::gossiper& _gossiper;
    sharded<cql3::query_processor>& _qp;
    sharded<service::memory_limiter>& _mem_limiter;
    sharded<qos::service_level_controller>& _sl_controller;
    const db::config& _config;

    future<> set_cql_ready(bool ready);
    future<> do_start_server();
    future<> do_stop_server();

    future<> subscribe_server(sharded<cql_server>& server);
    future<> unsubscribe_server(sharded<cql_server>& server);

public:
    controller(sharded<auth::service>&, sharded<service::migration_notifier>&, gms::gossiper&,
            sharded<cql3::query_processor>&, sharded<service::memory_limiter>&,
            sharded<qos::service_level_controller>&, sharded<service::endpoint_lifecycle_notifier>&,
            const db::config& cfg);
    future<> start_server();
    future<> stop_server();
    future<> stop();
    future<bool> is_server_running();
};

} // namespace cql_transport
