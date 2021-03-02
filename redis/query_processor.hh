/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>


using namespace seastar;

class database;
class service_permit;

namespace service {
class storage_proxy;
}

namespace redis {

class redis_options;
struct request;
struct reply;
class redis_message;

class query_processor {
    service::storage_proxy& _proxy;
    seastar::sharded<database>& _db;
    seastar::metrics::metric_groups _metrics;
    seastar::gate _pending_command_gate;
public:
    query_processor(service::storage_proxy& proxy, seastar::sharded<database>& db);

    ~query_processor();

    seastar::sharded<database>& db() {
        return _db;
    }

    service::storage_proxy& proxy() {
        return _proxy;
    }

    future<redis_message> process(request&&, redis_options&, service_permit);

    future<> start();
    future<> stop();
};

}
