/*
 * Copyright (C) 2021-present ScyllaDB
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
#include <seastar/core/smp.hh>

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

namespace cql3 {
class query_processor;
}

namespace alternator {

using namespace seastar;

class executor;
class server;

class controller {
    sharded<service::storage_proxy>& _proxy;
    sharded<service::migration_manager>& _mm;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<cdc::generation_service>& _cdc_gen_svc;
    sharded<cql3::query_processor>& _qp;
    sharded<service::memory_limiter>& _memory_limiter;
    const db::config& _config;

    sharded<executor> _executor;
    sharded<server> _server;
    std::optional<smp_service_group> _ssg;

public:
    controller(sharded<service::storage_proxy>& proxy,
        sharded<service::migration_manager>& mm,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<cdc::generation_service>& cdc_gen_svc,
        sharded<cql3::query_processor>& qp,
        sharded<service::memory_limiter>& memory_limiter,
        const db::config& config);

    future<> start();
    future<> stop();
};

}
