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

#include "controller.hh"

using namespace seastar;

namespace alternator {

controller::controller(sharded<service::storage_proxy>& proxy,
        sharded<service::migration_manager>& mm,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<cdc::generation_service>& cdc_gen_svc,
        sharded<cql3::query_processor>& qp,
        sharded<service::memory_limiter>& memory_limiter,
        const db::config& config)
    : _proxy(proxy)
    , _mm(mm)
    , _sys_dist_ks(sys_dist_ks)
    , _cdc_gen_svc(cdc_gen_svc)
    , _qp(qp)
    , _memory_limiter(memory_limiter)
    , _config(config)
{
    (void)_proxy;
    (void)_mm;
    (void)_sys_dist_ks;
    (void)_cdc_gen_svc;
    (void)_qp;
    (void)_memory_limiter;
    (void)_config;
}

}
