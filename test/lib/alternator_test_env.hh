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
#include <seastar/core/future.hh>

#include "alternator/executor.hh"
#include "utils/rjson.hh"
#include "db/system_distributed_keyspace.hh"
#include "cdc/metadata.hh"

namespace service {
class storage_proxy;
class migration_manager;
class storage_service;
}
namespace cql3 {
class query_processor;
}

// Test environment for alternator frontend.
// The interface is minimal and does not cover alternator streams,
// because this environment has limited use as well - microbenchmarks.
// Regular unit tests should be performed by the test/alternator pytest
// suite, which has several advantages:
//  - it's not coded in raw C++, which is not great for unit tests
//  - it actually boots Scylla, which exercises more paths
//  - it's designed to run the tests against DynamoDB as well
// Because of that, the helper class above should only be used
// for benchmarks which need more low-level access to Seastar metrics.
class alternator_test_env {
    sharded<service::storage_proxy>& _proxy;
    sharded<service::migration_manager>& _mm;
    sharded<service::storage_service>& _storage_service;
    sharded<cql3::query_processor>& _qp;

    // Dummy service, only needed for alternator streams
    sharded<db::system_distributed_keyspace> _sdks;
    // Dummy service, only needed for alternator streams
    sharded<cdc::metadata> _cdc_metadata;

    sharded<alternator::executor> _executor;
public:
    alternator_test_env(sharded<service::storage_proxy>& proxy,
            sharded<service::migration_manager>& mm,
            sharded<service::storage_service>& ss,
            sharded<cql3::query_processor>& qp)
        : _proxy(proxy)
        , _mm(mm)
        , _storage_service(ss)
        , _qp(qp)
    {}

    future<> start(std::string_view isolation_level);
    future<> stop();
    future<> flush_memtables();

    alternator::executor& executor() {
        return _executor.local();
    }
};
