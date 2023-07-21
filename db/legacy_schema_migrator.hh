/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "seastarx.hh"

namespace replica {
class database;
}

namespace cql3 {
class query_processor;
}

namespace service {
class storage_proxy;
}

namespace db {
class system_keyspace;

namespace legacy_schema_migrator {

future<> migrate(sharded<service::storage_proxy>&, sharded<replica::database>& db, sharded<db::system_keyspace>& sys_ks, cql3::query_processor&);

}
}
