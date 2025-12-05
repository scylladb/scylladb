/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "schema/schema_fwd.hh"

namespace service {
class raft_group_registry;
}

namespace cql3 {
class query_processor;
}

namespace db {

class system_keyspace;

// Initialize virtual tables in the system0 keyspace which mirror group0 tables
// from the system keyspace but allow writes via group0.
future<> initialize_system0_virtual_tables(
    sharded<service::raft_group_registry>& dist_raft_gr,
    sharded<db::system_keyspace>& sys_ks,
    sharded<cql3::query_processor>& qp);

} // namespace db
