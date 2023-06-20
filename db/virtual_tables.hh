/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/distributed.hh>
#include "schema/schema_fwd.hh"

namespace replica {
class database;
}

namespace service {
class storage_service;
class raft_group_registry;
}

namespace gms {
class gossiper;
}

namespace db {

class config;
class system_keyspace;

void register_virtual_tables(distributed<replica::database>& dist_db, distributed<service::storage_service>& dist_ss,
        sharded<gms::gossiper>& dist_gossiper, sharded<service::raft_group_registry>& dist_raft_gr, db::config& cfg);
void install_virtual_readers(db::system_keyspace& sys_ks, replica::database& db);
std::vector<schema_ptr> all_virtual_tables();

} // namespace db
