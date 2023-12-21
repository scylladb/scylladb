/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/distributed.hh>
#include <map>
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

future<> initialize_virtual_tables(
    distributed<replica::database>&,
    distributed<service::storage_service>&,
    sharded<gms::gossiper>&,
    sharded<service::raft_group_registry>&,
    sharded<db::system_keyspace>& sys_ks,
    db::config&);


class virtual_table;

using virtual_tables_registry_impl = std::map<table_id, std::unique_ptr<virtual_table>>;

// Pimpl to hide virtual_table from the rest of the code
class virtual_tables_registry : public std::unique_ptr<virtual_tables_registry_impl> {
public:
    virtual_tables_registry();
    ~virtual_tables_registry();
};

} // namespace db
