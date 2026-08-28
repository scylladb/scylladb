/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "service/raft/raft_state_machine.hh"
#include "mutation/frozen_mutation.hh"
#include <functional>
#include <unordered_map>
#include "locator/tablets.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "utils/loading_cache.hh"

namespace db {
class system_keyspace;
}

namespace service {
class migration_manager;
}

namespace service::strong_consistency {

struct raft_command {
    frozen_mutation mutation;
};

std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    replica::database& db,
    service::migration_manager& mm,
    db::system_keyspace& sys_ks,
    raft_groups_storage& storage);

// Resolves schemas for frozen mutations and upgrades them to the current schema if needed.
//
// One instance is meant to serve a batch of mutations: schemas resolved for a given
// schema version are remembered, so mutations sharing a version are resolved only once.
class schema_store {
    using column_mappings_cache = utils::loading_cache<table_schema_version, column_mapping>;
    // Schema to apply a mutation with, plus the column mapping needed to upgrade
    // the mutation to that schema. The mapping is null when the mutation was
    // written with exactly that schema and thus needs no upgrade.
    using schema_entry = std::pair<schema_ptr, column_mappings_cache::value_ptr>;

    // Cache of column mappings, shared by all instances on this shard, so that
    // `system.scylla_table_schema_history` isn't queried for the same version repeatedly.
    static thread_local column_mappings_cache _column_mapping_cache;

    replica::database& _db;
    db::system_keyspace& _sys_ks;
    // Called as a last resort when a schema version cannot be resolved locally.
    // During normal operation this triggers a group0 barrier to wait for schema
    // propagation; during commitlog replay it is null, since group0 isn't started yet.
    std::function<future<>()> _barrier_trigger;

    // Resolved schemas, keyed by the schema version found in the mutations.
    std::unordered_map<table_schema_version, schema_entry> _schema_mappings;

    future<schema_entry> get_schema(table_id table, table_schema_version schema_version);

public:
    schema_store(replica::database& db, db::system_keyspace& sys_ks,
        std::function<future<>()> barrier_trigger = nullptr);

    // Returns the schema that `m` should be applied with. If `m` was written with an
    // older schema, it is upgraded in place to the returned schema, using the column
    // mapping fetched from `system.scylla_table_schema_history`.
    future<schema_ptr> resolve_and_upgrade(frozen_mutation& m);
};

namespace detail {
// Deserialize a frozen_mutation from a raft::log_entry_ptr.
// The log entry must contain a raft::command in its data variant.
frozen_mutation deserialize_to_frozen_mutation(const raft::log_entry_ptr& entry);
} // namespace detail
} // namespace service::strong_consistency
