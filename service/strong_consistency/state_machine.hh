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
#include "locator/tablets.hh"

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
    db::system_keyspace& sys_ks);

// Resolve schemas for frozen mutations and upgrade them to the current schema if needed.
// For each mutation, looks up the schema version in the local schema registry. If the mutation
// was written with an older schema, the column mapping is fetched from system.scylla_table_schema_history
// and used to upgrade the mutation to the current schema.
// Returns a parallel vector of schema_ptr (one per mutation, aligned by index).
//
// If `barrier_trigger` is provided, it is called as a last resort when a schema version cannot
// be resolved locally (e.g., during normal operation, this triggers a group0 barrier to wait for
// schema propagation). During replay, pass nullptr since group0 is not yet started.
future<std::vector<schema_ptr>> resolve_and_upgrade_mutations(
    utils::chunked_vector<frozen_mutation>& muts,
    table_id table,
    replica::database& db,
    db::system_keyspace& sys_ks,
    std::function<future<>()> barrier_trigger = nullptr);

namespace detail {
// Deserialize a frozen_mutation from a raft::log_entry_ptr.
// The log entry must contain a raft::command in its data variant.
frozen_mutation deserialize_to_frozen_mutation(const raft::log_entry_ptr& entry);
} // namespace detail
} // namespace service::strong_consistency
