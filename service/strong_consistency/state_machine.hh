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
#include <variant>
#include "locator/tablets.hh"
#include "service/strong_consistency/raft_groups_storage.hh"

namespace db {
class system_keyspace;
}

namespace service {
class migration_manager;
}

namespace service::strong_consistency {

// A write to the table of the tablet this Raft group serves.
struct write_mutation {
    frozen_mutation mutation;
};

// The phase of a tablet resize a resize_marker entry announces.
//
// Terminology, used here and throughout the strongly consistent resize code: the group being
// replaced by a tablet split or merge is the *parent*, and the groups replacing it are its
// *children*. Committing both markers below is what *sealing* the parent means. A merge gives a
// child several parents, hence the neutral *resize* in the identifiers; merging is not
// implemented yet.
enum class resize_marker_kind : uint8_t {
    // The parent's writes are from now on served by its children.
    start_resize = 0,
    // The parent's log is final, so its children may start applying their own entries.
    end_resize = 1,
};

// Marks a phase of the resize of the Raft group the entry is appended to.
//
// Needs to be converted into a mutation to system.raft_groups when applying
struct resize_marker {
    resize_marker_kind kind;
};

// An entry which carries no state change. Appended to a child so that its applier fiber has
// something to block on until its parent is sealed.
struct no_op {};

struct raft_command {
    // Note: needs to be default-constructible to use with ser::deserialize
    std::variant<no_op, write_mutation, resize_marker> change;
};

// Builds the mutation which records `kind` in the system.raft_groups row of the group `gid`
// hosted on `shard`. Only the presence of a marker is ever read back.
mutation make_resize_marker_mutation(raft::group_id gid, shard_id shard, resize_marker_kind kind);

std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    replica::database& db,
    service::migration_manager& mm,
    db::system_keyspace& sys_ks,
    raft_groups_storage& storage);

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
// Deserialize a raft_command from a raft::log_entry_ptr.
// The log entry must contain a raft::command in its data variant.
raft_command deserialize_raft_command(const raft::log_entry_ptr& entry);
} // namespace detail
} // namespace service::strong_consistency
