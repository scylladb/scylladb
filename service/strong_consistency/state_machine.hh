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
#include <variant>
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

// A write to the table of the tablet this Raft group serves.
struct write_mutation {
    frozen_mutation mutation;
};

// The phase of a tablet resize a resize_marker entry announces.
//
// Terminology, used throughout the strongly consistent resize code. The group being replaced by a
// tablet split or merge is the *parent*, and the groups replacing it are its *children*.
// Committing both markers below is what *sealing* the parent means. A merge gives a child several
// parents, hence the neutral *resize* in the identifiers; merging is not implemented yet.
enum class resize_marker_kind : uint8_t {
    // The parent's writes are from now on served by its children.
    start_resize = 0,
    // The parent's log is final, so its children may start applying their own entries.
    end_resize = 1,
};

// Marks a phase of the resize of the Raft group the entry is appended to. Every replica turns it
// into a mutation to system.raft_groups of its own when it applies the entry.
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
// The log entry must contain a raft::command in its data variant.
raft_command deserialize_raft_command(const raft::log_entry_ptr& entry);

// The table an entry's commitlog position is accounted to. A resize marker is recorded in
// system.raft_groups, and a write in the tablet's own table. An entry which writes nothing may go
// to either; `tablet_table` is used for it.
//
// Reads the command's variant tag rather than deserializing it: this is on the write path, and
// the payload of a write is a frozen mutation. Kept next to deserialize_raft_command() because it
// depends on how raft_command is encoded.
table_id command_target_table(const raft::log_entry_ptr& entry, table_id tablet_table);
} // namespace detail
} // namespace service::strong_consistency
