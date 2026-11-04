/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "service/raft/raft_state_machine.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/timestamp.hh"
#include <functional>
#include <unordered_map>
#include <variant>
#include "locator/tablets.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "utils/estimated_histogram.hh"
#include "utils/UUID.hh"

namespace db {
class system_keyspace;
}

namespace service {
class migration_manager;
}

namespace service::strong_consistency {

class raft_resize_tracker;

struct state_machine_stats {
    utils::time_estimated_histogram apply_latency;
    uint64_t schema_barrier_waits = 0;
};

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
// into a mutation to system.raft_groups of its own when it applies the entry, because the row is
// keyed by the shard hosting the group, which differs between replicas.
struct resize_marker {
    resize_marker_kind kind;
    // The timestamp of the marker's cell, taken from the clock of the leader which appended the
    // entry - the clock every write of the group is stamped by - so that the marker sorts after
    // every write appended ahead of it. Carried in the entry rather than taken on apply so that
    // every replica, and every replay, writes the same cell.
    api::timestamp_type timestamp;
};

struct truncate_command {
    api::timestamp_type truncated_at;
    utils::UUID request_id;
};

// A schema_changed (see raft_groups_storage.hh) announces the schema version the writes
// appended after it, up to the next schema_changed, were built with. The leader appends one
// before the first write of a version it has not announced in its term, so the log reads
// [schema_changed[v1], write, write, schema_changed[v2], write, ...]. Its mapping lets a
// replica upgrade those writes to its own, newer, schema without consulting
// system.scylla_table_schema_history, whose rows expire.
struct raft_command {
    // Note: needs to be default-constructible to use with ser::deserialize, which is why the
    // marker, and not the write, comes first.
    std::variant<resize_marker, write_mutation, truncate_command, schema_changed> change;
};

// Builds the mutation which records `marker` in the system.raft_groups row of the group `gid`
// hosted on `shard`.
mutation make_resize_marker_mutation(raft::group_id gid, shard_id shard, const resize_marker& marker);

std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    replica::database& db,
    service::migration_manager& mm,
    db::system_keyspace& sys_ks,
    raft_groups_storage& storage,
    raft_resize_tracker& resize_tracker,
    lw_shared_ptr<state_machine_stats> stats);

// Applies the truncate_command `tc`, committed at `idx` in the log of the group `gid`, which
// serves `tablet`, on this shard. Returns once the record is durable. A no-op when `record`
// already carries tc.request_id.
future<> apply_truncate_command(replica::database& db, locator::global_tablet_id tablet, raft::group_id gid,
        raft::index_t idx, const truncate_command& tc, truncate_record& record);

// Resolves the schema to apply writes with, from the schema_changed the leader announced
// them under, and upgrades them to it if needed.
//
// A version is resolved once per instance: the state machine keeps one for as long as an
// announcement stands, commitlog replay one for the whole replay.
class schema_store {
    // Schema to apply a write with, plus the column mapping needed to upgrade the
    // write to that schema. The mapping is disengaged when the announced version is
    // known locally, so the write needs no upgrade.
    struct schema_entry {
        schema_ptr schema;
        std::optional<column_mapping> mapping;
    };

    replica::database& _db;
    // Called when the announced version is unknown locally: either the replica is
    // behind on group0 and the version is newer than its table's, or the version is
    // older and the mapping must be used. The barrier tells the two apart: after it
    // the table is at least as new as the leader's was. During commitlog replay it is
    // null, since group0 isn't started yet.
    std::function<future<>()> _barrier_trigger;

    // Resolved schemas, keyed by the announced version.
    std::unordered_map<table_schema_version, schema_entry> _schema_mappings;

    future<schema_entry> get_schema(table_id table, const schema_changed& announced);

public:
    schema_store(replica::database& db, std::function<future<>()> barrier_trigger = nullptr);

    // Forget the resolved versions, once a new announcement supersedes them.
    void clear() {
        _schema_mappings.clear();
    }

    // Returns the schema that `m`, announced under `announced`, should be applied with;
    // `m` has to carry the announced version.
    // If that version is older than the table's schema, `m` is upgraded in place to the
    // returned schema using the announced column mapping.
    future<schema_ptr> resolve_and_upgrade(table_id table, const schema_changed& announced, frozen_mutation& m);
};

namespace detail {
// The log entry must contain a raft::command in its data variant.
raft_command deserialize_raft_command(const raft::log_entry_ptr& entry);
} // namespace detail
} // namespace service::strong_consistency

template <>
struct fmt::formatter<service::strong_consistency::resize_marker_kind> : fmt::formatter<string_view> {
    auto format(service::strong_consistency::resize_marker_kind, fmt::format_context& ctx) const -> decltype(ctx.out());
};
