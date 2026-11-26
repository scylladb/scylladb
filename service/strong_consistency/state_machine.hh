/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "service/raft/raft_state_machine.hh"
#include "mutation/mutation.hh"
#include "mutation/timestamp.hh"
#include "reader_permit.hh"
#include "sstables/mx/partition_codec.hh"
#include <fmt/ranges.h>
#include <functional>
#include <stdexcept>
#include <unordered_map>
#include <variant>
#include "locator/tablets.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "utils/loading_cache.hh"
#include "utils/hash.hh"
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

// Index 1 of raft_command::change held a frozen_mutation write. Never written;
// deserialize_raft_command() refuses it with retired_raft_command_error.
struct retired_write_mutation {
};

// A write to the table of the tablet this Raft group serves. `data` is laid out by
// encode_write_mutation(). The table is the group's tablet's, so it is not carried.
struct write_mutation_v2 {
    table_schema_version schema_version;
    bytes_ostream data;
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

struct raft_command {
    // Note: needs to be default-constructible to use with ser::deserialize, which is why the
    // marker, and not the write, comes first.
    std::variant<resize_marker, retired_write_mutation, truncate_command, write_mutation_v2> change;
};

// The variant index is the wire format, so the alternatives never move.
static_assert(std::is_same_v<std::variant_alternative_t<1, decltype(raft_command::change)>, retired_write_mutation>);
static_assert(std::is_same_v<std::variant_alternative_t<3, decltype(raft_command::change)>, write_mutation_v2>);

// Shard-wide cache of the codec_schemas write_mutation_v2 payloads are encoded and
// decoded with, keyed by the writer's and the reader's schema version. Building one
// copies the type name of every column, too costly to repeat per write.
class codec_schema_cache {
    // The writer's and the reader's schema version.
    using key = std::pair<table_schema_version, table_schema_version>;
    using cache_type = utils::loading_cache<key, sstables::mx::codec_schema, 0, utils::loading_cache_reload_enabled::no,
            utils::simple_entry_size<sstables::mx::codec_schema>, utils::tuple_hash>;
    cache_type _cache;
public:
    codec_schema_cache();
    // Encodes and decodes with s.
    future<sstables::mx::codec_schema> get(const schema_ptr& s);
    // Decodes data written with `writer` into `reader`.
    future<sstables::mx::codec_schema> get(const schema_ptr& writer, const schema_ptr& reader);
    // Decodes data written with schema version `writer`, known by its column mapping
    // and clustering key fixed lengths, into `reader`.
    future<sstables::mx::codec_schema> get(table_schema_version writer, const column_mapping& writer_columns,
            const std::vector<std::optional<uint32_t>>& ck_fixed_lengths, const schema_ptr& reader);
};

// The codec_schema encode_write_mutation() takes for mutations of schema s.
sstables::mx::codec_schema make_write_codec_schema(const schema& s);

// Encodes m as a write_mutation_v2. base_timestamp is the delta base of the cell
// timestamps: the write's timestamp makes each delta one byte. cs is
// make_write_codec_schema(*m.schema()), or a cached copy of it.
write_mutation_v2 encode_write_mutation(const mutation& m, api::timestamp_type base_timestamp,
        const sstables::mx::codec_schema& cs);

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
    codec_schema_cache& codecs,
    lw_shared_ptr<state_machine_stats> stats);

// Applies the truncate_command `tc`, committed at `idx` in the log of the group `gid`, which
// serves `tablet`, on this shard. Returns once the record is durable. A no-op when `record`
// already carries tc.request_id.
future<> apply_truncate_command(replica::database& db, locator::global_tablet_id tablet, raft::group_id gid,
        raft::index_t idx, const truncate_command& tc, truncate_record& record);

// Resolves the schemas of write_mutation_v2 commands and decodes them.
//
// One instance is meant to serve a batch of commands: schemas resolved for a given
// schema version are remembered, so commands sharing a version are resolved only once.
class schema_store {
    // Keyed by table and schema version, as system.scylla_table_schema_history is. One
    // cache serves every group of the shard, and the version alone would hand a group
    // the mapping of another table's version.
    using column_mappings_cache = utils::loading_cache<std::pair<table_id, table_schema_version>, column_mapping, 0,
            utils::loading_cache_reload_enabled::no, utils::simple_entry_size<column_mapping>, utils::tuple_hash>;
    // Schema to decode a command with, plus the column mapping of the schema version
    // it was written with. The mapping is null when the schema is that version.
    using schema_entry = std::pair<schema_ptr, column_mappings_cache::value_ptr>;
    // Schema to decode a command into, and the codec_schema to decode it with.
    using codec_entry = std::pair<schema_ptr, sstables::mx::codec_schema>;

    // Cache of column mappings, shared by all instances on this shard, so that
    // `system.scylla_table_schema_history` isn't queried for the same table and version repeatedly.
    static thread_local column_mappings_cache _column_mapping_cache;

    replica::database& _db;
    db::system_keyspace& _sys_ks;
    // Null during commitlog replay, which builds each codec_schema once per instance.
    codec_schema_cache* _codecs;
    // Called as a last resort when a schema version cannot be resolved locally.
    // During normal operation this triggers a group0 barrier to wait for schema
    // propagation; during commitlog replay it is null, since group0 isn't started yet.
    std::function<future<>()> _barrier_trigger;

    // Resolved schemas, keyed by the schema version found in the commands.
    std::unordered_map<table_schema_version, codec_entry> _schema_mappings;

    future<schema_entry> get_schema(table_id table, table_schema_version schema_version);
    future<codec_entry> resolve_codec(table_id table, table_schema_version schema_version,
            const std::vector<std::optional<uint32_t>>& ck_fixed_lengths);

public:
    schema_store(replica::database& db, db::system_keyspace& sys_ks, codec_schema_cache* codecs,
        std::function<future<>()> barrier_trigger = nullptr);

    // Decodes `w`, a write to `table`, into the table's current schema. Data at or
    // before its column's drop is skipped. Other data the current schema cannot take
    // throws sstables::mx::column_mismatch_error.
    future<mutation> decode(table_id table, const write_mutation_v2& w, reader_permit permit);
};

// Thrown when a command's raft_command::change index is past the alternatives this
// version knows. Fails the applier or the replay instead of applying a wrong command.
class unknown_raft_command_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Thrown for a retired_write_mutation command, written by an older version in a
// format this version no longer decodes.
class retired_raft_command_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

namespace detail {
// The log entry must contain a raft::command in its data variant.
// Throws unknown_raft_command_error on a command kind this version does not know, and
// retired_raft_command_error on retired_write_mutation.
raft_command deserialize_raft_command(const raft::log_entry_ptr& entry);
} // namespace detail
} // namespace service::strong_consistency

template <>
struct fmt::formatter<service::strong_consistency::resize_marker_kind> : fmt::formatter<string_view> {
    auto format(service::strong_consistency::resize_marker_kind, fmt::format_context& ctx) const -> decltype(ctx.out());
};
