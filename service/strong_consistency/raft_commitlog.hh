/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include "raft/raft.hh"
#include "db/commitlog/commitlog.hh"
#include <deque>

namespace service::strong_consistency {
struct index_and_replay_position {
    raft::index_t index;
    db::rp_handle replay_position_handle;
};

// Raft indexes only increase, so entries are naturally sorted by index.
// A deque allows efficient access from both ends: front removal for
// truncate_log_tail() and back removal for truncate_log().
using replay_position_list = std::deque<index_and_replay_position>;

struct replayed_data_per_group {
    replay_position_list replay_positions;
    raft::log_entries entries;
};

// This class implements the persistence for raft log using database commit log.
// It is used by tablet raft groups to persist their log entries.
class raft_commitlog {
private:
    const raft::group_id _group_id;
    const db::cf_id_type _table_id;
    // cf_id used for the trailing commit_idx entry written alongside raft log
    // entries by store_log_entries(). The entry's rp_handle is handed to the
    // matching column family's memtable, so its cf_id must match to keep
    // per-cf dirty-count accounting consistent.
    const db::cf_id_type _raft_groups_table_id;
    // Common commit log.
    db::commitlog& _commit_log;
    // Replay position handles for committed and uncommitted command entries
    // (raft::command). Consumed by
    // acquire_replay_position_handles_for() when state_machine::apply() hands
    // the entry to its target memtable, which takes over segment lifetime.
    replay_position_list _command_positions;
    // Replay position handles for non-command entries (raft::configuration and
    // raft::log_entry::dummy). Never consumed by apply(); released only by
    // truncate_log() / truncate_log_tail() / release_noncommand_rp_handles().
    replay_position_list _noncommand_positions;
    // The log entries that were loaded from database commit log on startup.
    raft::log_entries _replayed_entries;

public:
    raft_commitlog(raft::group_id group_id, db::commitlog& commit_log, table_id target_table_id,
            db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data);

    ~raft_commitlog();

    // Persist the given log entries in the commit log together with a small
    // commit_idx entry for the current commit index (a raft_commit_idx_entry).
    // Returns the rp_handle of that commit_idx entry so the caller
    // (raft_groups_storage) can attach it to a fake system.raft_groups mutation
    // applied in-memory. The N raft entries' rp_handles are placed into
    // _command_positions or _noncommand_positions based on entry->data type.
    future<db::rp_handle> store_log_entries(const raft::log_entry_ptr_list& entries, raft::index_t commit_idx);

    // Get the log items that were loaded from database commit log on startup.
    raft::log_entries load_log();

    // Remove all the items with index >= idx, as they are considered truncated in Raft semantics.
    void truncate_log(raft::index_t idx);

    // Remove replay position handles for entries that have been snapshotted
    // and are no longer needed in the raft log. This allows the commitlog
    // segments holding those entries to be reclaimed.
    // Called from store_snapshot_descriptor after the snapshot is persisted.
    void truncate_log_tail(raft::index_t index);

    // Release non-command rp_handles with index <= idx. Safe only after
    // system.raft_groups.commit_idx has been durably persisted at or above
    // idx: below that watermark raft would need those non-command entries on
    // restart.
    void release_noncommand_rp_handles(raft::index_t idx);

    // Move replay position handles out of _command_positions for the specified
    // entries. The handles are handed to memtables in the raft state machine
    // apply(), transferring segment ownership. Triggers on_internal_error if a
    // requested entry is missing.
    std::vector<index_and_replay_position> acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries);
};
} // namespace service::strong_consistency
