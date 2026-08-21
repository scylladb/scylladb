/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include "raft/raft.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <unordered_map>

namespace cql3 { class query_processor; }
#include <deque>

namespace service::strong_consistency {
// Shard-wide batched raft IO ("rounds").
//
// One fiber per shard turns per-group commitlog writes into shared rounds:
// each round is ONE multi-entry commitlog write (force-synced) carrying
//   * every group's data-batch entries submitted while the previous round was
//     in flight (submit_append — awaited by the group's raft io_fiber,
//     preserving raft's log-before-message rule), and
//   * a commit_idx entry for every group whose commit advanced
//     (submit_commit_idx — awaited by the io_fiber before entries are pushed
//     to the applier, preserving raft's persist-before-apply ordering: once
//     anything reaches a memtable, its commit_idx is already durable, so
//     memtable flush and segment discard need no SC-side cooperation).
// Commit values ride the same synced write as the appends whenever appends
// are flowing, and get their own round only when idle. The accumulation
// window is the in-flight write's duration (write-behind; no timer), so the
// coalescing factor self-tunes: it grows exactly when syncs are expensive.
class sc_io_batcher {
public:
    sc_io_batcher(cql3::query_processor& qp, db::commitlog& cl);
    void start();
    future<> stop();
    // Returns this call's rp_handles, in writer order, once the round covering
    // them is durable.
    future<utils::chunked_vector<db::rp_handle>> submit_append(utils::chunked_vector<commitlog_raft_log_entry_writer> writers);
    // Resolves once a value >= idx for gid is durable and its fake
    // system.raft_groups mutation has been applied in memory.
    future<> submit_commit_idx(raft::group_id gid, raft::index_t idx);
private:
    future<> run();
    struct append_item {
        utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
        promise<utils::chunked_vector<db::rp_handle>> done;
    };
    future<> write_round(std::deque<append_item>& appends,
            const std::vector<std::pair<raft::group_id, raft::index_t>>& commits);

    cql3::query_processor& _qp;
    db::commitlog& _commitlog;
    std::deque<append_item> _appends;
    std::unordered_map<raft::group_id, raft::index_t> _pending_commit;
    shared_promise<> _round_done;
    condition_variable _cv;
    seastar::named_gate _gate{"sc_io_batcher"};
    bool _stopping = false;
    uint64_t _rounds = 0, _append_calls = 0, _entries = 0, _commit_values = 0;
};

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
    // Common commit log.
    db::commitlog& _commit_log;
    // Replay position handles for committed and uncommitted command entries
    // (raft::command). Consumed by
    // acquire_replay_position_handles_for() when state_machine::apply() hands
    // the entry to its target memtable, which takes over segment lifetime.
    replay_position_list _command_positions;
    // Replay position handles for dummy entries (raft::log_entry::dummy).
    // Never consumed by apply(). A dummy carries no state, so once commit_idx
    // covers it a restart has no need of it and its handle can be released;
    // see release_dummy_rp_handles().
    replay_position_list _dummy_positions;
    // Replay position handles for configuration entries (raft::configuration).
    // Never consumed by apply(). Unlike dummies these carry state that is only
    // persisted by store_snapshot_descriptor(), and commitlog replay discards
    // committed non-command entries — so their handles must stay held until a
    // snapshot has written the configuration durably. Released only by
    // truncate_log() / truncate_log_tail().
    replay_position_list _config_positions;
    // The log entries that were loaded from database commit log on startup.
    raft::log_entries _replayed_entries;
    sc_io_batcher* _batcher = nullptr;

public:
    raft_commitlog(raft::group_id group_id, db::commitlog& commit_log, table_id target_table_id, replayed_data_per_group replayed_data);

    ~raft_commitlog();

    // Persist the given log entries in the commit log. Each entry's rp_handle
    // is placed into _command_positions, _config_positions or _dummy_positions
    // based on the entry data type.
    future<> store_log_entries(const raft::log_entry_ptr_list& entries);

    void set_batcher(sc_io_batcher* b) { _batcher = b; }

    db::commitlog& commit_log() noexcept { return _commit_log; }

    // Get the log items that were loaded from database commit log on startup.
    raft::log_entries load_log();

    // Remove all the items with index >= idx, as they are considered truncated in Raft semantics.
    void truncate_log(raft::index_t idx);

    // Remove replay position handles for entries that have been snapshotted
    // and are no longer needed in the raft log. This allows the commitlog
    // segments holding those entries to be reclaimed.
    // Called from store_snapshot_descriptor after the snapshot is persisted.
    void truncate_log_tail(raft::index_t index);

    // Release dummy-entry rp_handles with index <= idx. Safe only after
    // system.raft_groups.commit_idx has been durably persisted at or above
    // idx: below that watermark raft would need those entries on restart.
    //
    // Configuration entries are deliberately NOT released here. commit_idx
    // covering a configuration entry means raft will not replay it, but the
    // configuration it carries is durable only once store_snapshot_descriptor()
    // has written it. Releasing on the commit_idx watermark would let the
    // segment be recycled while the configuration exists nowhere durable, and
    // the group would come back with a stale configuration. See SCYLLADB-3842.
    void release_dummy_rp_handles(raft::index_t idx);

    // Move replay position handles out of _command_positions for the specified
    // entries. The handles are handed to memtables in the raft state machine
    // apply(), transferring segment ownership. Triggers on_internal_error if a
    // requested entry is missing.
    std::vector<index_and_replay_position> acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries);
};
} // namespace service::strong_consistency
