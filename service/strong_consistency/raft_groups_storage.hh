/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/raft.hh"

#include <optional>
#include <vector>
#include <functional>

#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "raft_commitlog.hh"

namespace cql3 {

class query_processor;

namespace statements {

class modification_statement;

} // namespace cql3::statements

} // namespace cql3

namespace service::strong_consistency {

// Raft persistence for strongly consistent tablet groups.
//
// Similar to raft_sys_table_storage but uses the tablet-specific tables
// (raft_groups, raft_groups_snapshots, raft_groups_snapshot_config) which
// have a (shard, group_id) composite partition key allowing data to reside
// on the same shard as the tablet replica.
class raft_groups_storage : public raft::persistence {
    raft_commitlog _raft_commitlog;
    raft::group_id _group_id;
    raft::server_id _server_id;
    uint16_t _shard;
    cql3::query_processor& _qp;
    // The future of the currently executing (or already finished) write operation.
    //
    // Used to linearize write operations to system.raft_groups table.
    // This is managed by `execute_with_linearization_point` helper function.
    future<> _pending_op_fut;

public:
    explicit raft_groups_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id, shard_id shard,
        db::commitlog& commit_log, table_id target_table_id, replayed_data_per_group replayed_data);

    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override;
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override;
    future<> store_commit_idx(raft::index_t) override;
    future<raft::index_t> load_commit_idx() override;
    future<raft::log_entries> load_log() override;
    future<raft::snapshot_descriptor> load_snapshot_descriptor() override;

    // Store a snapshot `snap` and preserve the most recent `preserve_log_entries` log entries,
    // i.e. truncate all entries with `idx <= (snap.idx - preserve_log_entries)`
    future<> store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) override;
    // Pre-checks that no log truncation is in process before dispatching to the actual implementation
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override;
    future<> truncate_log(raft::index_t idx) override;
    future<> abort() override;

    // Persist initial configuration of a new Raft group.
    // To be called before start for the new group.
    future<> bootstrap(raft::configuration initial_configuation, bool nontrivial_snapshot);

    // Static version that doesn't require constructing a full raft_groups_storage object.
    // Useful during commitlog replay when only read access to metadata is needed.
    static future<raft::index_t> load_commit_idx(cql3::query_processor& qp, raft::group_id gid, shard_id shard);

    // The persisted commit index, or nullopt when this shard persists nothing about the
    // group at all. load_commit_idx() reports both as 0, but the two mean very different
    // things: a group that was bootstrapped and hasn't committed anything yet, versus one
    // whose state was never written or has been erased because the replica left. Commitlog
    // replay and group startup both have to reject entries in the second case.
    static future<std::optional<raft::index_t>> load_commit_idx_if_persisted(cql3::query_processor& qp,
        raft::group_id gid, shard_id shard);

    // Deletes everything this shard persists about the group: term and vote, commit index,
    // snapshot descriptor and its configuration.
    //
    // Called when a replica leaves the group, so that a tablet migrated back to this shard
    // later rejoins with no history of its previous membership. Without it the replica
    // rejoins claiming a commit index it no longer has data for, and the leader never
    // resends the entries below it.
    //
    // The group's entries in the shared commitlog cannot be deleted; what makes them inert
    // is the absence of the state this erases - see raft_commitlog_replay_buffer and
    // groups_manager::start_raft_group().
    static future<> erase_persisted_state(cql3::query_processor& qp, raft::group_id gid, shard_id shard);
    // Store snapshot idx and term without updating the configuration.
    // Used to advance the persisted snapshot index so that raft does not
    // re-apply already applied entries on restart. Only writes if the new
    // index is higher than the existing one (safe to call on repeated replays).
    static future<> store_snapshot_index(cql3::query_processor& qp, raft::group_id gid, shard_id shard, const raft::snapshot_descriptor& snap);

    std::vector<index_and_replay_position> acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries);

private:

    future<> update_snapshot(const raft::snapshot_descriptor &snap);

    future<> execute_with_linearization_point(std::function<future<>()> f);
};

} // namespace service::strong_consistency
