/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "raft/raft.hh"

#include <vector>
#include <functional>
#include <optional>
#include <map>
#include <unordered_map>

#include <seastar/core/future.hh>

#include "service/query_state.hh"
#include "seastarx.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/rp_set.hh"

namespace cql3 {

class query_processor;

} // namespace cql3

namespace service::strong_consistency {

// Raft persistence for strongly consistent tablet groups.
//
// Similar to raft_sys_table_storage but uses the tablet-specific tables
// (raft_groups, raft_groups_snapshots, raft_groups_snapshot_config) which
// have a (shard, group_id) composite partition key allowing data to reside
// on the same shard as the tablet replica.
class raft_groups_storage : public raft::persistence {
public:
    static inline const std::string COMMITLOG_FILENAME_PREFIX = "RaftGroupsLog-";

    struct group_replay_state {
        std::map<raft::index_t, raft::log_entry_ptr> log;
        std::optional<raft::index_t> replayed_max_idx;
        std::optional<raft::index_t> replayed_seen_max_idx;
        std::optional<raft::index_t> replayed_truncate_idx;
        std::optional<raft::index_t> replayed_truncate_prefix_idx;
        bool replayed_has_truncate = false;
    };

private:

    raft::group_id _group_id;
    raft::server_id _server_id;
    uint16_t _shard;
    cql3::query_processor& _qp;
    service::query_state _dummy_query_state;
    // The future of the currently executing (or already finished) write operation.
    //
    // Used to linearize write operations to system.raft_groups table.
    // This is managed by `execute_with_linearization_point` helper function.
    future<> _pending_op_fut;

    db::commitlog* _commitlog = nullptr;
    table_id _table_id;
    std::map<raft::index_t, raft::log_entry_ptr> _log;
    std::map<raft::index_t, db::rp_handle> _entry_rp_handles;
    std::optional<db::rp_handle> _truncate_tail_rp_handle;
    std::optional<db::rp_handle> _truncate_prefix_rp_handle;
    std::optional<raft::index_t> _replayed_max_idx;
    std::optional<raft::index_t> _replayed_seen_max_idx;
    std::optional<raft::index_t> _replayed_truncate_idx;
    std::optional<raft::index_t> _replayed_truncate_prefix_idx;
    bool _replayed_has_truncate = false;
    bool _replay_state_loaded = false;
    raft::index_t _commit_idx_floor = raft::index_t{0};
    bool _allow_legacy_cleanup = false;
    bool _replay_had_errors = false;

public:
    explicit raft_groups_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id, shard_id shard, db::commitlog* commitlog);
    explicit raft_groups_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id, shard_id shard);

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

    future<> replay_log_entries(std::vector<sstring> files);
    static future<std::unordered_map<raft::group_id, group_replay_state>> replay_log_entries_for_groups(cql3::query_processor& qp, std::vector<sstring> files, bool* had_errors = nullptr);
    void set_replayed_log_state(group_replay_state state);
    void set_replay_had_errors(bool had_errors);
    future<> filter_replayed_log_with_snapshot();
    future<> materialize_log_state_to_commitlog();
    future<bool> import_legacy_log_entries();
    future<> cleanup_legacy_log_entries();

    bool has_commitlog_log_entries() const;
    bool should_cleanup_legacy_log_entries() const;

    // Persist initial configuration of a new Raft group.
    // To be called before start for the new group.
    future<> bootstrap(raft::configuration initial_configuation, bool nontrivial_snapshot);

private:
    // Truncate all entries from the persisted log with indices <= idx
    // Called from the `store_snapshot` function.
    future<> update_snapshot_and_truncate_log_tail(const raft::snapshot_descriptor &snap, size_t preserve_log_entries);

    future<> execute_with_linearization_point(std::function<future<>()> f);
    future<> store_log_entries_internal(const std::vector<raft::log_entry_ptr>& entries);
    future<db::rp_handle> add_raft_entry_to_commitlog(const raft_commit_log_entry& e);
    future<> replay_log_entries_from_file(const sstring& file);
    void apply_replayed_entry(const raft::log_entry& entry);
    void apply_replayed_truncate(raft::index_t idx);
    void record_replayed_truncate_prefix(raft::index_t idx);
    void apply_replayed_truncate_prefix(raft::index_t idx);
    future<> truncate_prefix_log(raft::index_t idx);
    future<> truncate_prefix_log_impl(raft::index_t idx);
};

} // namespace service::strong_consistency
