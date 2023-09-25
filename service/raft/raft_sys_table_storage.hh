/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "raft/raft.hh"

#include <vector>
#include <functional>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>

#include "service/query_state.hh"
#include "seastarx.hh"

namespace cql3 {

class query_processor;

namespace statements {

class modification_statement;

} // namespace cql3::statements

} // namespace cql3

namespace service {

// Scylla-specific implementation of raft persistence module.
//
// Uses "raft" system table as a backend storage to persist raft state.
class raft_sys_table_storage : public raft::persistence {
    raft::group_id _group_id;
    raft::server_id _server_id;
    // Prepared statement instance used for construction of batch statements on
    // `store_log_entries` calls.
    shared_ptr<cql3::statements::modification_statement> _store_entry_stmt;
    cql3::query_processor& _qp;
    service::query_state _dummy_query_state;
    // The future of the currently executing (or already finished) write operation.
    //
    // Used to linearize write operations to system.raft table.
    // This is managed by `execute_with_linearization_point` helper function.
    // All RPC entry points that involve writing to system.raft are guarded with
    // this helper.
    future<> _pending_op_fut;

    const size_t _max_mutation_size;

public:
    explicit raft_sys_table_storage(cql3::query_processor& qp, raft::group_id gid, raft::server_id server_id);

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
    //
    // If `nontrivial_snapshot` is true, the initial snapshot will have index 1 instead of 0,
    // which will trigger a snapshot transfer to servers which start with snapshot index 0.
    // This should be set for the first group 0 server during upgrade or recovery, which
    // will force snapshot transfers for subsequently joining nodes (so we can transfer initial
    // schema etc.). It's also correct to do it when booting a cluster from
    // scratch with Raft, although not necessary (it will force an empty snapshot transfer).
    future<> bootstrap(raft::configuration initial_configuation, bool nontrivial_snapshot);
private:

    future<size_t> do_store_log_entries_one_batch(const std::vector<raft::log_entry_ptr>& entries, size_t start_idx);
    future<> do_store_log_entries(const std::vector<raft::log_entry_ptr>& entries);
    // Truncate all entries from the persisted log with indices <= idx
    // Called from the `store_snapshot` function.
    future<> update_snapshot_and_truncate_log_tail(const raft::snapshot_descriptor &snap, size_t preserve_log_entries);

    future<> execute_with_linearization_point(std::function<future<>()> f);
};

} // end of namespace service
