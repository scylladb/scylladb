/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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

// Scylla-specific implementation of raft persistence module.
//
// Uses "raft" system table as a backend storage to persist raft state.
class raft_sys_table_storage : public raft::persistence {
    raft::group_id _group_id;
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

public:
    explicit raft_sys_table_storage(cql3::query_processor& qp, raft::group_id gid);

    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override;
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override;
    future<raft::log_entries> load_log() override;
    future<raft::snapshot> load_snapshot() override;

    // Store a snapshot `snap` and preserve the most recent `preserve_log_entries` log entries,
    // i.e. truncate all entries with `idx <= (snap.idx - preserve_log_entries)`
    future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) override;
    // Pre-checks that no log truncation is in process before dispatching to the actual implementation
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override;
    future<> truncate_log(raft::index_t idx) override;
    future<> abort() override;

private:

    future<> do_store_log_entries(const std::vector<raft::log_entry_ptr>& entries);
    // Truncate all entries from the persisted log with indices <= idx
    // Called from the `store_snapshot` function.
    future<> truncate_log_tail(raft::index_t idx);

    future<> execute_with_linearization_point(std::function<future<>()> f);
};
