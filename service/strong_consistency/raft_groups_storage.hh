/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/raft.hh"

#include <vector>
#include <deque>
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

namespace replica {
class database;
}

namespace service::strong_consistency {

// Ask table `id` to flush, in the background. `pos` only decides whether the
// table skips the request.
using flush_request_fn = std::function<void(db::cf_id_type id, db::replay_position pos)>;

// Raft persistence for strongly consistent tablet groups, backed by the database
// commitlog: the log is one commitlog entry per batch, and the snapshot descriptor
// and truncation history are one row in system.raft_groups. A segment goes away
// only once a durable descriptor covers its entries. raft_commitlog owns
// everything about segments.
class raft_groups_storage : public raft::persistence {
    raft::group_id _group_id;
    raft::server_id _server_id;
    uint16_t _shard;
    cql3::query_processor& _qp;
    replica::database& _db;
    const db::cf_id_type _raft_groups_table_id;
    // The tablet's own table, which apply() puts this group's commands into.
    const db::cf_id_type _target_table_id;
    // Empty in tests that do not care which flushes a release asks for.
    flush_request_fn _request_flush;

    raft_commitlog _raft_commitlog;

    // Highest index raft has told us is committed. In memory only: recovered
    // from the batch headers after a crash, from the snapshot index otherwise.
    raft::index_t _commit_index{0};
    // Highest index whose command has been handed to a memtable by apply().
    raft::index_t _apply_index{0};
    // The configuration the last release persisted, so that a record carrying
    // no configuration of its own re-persists it instead of clearing it.
    raft::configuration _snapshot_config;
    // The snapshot id in the row. Raft only checks it for being set.
    raft::snapshot_id _snapshot_id;
    // Timestamp of the last descriptor mutation, kept strictly increasing so two
    // releases in one reactor task cannot tie.
    api::timestamp_type _last_row_timestamp = api::min_timestamp;
    // The truncation history as the row currently holds it, so a release can leave
    // that cell alone when nothing changed.
    std::vector<truncation_record> _persisted_truncations;

    // The future of the currently executing (or already finished) write operation.
    //
    // Used to linearize write operations to system.raft_groups table.
    // This is managed by `execute_with_linearization_point` helper function.
    future<> _pending_op_fut;

public:
    explicit raft_groups_storage(cql3::query_processor& qp, replica::database& db, raft::group_id gid,
        raft::server_id server_id, shard_id shard, db::commitlog& commit_log, table_id target_table_id,
        replayed_data_per_group replayed_data, flush_request_fn request_flush = {});


    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override;
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override;
    future<> store_commit_idx(raft::index_t) override;
    future<raft::index_t> load_commit_idx() override;
    future<raft::log_entries> load_log() override;
    future<raft::snapshot_descriptor> load_snapshot_descriptor() override;

    // A no-op: the record releases write the descriptor, from the indexes the
    // group has actually made durable.
    future<> store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) override;
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override;
    future<> truncate_log(raft::index_t idx) override;
    future<> abort() override;

    // Persist the initial descriptor of a new raft group. To be called before
    // start for a group this node has not hosted before.
    future<> bootstrap(raft::configuration initial_configuation, bool nontrivial_snapshot);

    // Take a reference for the command at `idx`, for the mutation apply() puts
    // in the target table's memtable: the segment lives until that flush.
    db::rp_handle pin_for_apply(raft::index_t idx);

    // Tell the storage that the command at `idx` has been handed to a memtable.
    // May release records, so it must be called after the apply, not before.
    void note_applied(raft::index_t idx);

    // Report the commitlog's flush position. Releasing the newest record needs it.
    void mark_segment_closed(db::replay_position pos);

    // Release every record that is now committed, applied and closed.
    void maybe_release();

    // Everything one group's row holds, as commitlog replay needs to read it.
    struct persisted_descriptor {
        bool exists = false;
        raft::index_t idx{0};
        raft::term_t term{0};
        raft::configuration config;
        std::vector<truncation_record> truncations;
    };

    // For commitlog replay, before any group is running. At runtime only the
    // record releases write the row.
    static future<raft::index_t> load_commit_idx(cql3::query_processor& qp, raft::group_id gid, shard_id shard);
    static future<persisted_descriptor> load_descriptor(cql3::query_processor& qp, raft::group_id gid, shard_id shard);
    // Persist a snapshot descriptor by CQL. Only used during commitlog replay,
    // before any group is running; at runtime the row is written exclusively by
    // the record releases. Only advances the index, so repeated replays are
    // idempotent.
    static future<> store_snapshot_index(cql3::query_processor& qp, raft::group_id gid, shard_id shard,
        const raft::snapshot_descriptor& snap);

private:
    void write_snapshot_descriptor(segment_record& rec);

    future<> execute_with_linearization_point(std::function<future<>()> f);
};

} // namespace service::strong_consistency
