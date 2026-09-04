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

// Raft persistence for strongly consistent tablet groups, backed by the
// database commitlog.
//
// The raft log itself lives in the commitlog: store_log_entries() writes one
// batch as one commitlog entry, and nothing else writes to disk on the raft
// io_fiber. Everything the group has to remember beyond the log — the snapshot
// descriptor and the truncation history — is a single row in
// system.raft_groups, and that row is written by a mutation applied straight to
// the raft_groups memtable rather than by a CQL statement.
//
// Releasing a record (see raft_commitlog, which owns everything about segments)
// writes the descriptor from that record's own (max, term) and hands the
// record's raft_groups reference to the same mutation, so the segment lives
// exactly until the flush that makes the descriptor durable. That is the whole
// retention rule: a segment holding a group's raft entries goes away only after
// a durable descriptor covers them.
class raft_groups_storage : public raft::persistence {
    raft::group_id _group_id;
    raft::server_id _server_id;
    uint16_t _shard;
    cql3::query_processor& _qp;
    replica::database& _db;
    // system.raft_groups: the table whose memtable carries the descriptor
    // mutations.
    const db::cf_id_type _raft_groups_table_id;

    // The group's raft log in the commitlog; see raft_commitlog's class comment.
    raft_commitlog _raft_commitlog;

    // Highest index raft has told us is committed. In memory only: it is
    // recovered after a crash from the batch headers, and after a clean
    // shutdown from the persisted snapshot index.
    raft::index_t _commit_index{0};
    // Highest index whose command has been handed to a memtable by apply().
    raft::index_t _apply_index{0};
    // The configuration the last released record persisted, so that a record
    // carrying no configuration of its own re-persists the current one rather
    // than clearing it.
    raft::configuration _snapshot_config;
    // Timestamp of the last descriptor mutation. api::new_timestamp() is the
    // microsecond wall clock and is neither unique per call nor monotonic across
    // a clock step, and two releases in one reactor task routinely land in the
    // same microsecond. At equal timestamps cells reconcile by value, and for
    // the two blob cells that means the larger blob wins per cell — so a row
    // could end up pairing one release's index with another's configuration.
    // Keeping our own strictly increasing value removes that.
    api::timestamp_type _last_row_timestamp = api::min_timestamp;
    // The truncation history as the row currently holds it, so a release can
    // leave that cell alone when nothing changed.
    //
    // Every release rewrites the whole list, and the list grows with leader
    // changes — a group that loses leadership once per entry would rewrite an
    // ever-longer cell on every segment it fills, for a history that did not
    // move. Skipping is safe because the cell is independent of the others: what
    // was written last stays until it is written again.
    //
    // Compared against what was *written*, not against the live list, since
    // purge_stale_truncations() edits the latter in place.
    std::vector<truncation_record> _persisted_truncations;

    // The future of the currently executing (or already finished) write operation.
    //
    // Used to linearize write operations to system.raft_groups table.
    // This is managed by `execute_with_linearization_point` helper function.
    future<> _pending_op_fut;

public:
    explicit raft_groups_storage(cql3::query_processor& qp, replica::database& db, raft::group_id gid,
        raft::server_id server_id, shard_id shard, db::commitlog& commit_log, table_id target_table_id,
        replayed_data_per_group replayed_data);


    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override;
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override;
    future<> store_commit_idx(raft::index_t) override;
    future<raft::index_t> load_commit_idx() override;
    future<raft::log_entries> load_log() override;
    future<raft::snapshot_descriptor> load_snapshot_descriptor() override;

    // A no-op: the descriptor is written by the record releases, from the
    // indexes the group has actually made durable, and raft's own idea of when
    // to snapshot has nothing to add to that.
    future<> store_snapshot_descriptor(const raft::snapshot_descriptor& snap, size_t preserve_log_entries) override;
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override;
    future<> truncate_log(raft::index_t idx) override;
    future<> abort() override;

    // Persist the initial descriptor of a new raft group. To be called before
    // start for a group this node has not hosted before.
    future<> bootstrap(raft::configuration initial_configuation, bool nontrivial_snapshot);

    // Take a reference for the command at `idx` from the record that holds it,
    // to be attached to the mutation apply() is about to put in the target
    // table's memtable. The segment then lives until that memtable's flush.
    db::rp_handle pin_for_apply(raft::index_t idx);

    // Tell the storage that the command at `idx` has been handed to a memtable.
    // May release records, so it must be called after the apply, not before.
    void note_applied(raft::index_t idx);

    // Give up every commitlog segment reference this group holds, because the
    // group is being destroyed deliberately and nothing will replay its log.
    // See raft_commitlog::release_all().
    void release_all();

    // Called with the commitlog's flush position: everything at or below it is
    // in a closed segment, which is what lets the newest record be released.
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

    // Static versions that don't require constructing a full raft_groups_storage
    // object. Used during commitlog replay, before any group is running; at
    // runtime the row is written exclusively by the record releases.
    static future<raft::index_t> load_commit_idx(cql3::query_processor& qp, raft::group_id gid, shard_id shard);
    static future<persisted_descriptor> load_descriptor(cql3::query_processor& qp, raft::group_id gid, shard_id shard);
    // Persist a descriptor by CQL. Only advances the index, so a replay that
    // runs twice cannot move the group backwards.
    static future<> store_descriptor(cql3::query_processor& qp, raft::group_id gid, shard_id shard,
        raft::index_t idx, raft::term_t term, const raft::configuration& config,
        const std::vector<truncation_record>& truncations);

private:
    // Write the group's snapshot descriptor from `rec` and hand the record's
    // raft_groups reference to that mutation.
    void write_snapshot_descriptor(segment_record& rec);

    future<> execute_with_linearization_point(std::function<future<>()> f);
};

} // namespace service::strong_consistency
