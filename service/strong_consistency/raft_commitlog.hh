/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include "raft/raft.hh"
#include "db/commitlog/commitlog.hh"
#include "utils/chunked_vector.hh"
#include <deque>
#include <optional>

namespace service::strong_consistency {

struct raft_term_and_index {
    raft::index_t idx{0};
    raft::term_t term{0};
};

// One truncation as persisted in system.raft_groups.truncations: in this
// segment, the then-current copies of indexes [from, to] were truncated.
struct truncation_record {
    db::segment_id_type segment{0};
    raft::index_t from{0};
    raft::index_t to{0};

    bool operator==(const truncation_record&) const = default;
};

// A segment_record is maintained for each segment in which the Raft group has data.
struct segment_record {
    // The group's own reference to the segment. It must outlive every entry
    // here that is not yet both committed and applied, and it is the source of
    // the per-command references handed to the target table's memtable.
    db::rp_handle pin_user_table;
    // The same segment, referenced under system.raft_groups. Taken when the
    // record is created rather than when it is released, and that order is what
    // keeps a quiescent group from deadlocking: releasing the newest record
    // needs the closed signal (see _closed_up_to), the signal comes only from
    // flush requests carrying the raft_groups id, and a round names a table only
    // for segments dirty under it. A segment holding nothing but raft entries is
    // raft_groups-dirty solely because of this reference — take it at release
    // instead and the release would be waiting for a signal that only the
    // release could produce.
    //
    // At release it moves into the in-memory mutation that persists the
    // descriptor, so the segment then lives exactly until the raft_groups
    // memtable flush that makes that value durable.
    db::rp_handle pin_raft_groups;
    // Index range of this group's entries in the segment, both ends inclusive.
    raft::index_t first{0};
    raft::index_t max{0};
    // (index, term) runs, ascending: the term of the entry at any index in
    // [first, max] is the term of the last run whose index is at or below it.
    utils::chunked_vector<raft_term_and_index> terms;
    // Configurations this record's entries carried, in index order.
    utils::chunked_vector<std::pair<raft::index_t, raft::configuration>> configs;
    // Indexes of the dummy and configuration entries.
    utils::chunked_vector<raft::index_t> noncmd_indexes;

    db::segment_id_type segment() const {
        return pin_user_table.rp().id;
    }

    // Term of the entry at `max`, the term persisted with that index.
    raft::term_t max_term() const {
        return terms.empty() ? raft::term_t{0} : terms.back().term;
    }

    // Highest command index in [first, max]; disengaged for a record holding
    // only dummies and configurations.
    std::optional<raft::index_t> last_cmd() const;

    std::optional<std::pair<raft::index_t, raft::configuration>> last_conf() const {
        if (configs.empty()) {
            return std::nullopt;
        }
        return configs.back();
    }

    // Drop everything at or above `idx` after a truncation, and clamp `max`.
    void trim_from(raft::index_t idx);

    // Give up both references without decrementing, so the segments outlive this
    // record. Teardown paths only.
    void detach() {
        pin_user_table.release();
        pin_raft_groups.release();
    }
};

// What commitlog replay hands a starting group: its rewritten uncommitted
// entries, and the records for the batch they were rewritten as.
struct replayed_data_per_group {
    std::deque<segment_record> records;
    raft::log_entries entries;
};

// Write `entries` to `cl` under `table` as one raft batch carrying `commit_idx`
// in its header, and return the reference that write produced.
//
// Raises an internal error if the batch does not fit one commitlog entry. The
// commitlog would otherwise fragment it across segments, and the segment records
// and the truncation records need a copy of an entry to live in exactly one
// segment.
//
// The batch size is effectively bounded by max_log_size: raft admits no command
// over max_command_size, and fsm's log_limiter_semaphore holds a leader's
// accounted log at max_log_size, of which a batch is a subset (see
// groups_manager's raft::server configuration). Their sum clears a default 64MB
// segment's 32MB max_record_size(), but not a much smaller configured segment,
// so the error is reachable.
//
// Fixing an oversized batch means splitting it into several whole batches.
// Fragmenting one entry instead needs the commitlog to release an oversized
// entry's tail segments per position, which is SCYLLADB-3986.
//
// Static so that commitlog replay can write the same format when it rewrites a
// group's uncommitted entries.
future<db::rp_handle> write_raft_batch(db::commitlog& cl, table_id table,
        raft::group_id group_id, raft::index_t commit_idx, const raft::log_entry_ptr_list& entries);

// Fold a written batch into `segment_queue`, extending the newest record or
// starting a new one when the batch landed in a segment new to the group.
void account_batch(std::deque<segment_record>& segment_queue, const db::cf_id_type& raft_groups_table_id,
        db::rp_handle&& handle, std::span<const raft::log_entry_ptr> entries);

// One group's raft log in the commitlog: which segments hold it, what index range
// each holds, which references keep them, and what a truncation superseded.
class raft_commitlog {
    const raft::group_id _group_id;
    // The tablet's own table: batches are written under it, and the per-command
    // references handed to apply() are accounted to it.
    const db::cf_id_type _table_id;
    // system.raft_groups, under which each segment's second reference is held.
    const db::cf_id_type _raft_groups_table_id;
    db::commitlog& _commit_log;

    // One record per segment this group has entries in, oldest first.
    std::deque<segment_record> _commitlog_segment_queue;
    // Truncation history as persisted in the row. Not ordered by segment; within
    // one segment it is chronological, which is all replay's cursors need.
    std::vector<truncation_record> _truncations;
    // How far the commitlog has closed segments on this shard, as reported by
    // its flush handler.
    db::replay_position _closed_up_to;
    raft::log_entries _replayed_entries;

public:
    raft_commitlog(raft::group_id group_id, db::commitlog& commit_log, table_id target_table_id,
        db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data);
    ~raft_commitlog();

    // Write the entries as one batch (see write_raft_batch) and account it.
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries, raft::index_t commit_idx);

    // Discard the entries at or above `idx`, dropping or clamping the records
    // that held them and remembering in _truncations what was superseded.
    void truncate_log(raft::index_t idx);

    db::rp_handle pin_for_apply(raft::index_t idx);

    // Report the commitlog's flush position: everything at or below it is in a
    // closed segment.
    void note_closed_up_to(db::replay_position pos);

    // The oldest record that may now be released, or nullptr. Ownership stays
    // here: the caller persists the descriptor, then calls pop_released().
    segment_record* front_releasable(raft::index_t commit_idx, raft::index_t apply_idx);
    void pop_released();

    // Seed the truncation history from the group's row, once, before it starts.
    void seed_truncations(std::vector<truncation_record> truncations) {
        _truncations = std::move(truncations);
    }

    // Drop the truncation records whose segment the commitlog no longer has: it
    // cannot hand out a position that low, so no replay can see those copies.
    void purge_stale_truncations();
    const std::vector<truncation_record>& truncations() const {
        return _truncations;
    }

    // The entries commitlog replay recovered, handed over once.
    raft::log_entries load_log();

    size_t segment_count() const {
        return _commitlog_segment_queue.size();
    }
};

} // namespace service::strong_consistency
