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

// One record per commitlog segment holding this group's entries.
struct segment_record {
    // Reference at the position this record's first batch was written to,
    // accounted to the group's own table. Source of the per-command references.
    db::rp_handle pin_user_table;
    // A second reference at the same position, accounted to system.raft_groups.
    // Taken when the record is created, not when it is released. A segment
    // holding only raft entries is raft_groups-dirty because of this reference
    // alone, and the closed signal a release waits for arrives only in a flush
    // round naming that table. Take it at release and the release would wait for
    // a signal only the release could produce.
    db::rp_handle pin_raft_groups;
    // Index range of this group's entries in the segment, both ends inclusive.
    raft::index_t first_index{0};
    raft::index_t max_index{0};
    // (index, term) runs, ascending: the term of the entry at any index in
    // [first_index, max_index] is the term of the last run at or below it.
    utils::chunked_vector<raft_term_and_index> terms;
    // Configurations this record's entries carried, in index order.
    utils::chunked_vector<std::pair<raft::index_t, raft::configuration>> configs;
    // Indexes of the dummy and configuration entries.
    utils::chunked_vector<raft::index_t> noncmd_indexes;

    db::segment_id_type segment() const {
        return pin_user_table.rp().id;
    }

    // Term of the entry at `max_index`, the term persisted with that index.
    raft::term_t max_term() const {
        return terms.empty() ? raft::term_t{0} : terms.back().term;
    }

    // Highest command index in [first_index, max_index]; disengaged for a
    // record holding only dummies and configurations.
    std::optional<raft::index_t> last_cmd() const;

    std::optional<std::pair<raft::index_t, raft::configuration>> last_conf() const {
        if (configs.empty()) {
            return std::nullopt;
        }
        return configs.back();
    }

    // Drop everything at or above `idx` after a truncation, and clamp `max_index`.
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

// Bounds on a group's log, which together bound one raft batch. raft's add_entry()
// rejects a command over raft_max_command_size, and fsm's log_limiter_semaphore
// holds a leader's accounted log at raft_max_log_size, of which a batch is a subset.
inline constexpr size_t raft_max_log_size = 20 * 1024 * 1024;
inline constexpr size_t raft_max_command_size = 100 * 1024;

// Write `entries` to `cl` under `table` as one raft batch carrying `commit_idx`,
// and return the reference that write produced. `entries` must not be empty.
// Internal error if the batch does not fit one commitlog entry: every copy of an
// entry must live in a single segment. Fixing an oversized batch means splitting
// it into whole batches; fragmenting one entry instead needs the commitlog to
// release an oversized entry's tail segments per position (SCYLLADB-3986).
future<db::rp_handle> write_raft_batch(db::commitlog& cl, table_id table,
        raft::group_id group_id, raft::index_t commit_idx, const raft::log_entry_ptr_list& entries);

// Split `entries` into consecutive batches, each of which write_raft_batch() can
// write as one commitlog entry, and return each batch's end offset. A batch that
// already fits returns a single offset without measuring its entries one by one.
std::vector<size_t> split_raft_batch(const db::commitlog& cl, raft::group_id group_id,
        raft::index_t commit_idx, const raft::log_entry_ptr_list& entries);

// Fold a written batch into `segment_queue`, extending the newest record or
// starting a new one when the batch landed in a segment new to the group.
void account_batch(std::deque<segment_record>& segment_queue, const db::cf_id_type& raft_groups_table_id,
        db::rp_handle&& handle, std::span<const raft::log_entry_ptr> entries);

// One group's raft log in the commitlog: which segments hold it, what index range
// each holds, which references keep them, and what a truncation superseded.
// raft_groups_storage owns the group's row and the raft::persistence interface,
// and asks raft_commitlog what may be released.
//
// A lifetime of a segment containing a SC record:
//
//  WRITE        store_log_entries(entries, commit_idx)
//    |            `- write_raft_batch --> commitlog.add(force_sync=yes)
//    |                 one batch = one commitlog entry = ONE position
//    v            `- account_batch --> new record, or extend the newest
//  +------------------------------------------------------------------+
//  | segment held by: pin_user_table + pin_raft_groups                |
//  +------------------------------------------------------------------+
//    |
//    +- COMMIT  store_commit_idx(i) --> _commit_index --> maybe_release()
//    |            no IO at all
//    |
//    +- APPLY   state_machine::apply()
//    |            pin_for_apply(idx) = pin_user_table.clone(tablet table)
//    |            --> the clone lands in the TABLET TABLE's memtable
//    |            note_applied(idx) --> _apply_index --> maybe_release()
//    |
//    +- CLOSE   commitlog round --> groups_manager handler
//    |            --> mark_segment_closed(pos) --> maybe_release()
//    v
//  RELEASE GATE  front_releasable(commit_idx, apply_idx)
//  +------------------------------------------------------------------+
//  | final      size()>1  OR  rp <= closed_up_to()                    |
//  | committed  max_index <= commit_idx                               |
//  | applied    !last_cmd()  OR  *last_cmd() <= apply_idx             |
//  +------------------------------------------------------------------+
//    | all three
//    v
//  WRITE DESCRIPTOR  mutation --> system.raft_groups memtable
//    |                 snapshot_idx  = max_index
//    |                 snapshot_term = max_term()
//    |                 snapshot_config, truncations
//    |                 carrying pin_raft_groups  <== MOVED IN
//    v
//  pop_released()    record destroyed, pin_user_table dropped
//  +------------------------------------------------------------------+
//  | segment now held ONLY by memtable contents:                      |
//  |   tablet table  <- the per-command clones                        |
//  |   raft_groups   <- the descriptor mutation's handle              |
//  +------------------------------------------------------------------+
//    |
//    +- round already ran?  --yes--> _request_flush(seg+1:0) for raft_groups,
//    |                               and the tablet table if the record held
//    |                               any command
//    |                      --no--> the round ahead covers it
//    v
//  BOTH MEMTABLES FLUSH --> refcount 0 --> SEGMENT RECLAIMED
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
    // Furthest flush-round position reported to us, via mark_segment_closed(); a
    // round is the commitlog asking a segment's dirty tables to flush (see
    // db::commitlog::flush_position). Only half of closed_up_to().
    db::replay_position _reported_up_to;
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

    // Another reference to the segment holding `idx`, accounted to the tablet's
    // table, for the memtable the applied mutation goes into. It sits at the
    // record's first batch; retention counts per segment, so the count is exact.
    // Aborts if no record holds `idx`.
    db::rp_handle pin_for_apply(raft::index_t idx);

    // Report a flush round's position: everything at or below it is in a closed
    // segment. One of the two inputs of closed_up_to().
    void mark_segment_closed(db::replay_position pos);

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

    // How far the commitlog's flush rounds have got: a record at or below the
    // returned position has had its segment's round. _reported_up_to and
    // flush_position() are both needed: _reported_up_to carries the round now
    // running, whose handler runs before flush_position() advances, and
    // flush_position() carries rounds whose handler never reached this group,
    // as in the boot window.
    //
    // A round reaching a record means the record's segment stopped allocating, so
    // nothing more of this group can land there. Two commitlog details carry that:
    //
    // flush_position() is the start of the still-allocating segment while one
    // exists, so it names a live segment. A record is never at offset 0, because a
    // segment opens with its descriptor header, so no record of a live segment
    // compares at or below it.
    //
    // The max-data-lifetime sweep reports a segment it closes in the same pass,
    // discarding the close's future. segment::close() sets _closed before its first
    // co_await, so is_still_allocating() is already false when the round is
    // reported; only the file work is deferred.
    db::replay_position closed_up_to() const {
        return std::max(_reported_up_to, _commit_log.flush_position());
    }
};

} // namespace service::strong_consistency
