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

// A raft log position: an entry's index and the term it was appended in.
struct raft_term_and_index {
    raft::index_t idx{0};
    raft::term_t term{0};
};

// One truncation, as it is persisted in system.raft_groups.truncations:
// "in this segment, the then-current copies of indexes [from, to] were
// truncated". truncate_log() removes entries from the raft log but not from
// disk, so an index can have several copies on disk and only the last one
// written is current; replay uses these records to drop the superseded ones
// without having to reason about terms.
struct truncation_record {
    db::segment_id_type segment{0};
    raft::index_t from{0};
    raft::index_t to{0};

    bool operator==(const truncation_record&) const = default;
};

// What one group remembers about its own entries in one commitlog segment.
//
// One record per segment rather than per batch: batches keep landing in the
// same still-allocating segment, and retention is accounted per segment, so a
// later batch in the same segment only advances `max`.
//
// The record is the unit of release: once every index it covers is committed
// and (for commands) applied, its (max, term) pair becomes the group's
// persisted snapshot descriptor and the segment may go.
struct segment_record {
    // The group's own reference to the segment. It must outlive every entry
    // here that is not yet both committed and applied, and it is the source of
    // the per-command references handed to the target table's memtable.
    db::rp_handle pin_table;
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
    // At release it moves into the mutation that persists the descriptor, so the
    // segment then lives exactly until the raft_groups memtable flush that makes
    // that value durable.
    db::rp_handle pin_rg;
    // Index range of this group's entries in the segment. Both ends are
    // inclusive; `max` advances as later batches land in the same segment.
    raft::index_t first{0};
    raft::index_t max{0};
    // (index, term) runs, ascending: the term of the entry at any index in
    // [first, max] is the term of the last run whose index is at or below it.
    // One element per term change, so one per leader change rather than per
    // entry.
    utils::chunked_vector<raft_term_and_index> terms;
    // Configurations this record's entries carried, in index order.
    utils::chunked_vector<std::pair<raft::index_t, raft::configuration>> configs;
    // Indexes of the dummy and configuration entries. state_machine::apply()
    // never sees those, so the applied index never reaches them, and the
    // release gate must wait for the last *command* rather than for `max`
    // (SCYLLADB-2375).
    utils::chunked_vector<raft::index_t> noncmd;

    db::segment_id_type segment() const {
        return pin_table.rp().id;
    }

    // Term of the entry at `max` — the term that goes with the index this
    // record would persist.
    raft::term_t max_term() const {
        return terms.empty() ? raft::term_t{0} : terms.back().term;
    }

    // Highest index in [first, max] that is a command, if any. Disengaged for a
    // record holding nothing but dummies and configurations, which nothing will
    // ever apply.
    std::optional<raft::index_t> last_cmd() const;

    // The newest configuration this record carried, if any.
    std::optional<std::pair<raft::index_t, raft::configuration>> last_conf() const {
        if (configs.empty()) {
            return std::nullopt;
        }
        return configs.back();
    }

    // Drop everything at or above `idx` after a truncation, and clamp `max`.
    void trim_from(raft::index_t idx);

    // Give up both references without decrementing, so the segments survive
    // this record. For teardown paths only: the entries are still needed, and
    // replay is what recovers them. Destroying a record instead decrements,
    // which is what the release path wants — see release_all().
    void detach() {
        pin_table.release();
        pin_rg.release();
    }
};

// What commitlog replay hands to a group when it starts: the rewritten
// uncommitted entries, and the records for the batch they were rewritten as —
// the queue the group starts with.
struct replayed_data_per_group {
    std::deque<segment_record> records;
    raft::log_entries entries;
};

// Write `entries` to `cl` under `table` as one raft batch carrying `commit_idx`
// in its header, and return the reference that write produced.
//
// The batch must fit in one commitlog entry. An entry over max_record_size()
// would be fragmented across segments, breaking the rule that a copy of an
// entry lives in exactly one segment — the rule the segment records and the
// truncation records are built on — so a batch that does not fit is an internal
// error, not something to split.
//
// What bounds a batch: raft admits no command over max_command_size, and fsm's
// log_limiter_semaphore holds a leader's accounted log at max_log_size, of which
// a batch is a subset (see groups_manager's raft::server configuration). Their sum clears
// a default 64MB segment's 32MB max_record_size(), but not a much smaller
// configured segment, so the assert is reachable. Splitting such a batch into
// several whole batches would be a legal fix; fragmenting one entry would not
// (SCYLLADB-3986).
//
// Static so that commitlog replay can write the same format when it rewrites a
// group's uncommitted entries.
future<db::rp_handle> write_raft_batch(db::commitlog& cl, table_id table,
        raft::group_id group_id, raft::index_t commit_idx, const raft::log_entry_ptr_list& entries);

// Fold a written batch into `queue`, creating a record when it landed in a
// segment the group has not written to yet and extending the last record
// otherwise.
void account_batch(std::deque<segment_record>& queue, const db::cf_id_type& raft_groups_table_id,
        db::rp_handle&& handle, std::span<const raft::log_entry_ptr> entries);

// One group's raft log in the commitlog, and everything the group knows about
// the segments holding it.
//
// This is the only place that knows a group's entries live in commitlog
// segments: which segments they are, what index range each holds, which
// references keep them, and which indexes a truncation superseded.
// raft_groups_storage owns the group's row and the raft::persistence interface,
// and asks this class what may be released; it does not itself reason about
// segments.
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
    // Truncation history as it is persisted in the row. Not globally ordered by
    // segment: one truncate_log() call pops the queue from the back, so it
    // appends the newest segment's record first. Order within one segment is
    // still the order the truncations happened, which is all replay's per-segment
    // cursors need. Records naming segments the commitlog no longer has are
    // dropped.
    std::vector<truncation_record> _truncations;
    // How far the commitlog has closed segments on this shard.
    //
    // This is what lets the *newest* record be released. Every other record
    // learns its segment closed from a later record existing in the queue, but
    // the newest one has no successor: a group that stops writing would hold its
    // last segment indefinitely. groups_manager feeds this from the commitlog's
    // flush rounds, which name only closed segments, so a position at or above a
    // record's own says that record's segment is closed.
    db::replay_position _closed_up_to;
    // The log entries commitlog replay recovered for this group.
    raft::log_entries _replayed_entries;

public:
    raft_commitlog(raft::group_id group_id, db::commitlog& commit_log, table_id target_table_id,
        db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data);
    ~raft_commitlog();

    // Write the entries as one batch (see write_raft_batch) carrying
    // `commit_idx` in the header, and account the result to the segment records.
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries, raft::index_t commit_idx);

    // Discard the entries at or above `idx`: the records they were the whole of
    // go away, the one the point lands inside is clamped, and in both cases a
    // truncation_record remembers what was superseded.
    void truncate_log(raft::index_t idx);

    // Take a reference for the command at `idx` from the record that holds it.
    db::rp_handle pin_for_apply(raft::index_t idx);

    // Report the commitlog's flush position: everything at or below it is in a
    // closed segment. Backstop only — see _closed_up_to.
    void mark_segment_closed(db::replay_position pos);

    // The oldest record that may now be released, or nullptr. A record is
    // releasable once no more of this group's entries can land in its segment,
    // every index it holds is committed, and every command in it applied.
    // Ownership stays here: the caller persists the descriptor and then calls
    // pop_released(), so a failed write leaves the record in place.
    segment_record* front_releasable(raft::index_t commit_idx, raft::index_t apply_idx);
    void pop_released();

    // Seed the truncation history from the group's row, once, before it starts.
    // A release re-writes the whole list, so what was persisted by a previous
    // run has to be here or it would be dropped.
    void seed_truncations(std::vector<truncation_record> truncations) {
        _truncations = std::move(truncations);
    }

    // Drop the truncation records whose segment the commitlog no longer has:
    // once it cannot hand out a position that low, no replay can see those
    // copies again.
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
