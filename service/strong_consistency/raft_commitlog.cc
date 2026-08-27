/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/on_internal_error.hh>
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "raft/raft.hh"

#include "raft_commitlog.hh"

#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"

namespace service::strong_consistency {
namespace {
seastar::logger logger("raft_commitlog");

// Command entries are the ones state_machine::apply() consumes; their
// handles move to the target table's memtable there.
bool is_command_entry(const raft::log_entry& e) {
    return std::holds_alternative<raft::command>(e.data);
}

// Configuration entries carry state that is only made durable by
// store_snapshot_descriptor(), so their handles follow a different release
// rule than commands and dummies (see SCYLLADB-3842).
bool is_config_entry(const raft::log_entry& e) {
    return std::holds_alternative<raft::configuration>(e.data);
}
} // namespace

raft_commitlog::raft_commitlog(raft::group_id group_id, db::commitlog& commitlog, table_id target_table_id,
        db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data)
    : _group_id(group_id)
    , _table_id(target_table_id)
    , _raft_groups_table_id(raft_groups_table_id)
    , _commit_log(commitlog)
    , _replayed_entries(std::move(replayed_data.entries)) {
    for (const auto& e : _replayed_entries) {
        _appended_terms.push_back(raft_term_and_index{.idx = e->idx, .term = e->term});
    }
    // Classify each replay position as config / non-config by the entry it
    // belongs to. Replay positions are a subset of the replayed entries (only
    // uncommitted entries are rewritten to the new commitlog on replay) and
    // both are in raft-index order, so we match them by index with a single
    // forward walk over _replayed_entries.
    auto entry_it = _replayed_entries.begin();
    for (auto& pos : replayed_data.replay_positions) {
        while (entry_it != _replayed_entries.end() && (*entry_it)->idx != pos.index) {
            ++entry_it;
        }
        SCYLLA_ASSERT(entry_it != _replayed_entries.end());
        auto& target = is_command_entry(**entry_it) ? _command_positions
                : is_config_entry(**entry_it) ? _config_positions
                : _dummy_positions;
        target.push_back(std::move(pos));
    }
    logger.debug("starting raft_commitlog group_id={}, table_id={}, replayed_entries={}, "
            "command_positions={}, dummy_positions={}, config_positions={}",
            _group_id, _table_id, _replayed_entries.size(), _command_positions.size(),
            _dummy_positions.size(), _config_positions.size());
}

raft_commitlog::~raft_commitlog() {
    // Release all remaining replay position handles without decrementing
    // segment dirty counts. This keeps commitlog segments alive after this
    // object is destroyed, ensuring uncommitted raft entries remain
    // available for replay after restart.
    for (auto& entry : _command_positions) {
        entry.replay_position_handle.release();
    }
    for (auto& entry : _dummy_positions) {
        entry.replay_position_handle.release();
    }
    for (auto& entry : _config_positions) {
        entry.replay_position_handle.release();
    }
    logger.debug("released {} command + {} dummy + {} config replay position handles for group_id={}",
            _command_positions.size(), _dummy_positions.size(), _config_positions.size(), _group_id);
}

seastar::future<utils::chunked_vector<db::rp_handle>> raft_commitlog::write_batches(db::commitlog& cl,
        db::cf_id_type table_id, db::cf_id_type raft_groups_table_id, raft::group_id group_id,
        const raft::log_entry_ptr_list& entries, raft_term_and_index committed, pending_cover_map& covers) {
    utils::chunked_vector<db::rp_handle> entry_handles;
    if (entries.empty()) {
        co_return entry_handles;
    }

    // One commitlog entry for the whole batch: the group id, the entries, and
    // the group's last committed (index, term) as the crash-replay floor. The
    // per-entry framing this replaces repeated the group id and an envelope
    // for every entry, and paid for a separate trailing record whose only
    // novel payload was one index.
    utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
    writers.emplace_back(raft_commitlog_entry{
            .group_id = group_id,
            .entries = {entries.begin(), entries.end()},
            .commit_idx = committed.idx,
            .commit_idx_term = committed.term});
    if (logger.is_enabled(seastar::log_level::debug)) {
        logger.debug("  storing batch: group_id={}, idx={}..{}, committed=({}, {})", group_id,
                entries.front()->idx, entries.back()->idx, committed.idx, committed.term);
    }

    // A single write, force_sync. Below the commitlog's oversized threshold
    // (half a segment: 32MB at the default 64MB segment size) the batch is
    // placed atomically in one segment. Raft caps a batch at max_log_size
    // (20MB for tablet groups), so at the default segment size the threshold
    // is unreachable; with smaller configured segments the commitlog
    // fragments the entry across segments, which requires
    // allow_fragmented_entries (the fragmented_commitlog_entries cluster
    // feature; strongly consistent tables are gated behind an experimental
    // flag, so in practice it is on). Either way the batch has exactly one
    // position — its head segment — and the tail segments of a fragmented
    // entry are held by the owner count on that position
    // (segment::extended_entry), so they outlive every claim taken below.
    auto handles = co_await cl.add_raft_entries(table_id, std::move(writers));
    SCYLLA_ASSERT(handles.size() == 1);
    auto& batch_handle = handles[0];

    // One claim per entry, all at the batch's position, so each entry's
    // lifetime is tracked separately even though they share a record: a
    // command's claim moves to the target table's memtable when apply()
    // consumes it, while a dummy's or a configuration's is released once the
    // batch is covered. Taken from the batch's own claim, which is still held
    // here — that is what makes them safe to take at all.
    entry_handles.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        entry_handles.emplace_back(cl.acquire_cf_count(batch_handle, table_id));
    }

    // The segment must also be held on behalf of system.raft_groups: without
    // that, a target-table flush could reclaim it before a covering commit_idx
    // value is durable. One claim per segment, not per batch — a segment
    // already in the caller's cover map keeps the claim it has (it has existed
    // continuously since the batch that first wrote to it, which is the
    // property the coverage relies on) and only its (max_idx, max_term) pair
    // advances. Batches keep landing in the same still-active segment, so that
    // is the common case.
    //
    // Entries ascend, so the batch's last entry carries the highest index the
    // group has put in this segment.
    const auto segment = batch_handle.rp().id;
    auto [it, inserted] = covers.try_emplace(segment);
    it->second.max_idx = entries.back()->idx;
    it->second.max_term = entries.back()->term;
    if (inserted) {
        it->second.segment_holder = cl.acquire_cf_count(batch_handle, raft_groups_table_id);
    }

    // batch_handle goes out of scope here: the write's own claim is replaced by
    // the per-entry claims and the cover's, which have their own lifetimes.
    co_return entry_handles;
}

seastar::future<> raft_commitlog::store_log_entries(const raft::log_entry_ptr_list& entries) {
    logger.debug("store_log_entries: group_id={}, num_entries={}", _group_id, entries.size());

    // The cover map is local for now: the claims it collects are released as
    // soon as this call returns (the batch's own entry handles keep the
    // segments alive), and commit_idx is still persisted by
    // store_commit_idx()'s CQL write. A later commit keeps the map across
    // batches and hands the claims to covering raft_groups mutations.
    pending_cover_map covers;
    auto entry_handles = co_await write_batches(_commit_log, _table_id, _raft_groups_table_id, _group_id, entries,
            _last_committed, covers);
    SCYLLA_ASSERT(entry_handles.size() == entries.size());

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& log_entry_ptr = entries[i];
        auto& target = is_command_entry(*log_entry_ptr) ? _command_positions
                : is_config_entry(*log_entry_ptr) ? _config_positions
                : _dummy_positions;
        target.push_back(
                index_and_replay_position{.index = log_entry_ptr->idx, .replay_position_handle = std::move(entry_handles[i])});
        _appended_terms.push_back(raft_term_and_index{.idx = log_entry_ptr->idx, .term = log_entry_ptr->term});
    }
    logger.debug("store_log_entries completed: command_positions={}, dummy_positions={}, config_positions={}",
            _command_positions.size(), _dummy_positions.size(), _config_positions.size());
}

void raft_commitlog::note_commit_idx(const raft::index_t idx) {
    // Consume the appended entries the commit index has reached; the last one
    // consumed is the pair the next batch records as its floor. Raft advances
    // the commit index before the entries are applied, so every entry at or
    // below idx is still tracked here — no lookup can miss.
    while (!_appended_terms.empty() && _appended_terms.front().idx <= idx) {
        _last_committed = _appended_terms.front();
        _appended_terms.pop_front();
    }
}

raft::log_entries raft_commitlog::load_log() {
    logger.debug("load_log: group_id={}", _group_id);
    return std::move(_replayed_entries);
}

void raft_commitlog::truncate_log(const raft::index_t idx) {
    logger.debug("truncate_log: group_id={}, idx={}", _group_id, idx);
    // Remove entries with index >= idx (Raft semantics: truncate from idx onward).
    // The deque is sorted by index, so binary search finds the cut point.
    auto trim = [idx] (replay_position_list& l) {
        auto it = std::ranges::lower_bound(l, idx, {}, &index_and_replay_position::index);
        l.erase(it, l.end());
    };
    trim(_command_positions);
    trim(_dummy_positions);
    trim(_config_positions);
    // The discarded entries can no longer be committed, so their terms must
    // not end up in a floor record.
    while (!_appended_terms.empty() && _appended_terms.back().idx >= idx) {
        _appended_terms.pop_back();
    }
}

void raft_commitlog::truncate_log_tail(const raft::index_t index) {
    logger.debug("truncate_log_tail: group_id={}, index={}", _group_id, index);
    // Remove entries with index <= the given index. The handles are destructed
    // normally, decrementing segment dirty counts and allowing commitlog
    // segments to be reclaimed once no other references hold them.
    auto it = std::ranges::upper_bound(_command_positions, index, {}, &index_and_replay_position::index);
    _command_positions.erase(_command_positions.begin(), it);
    auto dummy_it = std::ranges::upper_bound(_dummy_positions, index, {}, &index_and_replay_position::index);
    _dummy_positions.erase(_dummy_positions.begin(), dummy_it);
    // Configuration handles are released here and only here: the snapshot whose
    // descriptor we just persisted carries the configuration, so it is durable
    // now and the segments holding those entries can be reclaimed.
    auto cfg_it = std::ranges::upper_bound(_config_positions, index, {}, &index_and_replay_position::index);
    _config_positions.erase(_config_positions.begin(), cfg_it);
    logger.debug("truncate_log_tail completed: command_positions={}, dummy_positions={}, config_positions={}",
            _command_positions.size(), _dummy_positions.size(), _config_positions.size());
}

std::vector<index_and_replay_position> raft_commitlog::acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries) {
    logger.debug("acquire_replay_position_handles_for: group_id={}, entries_count={}, current_command_size={}",
            _group_id, entries.size(), _command_positions.size());

    std::vector<index_and_replay_position> ret;
    ret.reserve(entries.size());

    // Move replay position handles for requested entries. Both sequences are
    // command-only and index-sorted; scan forward matching them up.
    auto it = _command_positions.begin();
    for (const auto& entry : entries) {
        while (it != _command_positions.end() && it->index < entry->idx) {
            ++it;
        }
        if (it == _command_positions.end() || it->index != entry->idx) {
            on_internal_error(logger, fmt::format("missing replay position handle for group_id={}, idx={}", _group_id, entry->idx));
        }
        ret.emplace_back(index_and_replay_position{.index = it->index, .replay_position_handle = std::move(it->replay_position_handle)});
        it = _command_positions.erase(it);
    }

    logger.debug(
            "acquire_replay_position_handles_for completed: returned_count={}, remaining_command_size={}", ret.size(), _command_positions.size());
    return ret;
}
} // namespace service::strong_consistency
