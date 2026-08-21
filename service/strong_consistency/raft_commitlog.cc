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
}

raft_commitlog::raft_commitlog(raft::group_id group_id, db::commitlog& commitlog, table_id target_table_id, replayed_data_per_group replayed_data)
    : _group_id(group_id)
    , _table_id(target_table_id)
    , _commit_log(commitlog)
    , _replay_positions(std::move(replayed_data.replay_positions))
    , _replayed_entries(std::move(replayed_data.entries)) {
    logger.debug("starting raft_commitlog group_id={}, table_id={}, replayed_entries={}", _group_id, _table_id, _replayed_entries.size());
}

raft_commitlog::~raft_commitlog() {
    // Release all remaining replay position handles without decrementing
    // segment dirty counts. This keeps commitlog segments alive after this
    // object is destroyed, ensuring uncommitted raft entries remain
    // available for replay after restart.
    for (auto& entry : _replay_positions) {
        entry.replay_position_handle.release();
    }
    logger.debug("released {} replay position handles for group_id={}", _replay_positions.size(), _group_id);
}

seastar::future<> raft_commitlog::store_log_entries(const raft::log_entry_ptr_list& entries) {
    logger.debug("store_log_entries: group_id={}, num_entries={}", _group_id, entries.size());

    utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
    writers.reserve(entries.size());

    for (const auto& log_entry_ptr : entries) {
        logger.debug("  storing log entry: idx={}, term={}", log_entry_ptr->idx, log_entry_ptr->term);
        writers.emplace_back(raft_commitlog_entry{.group_id = _group_id, .entry = log_entry_ptr});
    }

    auto replay_handles = co_await _commit_log.add_raft_entries(_table_id, std::move(writers));

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& log_entry_ptr = entries[i];
        _replay_positions.push_back(
                index_and_replay_position{.index = log_entry_ptr->idx, .replay_position_handle = std::move(replay_handles[i])});
    }
    logger.debug("store_log_entries completed: total_entries_in_map={}", _replay_positions.size());
}

raft::log_entries raft_commitlog::load_log() {
    logger.debug("load_log: group_id={}", _group_id);
    return std::move(_replayed_entries);
}

void raft_commitlog::truncate_log(const raft::index_t idx) {
    logger.debug("truncate_log: group_id={}, idx={}", _group_id, idx);
    // Remove entries with index >= idx (Raft semantics: truncate from idx onward).
    // The deque is sorted by index, so binary search finds the cut point.
    auto it = std::ranges::lower_bound(_replay_positions, idx, {}, &index_and_replay_position::index);
    _replay_positions.erase(it, _replay_positions.end());
}

void raft_commitlog::truncate_log_tail(const raft::index_t index) {
    logger.debug("truncate_log_tail: group_id={}, index={}", _group_id, index);
    // Remove entries with index <= the given index. The handles are destructed
    // normally, decrementing segment dirty counts and allowing commitlog
    // segments to be reclaimed once no other references hold them.
    auto it = std::ranges::upper_bound(_replay_positions, index, {}, &index_and_replay_position::index);
    _replay_positions.erase(_replay_positions.begin(), it);
    logger.debug("truncate_log_tail completed: remaining_map_size={}", _replay_positions.size());
}

std::vector<index_and_replay_position> raft_commitlog::acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries) {
    logger.debug("acquire_replay_position_handles_for: group_id={}, entries_count={}, current_map_size={}", _group_id, entries.size(),
            _replay_positions.size());

    std::vector<index_and_replay_position> ret;
    ret.reserve(entries.size());

    // Move replay position handles for requested entries. The entries may not be
    // contiguous in _replay_positions because non-command entries (configuration,
    // dummy) are also tracked there but are not passed to state_machine::apply().
    // We scan forward through _replay_positions, skipping non-matching entries
    // (which are the non-command items). This is O(n) since both sequences are
    // sorted by index and we only move forward.
    auto it = _replay_positions.begin();
    for (const auto& entry : entries) {
        // Skip non-command entries that have indices below the one we're looking for.
        while (it != _replay_positions.end() && it->index < entry->idx) {
            ++it;
        }
        if (it == _replay_positions.end() || it->index != entry->idx) {
            on_internal_error(logger, fmt::format("missing replay position handle for group_id={}, idx={}", _group_id, entry->idx));
        }
        ret.emplace_back(index_and_replay_position{.index = it->index, .replay_position_handle = std::move(it->replay_position_handle)});
        it = _replay_positions.erase(it);
    }

    logger.debug(
            "acquire_replay_position_handles_for completed: returned_count={}, remaining_map_size={}", ret.size(), _replay_positions.size());
    return ret;
}
} // namespace service::strong_consistency
