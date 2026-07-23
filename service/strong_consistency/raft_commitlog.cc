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

// Command entries are the ones state_machine::apply() will consume.
// Everything else (configuration, dummy) is a "non-command" entry.
bool is_command_entry(const raft::log_entry& e) {
    return std::holds_alternative<raft::command>(e.data);
}
} // namespace

raft_commitlog::raft_commitlog(raft::group_id group_id, db::commitlog& commitlog, table_id target_table_id,
        db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data)
    : _group_id(group_id)
    , _table_id(target_table_id)
    , _raft_groups_table_id(raft_groups_table_id)
    , _commit_log(commitlog)
    , _replayed_entries(std::move(replayed_data.entries)) {
    // replay_positions and _replayed_entries are populated in lockstep by the
    // replay buffer rewrite loop (see db/commitlog/raft_commitlog_replay_buffer.cc),
    // so their i-th elements correspond by construction.
    SCYLLA_ASSERT(replayed_data.replay_positions.size() == _replayed_entries.size());
    for (size_t i = 0; i < replayed_data.replay_positions.size(); ++i) {
        const bool cmd = is_command_entry(*_replayed_entries[i]);
        (cmd ? _command_positions : _noncommand_positions).push_back(std::move(replayed_data.replay_positions[i]));
    }
    logger.debug("starting raft_commitlog group_id={}, table_id={}, replayed_entries={}, "
            "command_positions={}, noncommand_positions={}", _group_id, _table_id,
            _replayed_entries.size(), _command_positions.size(), _noncommand_positions.size());
}

raft_commitlog::~raft_commitlog() {
    // Release all remaining replay position handles without decrementing
    // segment dirty counts. This keeps commitlog segments alive after this
    // object is destroyed, ensuring uncommitted raft entries remain
    // available for replay after restart.
    for (auto& entry : _command_positions) {
        entry.replay_position_handle.release();
    }
    for (auto& entry : _noncommand_positions) {
        entry.replay_position_handle.release();
    }
    logger.debug("released {} command + {} non-command replay position handles for group_id={}",
            _command_positions.size(), _noncommand_positions.size(), _group_id);
}

seastar::future<db::rp_handle> raft_commitlog::store_log_entries(const raft::log_entry_ptr_list& entries, raft::index_t commit_idx) {
    logger.debug("store_log_entries: group_id={}, num_entries={}, commit_idx={}",
            _group_id, entries.size(), commit_idx);

    utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
    writers.reserve(entries.size() + 1);

    for (const auto& log_entry_ptr : entries) {
        logger.debug("  storing log entry: idx={}, term={}", log_entry_ptr->idx, log_entry_ptr->term);
        writers.emplace_back(_table_id, raft_commitlog_entry{.group_id = _group_id, .entry = log_entry_ptr});
    }
    // Trailing commit_idx entry. Its rp_handle is returned to the caller so
    // it can be attached to a fake system.raft_groups mutation applied
    // in-memory. That lets the raft_groups memtable eventually flush commit_idx
    // to an SSTable via a normal flush without a synchronous CQL write on the
    // raft io_fiber. The entry's cf_id is _raft_groups_table_id
    // (system.raft_groups) so segment dirty-count accounting stays consistent
    // with the memtable that ends up holding the handle.
    writers.emplace_back(
            _raft_groups_table_id,
            raft_commit_idx_entry{.group_id = _group_id, .commit_idx = commit_idx});

    auto replay_handles = co_await _commit_log.add_raft_entries(std::move(writers));
    // We passed N raft entries + 1 commit_idx entry; add_raft_entries preserves order.
    SCYLLA_ASSERT(replay_handles.size() == entries.size() + 1);

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& log_entry_ptr = entries[i];
        auto& target = is_command_entry(*log_entry_ptr) ? _command_positions : _noncommand_positions;
        target.push_back(
                index_and_replay_position{.index = log_entry_ptr->idx, .replay_position_handle = std::move(replay_handles[i])});
    }
    logger.debug("store_log_entries completed: total_command={}, total_noncommand={}",
            _command_positions.size(), _noncommand_positions.size());
    co_return std::move(replay_handles.back());
}

raft::log_entries raft_commitlog::load_log() {
    logger.debug("load_log: group_id={}", _group_id);
    return std::move(_replayed_entries);
}

void raft_commitlog::truncate_log(const raft::index_t idx) {
    logger.debug("truncate_log: group_id={}, idx={}", _group_id, idx);
    // Remove entries with index >= idx (Raft semantics: truncate from idx onward).
    // Both deques are sorted by index, so binary search finds the cut point.
    auto trim = [idx] (replay_position_list& l) {
        auto it = std::ranges::lower_bound(l, idx, {}, &index_and_replay_position::index);
        l.erase(it, l.end());
    };
    trim(_command_positions);
    trim(_noncommand_positions);
}

void raft_commitlog::truncate_log_tail(const raft::index_t index) {
    logger.debug("truncate_log_tail: group_id={}, index={}", _group_id, index);
    // Remove entries with index <= the given index. The handles are destructed
    // normally, decrementing segment dirty counts and allowing commitlog
    // segments to be reclaimed once no other references hold them.
    auto it = std::ranges::upper_bound(_command_positions, index, {}, &index_and_replay_position::index);
    _command_positions.erase(_command_positions.begin(), it);
    // Non-command handles are trimmed by the same rule; share the impl.
    release_noncommand_rp_handles(index);
    logger.debug("truncate_log_tail completed: command={}, noncommand={}",
            _command_positions.size(), _noncommand_positions.size());
}

void raft_commitlog::release_noncommand_rp_handles(const raft::index_t index) {
    auto it = std::ranges::upper_bound(_noncommand_positions, index, {}, &index_and_replay_position::index);
    _noncommand_positions.erase(_noncommand_positions.begin(), it);
    logger.debug("release_noncommand_rp_handles: group_id={}, up_to_idx={}, remaining={}",
            _group_id, index, _noncommand_positions.size());
}

std::vector<index_and_replay_position> raft_commitlog::acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries) {
    logger.debug("acquire_replay_position_handles_for: group_id={}, entries_count={}, current_command_size={}",
            _group_id, entries.size(), _command_positions.size());

    if (entries.empty()) {
        return {};
    }

    // The raft applier fiber hands us a contiguous, index-ordered prefix of
    // pending commands. _command_positions holds pending commands in index
    // order, so the requested range must be _command_positions[0..entries.size()).
    if (entries.size() > _command_positions.size()
            || _command_positions.front().index != entries.front()->idx
            || _command_positions[entries.size() - 1].index != entries.back()->idx) {
        on_internal_error(logger, fmt::format(
                "acquire_replay_position_handles_for: expected contiguous command prefix for group_id={}: "
                "requested [{}, {}] ({} entries), pending {} entries starting at {}",
                _group_id, entries.front()->idx, entries.back()->idx, entries.size(),
                _command_positions.size(),
                _command_positions.empty() ? raft::index_t(0) : _command_positions.front().index));
    }

    // Bulk-move the prefix to the result and drop it from _command_positions.
    std::vector<index_and_replay_position> ret;
    ret.reserve(entries.size());
    auto end = _command_positions.begin() + entries.size();
    std::move(_command_positions.begin(), end, std::back_inserter(ret));
    _command_positions.erase(_command_positions.begin(), end);

    logger.debug(
            "acquire_replay_position_handles_for completed: returned_count={}, remaining_command_size={}",
            ret.size(), _command_positions.size());
    return ret;
}
} // namespace service::strong_consistency
