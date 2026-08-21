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

// Configuration entries are non-command entries that carry state, so their
// handles follow a different release rule than dummies (see SCYLLADB-3842).
bool is_config_entry(const raft::log_entry& e) {
    return std::holds_alternative<raft::configuration>(e.data);
}
} // namespace

raft_commitlog::raft_commitlog(raft::group_id group_id, db::commitlog& commitlog, table_id target_table_id, replayed_data_per_group replayed_data)
    : _group_id(group_id)
    , _table_id(target_table_id)
    , _commit_log(commitlog)
    , _replayed_entries(std::move(replayed_data.entries)) {
    // Classify each replay position as command / non-command by the entry it
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
        const raft::log_entry& e = **entry_it;
        auto& target = is_command_entry(e) ? _command_positions
                : is_config_entry(e) ? _config_positions
                : _dummy_positions;
        target.push_back(std::move(pos));
    }
    logger.debug("starting raft_commitlog group_id={}, table_id={}, replayed_entries={}, "
            "command_positions={}, dummy_positions={}, config_positions={}", _group_id, _table_id,
            _replayed_entries.size(), _command_positions.size(), _dummy_positions.size(),
            _config_positions.size());
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

seastar::future<> raft_commitlog::store_log_entries(const raft::log_entry_ptr_list& entries) {
    logger.debug("store_log_entries: group_id={}, num_entries={}", _group_id, entries.size());

    utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
    writers.reserve(entries.size());

    for (const auto& log_entry_ptr : entries) {
        logger.debug("  storing log entry: idx={}, term={}", log_entry_ptr->idx, log_entry_ptr->term);
        writers.emplace_back(_table_id, raft_commitlog_entry{.group_id = _group_id, .entry = log_entry_ptr});
    }

    auto replay_handles = co_await _commit_log.add_raft_entries(std::move(writers));

    for (size_t i = 0; i < entries.size(); ++i) {
        const auto& log_entry_ptr = entries[i];
        auto& target = is_command_entry(*log_entry_ptr) ? _command_positions
                : is_config_entry(*log_entry_ptr) ? _config_positions
                : _dummy_positions;
        target.push_back(
                index_and_replay_position{.index = log_entry_ptr->idx, .replay_position_handle = std::move(replay_handles[i])});
    }
    logger.debug("store_log_entries completed: total_command={}, total_dummy={}, total_config={}",
            _command_positions.size(), _dummy_positions.size(), _config_positions.size());
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
    trim(_dummy_positions);
    trim(_config_positions);
}

void raft_commitlog::truncate_log_tail(const raft::index_t index) {
    logger.debug("truncate_log_tail: group_id={}, index={}", _group_id, index);
    // Remove entries with index <= the given index. The handles are destructed
    // normally, decrementing segment dirty counts and allowing commitlog
    // segments to be reclaimed once no other references hold them.
    auto it = std::ranges::upper_bound(_command_positions, index, {}, &index_and_replay_position::index);
    _command_positions.erase(_command_positions.begin(), it);
    // Dummies follow the same rule; share the impl.
    release_dummy_rp_handles(index);
    // Configuration handles are released here and only here: the snapshot whose
    // descriptor we just persisted carries the configuration, so it is durable
    // now and the segments holding those entries can be reclaimed.
    auto cfg_it = std::ranges::upper_bound(_config_positions, index, {}, &index_and_replay_position::index);
    _config_positions.erase(_config_positions.begin(), cfg_it);
    logger.debug("truncate_log_tail completed: command={}, dummy={}, config={}",
            _command_positions.size(), _dummy_positions.size(), _config_positions.size());
}

void raft_commitlog::release_dummy_rp_handles(const raft::index_t index) {
    auto it = std::ranges::upper_bound(_dummy_positions, index, {}, &index_and_replay_position::index);
    _dummy_positions.erase(_dummy_positions.begin(), it);
    logger.debug("release_dummy_rp_handles: group_id={}, up_to_idx={}, remaining_dummy={}, retained_config={}",
            _group_id, index, _dummy_positions.size(), _config_positions.size());
}

std::vector<index_and_replay_position> raft_commitlog::acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries) {
    logger.debug("acquire_replay_position_handles_for: group_id={}, entries_count={}, current_command_size={}",
            _group_id, entries.size(), _command_positions.size());

    std::vector<index_and_replay_position> ret;
    ret.reserve(entries.size());

    // Command entries are already contiguous in _command_positions: non-command
    // entries have their own deque and are not passed to apply(). Scan forward.
    auto it = _command_positions.begin();
    for (const auto& entry : entries) {
        // The raft applier fiber only hands command entries to apply(); a
        // non-command entry here would indicate a raft bug.
        if (!is_command_entry(*entry)) {
            on_internal_error(logger, fmt::format(
                    "acquire_replay_position_handles_for: unexpected non-command entry for group_id={}, idx={}",
                    _group_id, entry->idx));
        }
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
            "acquire_replay_position_handles_for completed: returned_count={}, remaining_command_size={}",
            ret.size(), _command_positions.size());
    return ret;
}
} // namespace service::strong_consistency
