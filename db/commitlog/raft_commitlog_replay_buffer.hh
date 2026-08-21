/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_map>
#include <seastar/core/future.hh>
#include "utils/chunked_vector.hh"
#include "raft/raft.hh"
#include "db/commitlog/replay_position.hh"
#include "service/strong_consistency/raft_commitlog.hh"

namespace db {
class system_keyspace;
namespace raft_buffer_detail {
// Result of checking entry ordering during commitlog replay.
// Used to detect leader changes and out-of-order tails from crash recovery.
enum class entry_ordering_check_result : uint8_t {
    // Entry is in order (idx > last_idx, or first entry)
    in_order,
    // Entry has a smaller or equal index but a strictly higher term — indicates a leader
    // change. All previously collected entries with idx >= this entry's idx should be
    // discarded, as they were uncommitted entries from the old leader that were replaced.
    leader_change,
    // Entry has a smaller or equal index with the same or lower term — we have hit the
    // start of a tail from an older pre-crash commitlog segment that overlaps with entries
    // already processed from a newer segment. All remaining entries should be ignored.
    out_of_order
};

struct raft_term_and_idx {
    raft::term_t term;
    raft::index_t idx;
};

// Check entry ordering relative to the last seen entry during commitlog replay.
//
// Returns:
//   in_order       — entry follows normally; process it.
//   leader_change  — new leader overwrote uncommitted entries; discard collected
//                    entries with idx >= current entry's idx, then process it.
//   out_of_order   — we have reached the tail of a pre-crash (old) commitlog segment
//                    whose entries were already covered by a later (new) segment;
//                    stop processing entirely.
//
// @param current Term and index of the current entry.
// @param last    Term and index of the last accepted entry ({0,0} if none yet).
entry_ordering_check_result check_entry_ordering(raft_term_and_idx current, raft_term_and_idx last);
} // namespace raft_buffer_detail

// Per-shard buffer that collects Raft log entries found in the commitlog
// during replay. Entries are grouped by group_id and later consumed by
// raft_commitlog instances when tablet Raft groups are started.
//
// Terminology:
//   "old commitlog"  — commitlog segment files left on disk from a previous run.
//                      On each startup, ScyllaDB replays these segments to recover
//                      any entries that had not yet been applied to sstables.
//   "new commitlog"  — the fresh commitlog created at the start of the current run.
//                      During recovery, uncommitted entries from the old commitlog are
//                      rewritten here to obtain valid rp_handles that keep the segment
//                      files alive until the data is flushed.
//
//   Importantly, old segment files are only deleted after the memtables they cover
//   have been flushed to sstables. If the node crashes again before that flush
//   completes, the next run will find segment files from *two* previous runs on disk:
//   the "pre-previous" run's segments (which were the old commitlog last time) and
//   the "previous" run's segments (which were the new commitlog last time, containing
//   the rewritten uncommitted entries). Both sets are replayed together, in segment
//   creation order, so entries from the same Raft group may appear twice — once from
//   the pre-previous run's segments and once from the previous run's segments.
//
// Lifecycle:
//   1. During old commitlog replay, entries are added via add().
//   2. After replay completes, process_raft_replay_buffer() is called to:
//      - Apply committed mutations to memtables (so they get flushed to sstables)
//      - Rewrite uncommitted entries to the new commitlog (getting rp_handles)
//      - Discard entries that precede the commit index
//   3. When tablet raft groups start, each raft_commitlog instance
//      consumes its group's entries via take_replayed_group_entries().
//
// Crash Recovery Handling:
//   After a crash, both old and new commitlog segments may be present, and replay
//   processes all of them in segment order (oldest first). This can cause entries
//   from the same Raft group to appear in unexpected orders. The processing handles
//   three scenarios:
//
//   1. Leader changes: When a new leader is elected, it may overwrite uncommitted
//      entries from the previous leader starting at some index. During replay,
//      this appears as an entry with a higher term but smaller or equal index. When
//      detected, all collected entries with idx >= the new entry's idx are discarded.
//
//   2. Duplicate tail: Since segments are replayed oldest-first, entries from
//      an older segment are processed before entries from a newer segment. If
//      the newer segment re-appends entries at the same indices (e.g., after the
//      old segment was recycled but not fully reclaimed before a crash), those
//      duplicate entries will have idx <= last_idx with the same or lower term.
//      When detected (out_of_order), processing stops — the earlier entries
//      already cover those indices.
//
//   3. Committed entries: Entries with idx <= commit_idx are NOT filtered out
//      during the first pass. They are applied to memtables (because after a
//      crash, data may not have been flushed to sstables), but are excluded
//      from the raft log since the raft server treats them as already applied.
class raft_commitlog_replay_buffer {
    // Entries replayed from the old (pre-crash) commitlog segments, grouped by raft group.
    // Populated via add() during replay, then consumed and cleared by process_raft_replayed_items().
    std::unordered_map<raft::group_id, utils::chunked_vector<raft::log_entry_ptr>> _replayed_commitlog_entries_by_group;

    // Resulting entries and rp_handles written to the new (post-crash) commitlog,
    // raft_commitlog instances when tablet Raft groups start.
    std::unordered_map<raft::group_id, service::strong_consistency::replayed_data_per_group> _per_group_data;
    uint64_t _total_entries = 0;

public:
    // Add an entry during commitlog replay (before processing).
    void add(const raft::group_id group_id, raft::log_entry_ptr entry) {
        _replayed_commitlog_entries_by_group[group_id].push_back(std::move(entry));
        ++_total_entries;
    }

    // Get the replayed items for a group. These are removed from the buffer since they are now owned by the caller (raft_commitlog).
    service::strong_consistency::replayed_data_per_group take_replayed_group_entries(const raft::group_id group_id) {
        auto it = _per_group_data.find(group_id);
        if (it == _per_group_data.end()) {
            return {};
        }
        service::strong_consistency::replayed_data_per_group result = std::move(it->second);
        _per_group_data.erase(it);
        return result;
    }

    uint64_t total_entries() const {
        return _total_entries;
    }

    // Number of groups that still have unconsumed entries.
    size_t remaining_groups() const {
        return _replayed_commitlog_entries_by_group.size();
    }

    future<> stop() {
        return make_ready_future<>();
    }

    // Process the raft replay items after commitlog replay completes but before
    // old commitlog segments are deleted and memtables are flushed.
    //
    // For each raft group in the buffer:
    //   1. Reads commit_idx from the raft system tables.
    //   2. Filters entries: detects leader changes (discards replaced uncommitted
    //      entries) and stops at out-of-order tails from older pre-crash segments.
    //   3. For committed entries (idx <= commit_idx) that contain mutations:
    //      deserializes and applies them to memtables via apply_in_memory.
    //      This ensures data is available even if it wasn't flushed to sstables
    //      before the crash.
    //   4. For uncommitted entries (idx > commit_idx): rewrites them to the new
    //      commitlog, obtaining real rp_handles that tie segment lifetime to the data.
    //   5. Only entries with idx > commit_idx are added to the raft log
    //      (group_data.entries) for later consumption by raft_commitlog.
    //      Committed entries don't need to be in the log — raft treats them as
    //      already applied (snapshot.idx is advanced to commit_idx after replay).
    //   6. Non-command entries (configuration, dummy) are kept in the raft log but
    //      don't need mutation application or commitlog rewrite.
    future<> process_raft_replayed_items(replica::database& db, cql3::query_processor& qp, db::system_keyspace& sys_ks);
};
} // namespace db
