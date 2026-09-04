/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <deque>
#include <optional>
#include <unordered_map>
#include <seastar/core/future.hh>
#include "utils/chunked_vector.hh"
#include "raft/raft.hh"
#include "db/commitlog/replay_position.hh"
#include "service/strong_consistency/raft_commitlog.hh"
#include "service/strong_consistency/state_machine.hh"

namespace cql3 {
class query_processor;
}

// Test seam, defined by commitlog_raft_replay_test.
class raft_replay_buffer_tester;

namespace db {
class system_keyspace;

namespace raft_buffer_detail {

// A truncation record being matched off against the copies being read: the
// range it covers, and the next index within it still expected.
struct truncation_cursor {
    raft::index_t from{0};
    raft::index_t to{0};
    raft::index_t next{0};

    bool exhausted() const {
        return next > to;
    }
};

// Truncation cursors for one segment, in the order the truncations were written.
using segment_cursors = std::deque<truncation_cursor>;

// Drop the copies a truncation superseded, returning the ones that survive.
//
// A truncation removes a suffix of the raft log, so within one batch the copies
// it discarded are always the batch's tail and the survivors a prefix. Each
// index is matched against the oldest cursor of this segment that is *waiting
// for that index* — not simply the oldest live one: truncations of one segment
// can reach back past each other, so several cursors can be live at an index
// with only a later one waiting for it.
std::vector<raft::log_entry_ptr> drop_stale_copies(segment_cursors& cursors,
    const std::vector<raft::log_entry_ptr>& entries);

// One entry held in the replay buffer, with the segment its copy was read from.
// The segment is needed when the copy turns out to be superseded: replay records
// that, so that replaying the same segments again cannot mistake the copy for
// the current one.
struct buffered_entry {
    raft::log_entry_ptr entry;
    db::segment_id_type segment{0};
};

// How many of the buffered entries a batch starting at `first` supersedes: a
// later write at index N replaces anything already buffered at or above N.
size_t superseded_by(const std::deque<buffered_entry>& buf, raft::index_t first);

} // namespace raft_buffer_detail

// Per-shard state for replaying the raft log of strongly consistent tablets out
// of the commitlog.
//
// Terminology:
//   "old commitlog"  — commitlog segment files left on disk from a previous run.
//                      On each startup, ScyllaDB replays these segments to recover
//                      any entries that had not yet been applied to sstables.
//   "new commitlog"  — the fresh commitlog created at the start of the current run.
//                      During recovery, the entries that are still uncommitted are
//                      rewritten here, so that they are held by a reference and
//                      released by a descriptor like any others.
//
//   Old segment files are only deleted after the memtables they cover have been
//   flushed. If the node crashes again before that flush completes, the next run
//   finds segments from *two* previous runs on disk, so entries of one group can
//   appear more than once — the "copies" below.
//
// One pass, in write order. Replay hands each raft batch to add_batch() in the
// order it was written (segments in creation order, entries by position within a
// segment), and everything is decided as it arrives:
//
//   * The batch header carries the group's commit index at write time. That is a
//     true statement about the past — commit indexes only advance and a committed
//     entry is never replaced — so the running maximum is a floor: entries at or
//     below it are committed, get applied to memtables straight away, and are
//     never buffered. Only the uncommitted tail is held in memory, which bounds
//     the buffer by the group's uncommitted window (SCYLLADB-1539).
//
//   * Which copy of an index is current is answered by the truncation records the
//     group persisted, not by comparing terms. Each record says "in this segment,
//     the then-current copies of indexes [from, to] were truncated", so the
//     copies to drop can be matched off against the records as they are read.
//
//   * A later write at index N supersedes anything already buffered at or above
//     N, which is what makes a leader change that reuses indexes come out right.
//
// At the end of replay finish_replay() writes each group's recovered descriptor
// to its system.raft_groups row and rewrites what is left in the buffer to the
// new commitlog as one batch, whose records seed the group's queue.
class raft_commitlog_replay_buffer {
    // Lets a test seed the records a replay would have parked here, without
    // standing up a database and tablet metadata to reach finish_replay().
    friend class ::raft_replay_buffer_tester;

    struct group_state {
        // Resolved on first sight, from tablet metadata. A group that is not
        // there has moved away or been dropped, and everything read for it is
        // discarded.
        bool resolved = false;
        bool known = false;
        table_id table;

        // The running commit index and the term of the entry at it.
        raft::index_t commit_idx{0};
        raft::term_t commit_term{0};
        // The newest configuration seen at or below commit_idx, and the index of
        // the entry that carried it.
        raft::configuration config;
        raft::index_t config_idx{0};

        // The uncommitted tail, ascending.
        std::deque<raft_buffer_detail::buffered_entry> buf;

        // The group's persisted truncation records, grouped by the segment they
        // name and kept in the order they were written, plus the whole list so
        // that finish_replay() can persist it again unchanged.
        std::unordered_map<db::segment_id_type, raft_buffer_detail::segment_cursors> cursors;
        std::vector<service::strong_consistency::truncation_record> truncations;

        uint64_t applied = 0;
        uint64_t dropped_stale = 0;
        uint64_t superseded = 0;
    };

    std::unordered_map<raft::group_id, group_state> _groups;
    // What each group starts with once replay is done.
    std::unordered_map<raft::group_id, service::strong_consistency::replayed_data_per_group> _per_group_data;
    uint64_t _total_entries = 0;
    // One schema store for the whole replay, shared by every group: it resolves
    // the table from the mutation itself, so entries written with the same schema
    // version resolve it only once. Created on first use because the database and
    // system keyspace arrive per call. No barrier trigger — group0 is not started
    // during replay, so an unresolvable version has nothing to wait for.
    std::optional<service::strong_consistency::schema_store> _schemas;

    // Resolve the group against tablet metadata and read back what it persisted:
    // the floor, its term, the configuration, and the truncation records.
    future<> resolve_group(replica::database& db, cql3::query_processor& qp,
        raft::group_id group_id, group_state& g);

    // Apply one committed command entry to its table's memtable. No reference is
    // attached: the segment being replayed is deleted only after the memtables
    // are flushed, so the data is either in an sstable or still in that segment.
    future<> apply_committed(replica::database& db, db::system_keyspace& sys_ks,
        const group_state& g, const raft::log_entry_ptr& entry);

    // Consume the buffered entries the floor has reached.
    future<> drain_committed(replica::database& db, db::system_keyspace& sys_ks, group_state& g);

    // Note a committed entry's term and configuration.
    void note_committed(group_state& g, const raft::log_entry_ptr& entry);

public:
    // Called once per raft batch during replay, in write order.
    future<> add_batch(replica::database& db, cql3::query_processor& qp, db::system_keyspace& sys_ks,
        raft::group_id group_id, db::segment_id_type segment, raft::index_t commit_idx,
        const std::vector<raft::log_entry_ptr>& entries);

    // Called after commitlog replay completes, but before the old segments are
    // deleted and the memtables flushed.
    future<> finish_replay(replica::database& db, cql3::query_processor& qp);

    // Get what a group starts with. Removed from the buffer, since the caller
    // (its raft_groups_storage) takes ownership of the references.
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

    // Detach the references of anything no group claimed, rather than letting
    // the handles decrement as they are destroyed. A group whose rewritten tail
    // was never taken — its start failed, or the node went down — needs those
    // segments to survive into the next replay; releasing them would lose
    // entries already acknowledged to a leader. Same rule as ~raft_commitlog.
    future<> stop();

    // The same rule again, because the implicit destructor would do the
    // opposite. Reaching here with records left means stop() was skipped — an
    // exception between start() and its shutdown hook, say — and the default
    // behaviour there would be to decrement, delete the segments holding a
    // rewritten tail, and lose entries a leader already counted as committed.
    // A crash cannot do that (destructors never run, so the files survive for
    // replay); only a live process can, which is why this path has to be
    // explicit rather than left to the compiler.
    ~raft_commitlog_replay_buffer();
};
} // namespace db
