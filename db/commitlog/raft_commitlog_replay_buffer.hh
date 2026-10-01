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

// One persisted truncation record, tracking the next index in [from, to] to match.
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

// Drop the copies a truncation superseded and return the surviving entries. Each
// index is claimed by the oldest cursor of this segment waiting for that index.
std::vector<raft::log_entry_ptr> drop_truncated_copies(segment_cursors& cursors,
    const std::vector<raft::log_entry_ptr>& entries);

// One buffered entry. The segment is recorded so a superseded copy stays
// superseded if the same segments are replayed again.
struct buffered_entry {
    raft::log_entry_ptr entry;
    db::segment_id_type segment{0};
    // Set on the entry a batch's chain link contradicted and on everything above it:
    // a later leader replaced that entry, and the batch holding the replacement was
    // never read. drain_committed() stops at an unverified entry and finish_replay()
    // refuses the group, unless a superseding batch pops the entry and its mark together.
    bool unverified = false;
};

// How many of the buffered entries `first`, the head of an arriving batch,
// supersedes. Raft truncates the log only where the term differs, so a batch
// that overlaps the buffer at the same term is another copy of those entries
// and supersedes nothing. `commit_idx` decides a batch starting below the
// buffer. At or under commit_idx the batch holds copies of committed entries,
// which are never replaced. Above commit_idx raft reached that index only by
// truncating at it, which leaves every buffered entry superseded.
size_t superseded_by(const std::deque<buffered_entry>& buf, const raft::log_entry_ptr& first,
        raft::index_t commit_idx);

} // namespace raft_buffer_detail

// Per-shard state for replaying the raft log of strongly consistent tablets out of
// the commitlog. Batches arrive through add_batch() in write order and each is
// decided as it is read; finish_replay() persists what is left.
class raft_commitlog_replay_buffer {
    // Lets a test seed the buffer directly, skipping the replay that fills it.
    friend class ::raft_replay_buffer_tester;

    struct group_state {
        // Resolved on first sight from tablet metadata. A group that is missing has
        // moved away or been dropped; everything read for it is discarded.
        bool resolved = false;
        bool known = false;
        table_id table;

        // The running commit index.
        raft::index_t commit_idx{0};
        // The floor the row was loaded with. A link to an index below it names an entry
        // already durable in an sstable, which no segment read can check.
        service::strong_consistency::raft_term_and_index loaded_floor;
        // Highest index this pass has applied, with its term. Seeded from the floor,
        // so a batch that starts one above the floor links to the floor's term.
        service::strong_consistency::raft_term_and_index last_consumed;
        // Index whose committed copy a later batch's chain link contradicted.
        // finish_replay() refuses the boot.
        std::optional<raft::index_t> broken_chain_at;
        // How far an unbroken run of indexes read in this pass reaches, starting from
        // the floor the row was loaded with. A run that stops below commit_idx means a
        // committed entry was lost, which refuses the boot; see finish_replay().
        raft::index_t contiguous_to{0};
        // Term of the entry at commit_idx.
        raft::term_t commit_term{0};
        // The newest configuration at or below commit_idx, and its entry's index.
        raft::configuration config;
        raft::index_t config_idx{0};

        // The uncommitted tail, ascending.
        std::deque<raft_buffer_detail::buffered_entry> buf;

        // The group's persisted truncation records: grouped by segment in write
        // order, plus the whole list so finish_replay() can persist it unchanged.
        std::unordered_map<db::segment_id_type, raft_buffer_detail::segment_cursors> cursors;
        std::vector<service::strong_consistency::truncation_record> truncations;

        uint64_t applied = 0;
        uint64_t dropped_truncated = 0;
        uint64_t superseded = 0;

        // Keep the earliest index a chain link contradicted, so the refusal names where
        // the log stopped chaining.
        void note_broken_chain(raft::index_t idx) {
            if (!broken_chain_at) {
                broken_chain_at = idx;
            }
        }
    };

    std::unordered_map<raft::group_id, group_state> _groups;
    std::unordered_map<raft::group_id, service::strong_consistency::replayed_data_per_group> _per_group_data;
    uint64_t _total_entries = 0;
    // One schema store for the whole replay, so each schema version resolves once
    // across all groups. Created on first use.
    std::optional<service::strong_consistency::schema_store> _schemas;

    // Which group each of this shard's tablet replicas belongs to. Built once, on
    // first use, by walking every tablet in the cluster's metadata; replay changes
    // no tablet.
    std::optional<std::unordered_map<raft::group_id, table_id>> _group_to_table;

    // Segments a replay could not read in full; see note_unreadable_segment().
    std::vector<db::segment_id_type> _unreadable_segments;

    // Resolve the group against tablet metadata and read back its persisted state.
    future<> resolve_group(replica::database& db, cql3::query_processor& qp,
        raft::group_id group_id, group_state& group);

    // Apply one committed command entry to its table's memtable.
    future<> apply_committed(replica::database& db, db::system_keyspace& sys_ks,
        const raft::log_entry_ptr& entry);

    // Consume the buffered entries the floor has reached.
    future<> drain_committed(replica::database& db, db::system_keyspace& sys_ks, group_state& group);

    // Note a committed entry's term and configuration.
    static void note_committed(group_state& group, const raft::log_entry_ptr& entry);

    // Compare a batch's chain link against what replay has read, and record a break.
    static void check_chain_link(raft::group_id group_id, group_state& group,
        raft::index_t first_idx, raft::term_t prev_term);

public:
    // Called once per raft batch during replay, in write order.
    future<> add_batch(replica::database& db, cql3::query_processor& qp, db::system_keyspace& sys_ks,
        raft::group_id group_id, db::segment_id_type segment, raft::index_t commit_idx,
        raft::term_t prev_term, const std::vector<raft::log_entry_ptr>& entries);

    // Called after commitlog replay completes, but before the old segments are
    // deleted and the memtables flushed. Persists each group's recovered descriptor
    // and rewrites the buffered tail to the new commitlog, split across as many
    // batches as it takes for each batch to fit a commitlog entry.
    future<> finish_replay(replica::database& db, cql3::query_processor& qp);

    // Take a group's replayed data out of the buffer: the caller takes ownership
    // of the references.
    service::strong_consistency::replayed_data_per_group take_replayed_group_entries(const raft::group_id group_id) {
        auto group_it = _per_group_data.find(group_id);
        if (group_it == _per_group_data.end()) {
            return {};
        }
        service::strong_consistency::replayed_data_per_group result = std::move(group_it->second);
        _per_group_data.erase(group_it);
        return result;
    }

    uint64_t total_entries() const {
        return _total_entries;
    }

    // A segment the replay could not read whole: a bad sector, or a header that failed
    // its check and skipped the file. finish_replay() decides from the indexes whether
    // the loss cost a raft group anything; the segments named here only go into the
    // warning. A truncated tail, where writing stopped at the crash, is not recorded.
    void note_unreadable_segment(db::segment_id_type segment) {
        _unreadable_segments.push_back(segment);
    }

    // Detach the references of anything no group claimed, as ~raft_commitlog does.
    // A rewritten tail nobody took must survive into the next replay: a decrement
    // retires its segments and loses entries already acknowledged to a leader.
    future<> stop();

    // Detaches the unclaimed references, as stop() does, for the one path that skips
    // stop(): an allocation failure in the submit_to of sharded::stop()'s first phase.
    ~raft_commitlog_replay_buffer();
};
} // namespace db
