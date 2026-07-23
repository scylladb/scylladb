/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/database.hh"
#include "db/commitlog/raft_commitlog_replay_buffer.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/timeout_clock.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema_registry.hh"
#include "db/system_keyspace.hh"
#include "service/strong_consistency/state_machine.hh"
#include "serializer_impl.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace db {

static seastar::logger logger("raft_commitlog_replay");

namespace {
// Build a mapping from group_id to table_id using tablet metadata.
std::unordered_map<raft::group_id, table_id> build_group_to_table_map(const locator::token_metadata& tm) {
    std::unordered_map<raft::group_id, table_id> result;
    const auto& tablets = tm.tablets();
    for (const auto& [tid, _] : tablets.all_table_groups()) {
        const auto& tablet_map = tablets.get_tablet_map(tid);
        if (!tablet_map.has_raft_info()) {
            continue;
        }
        for (const auto& tablet_id : tablet_map.tablet_ids()) {
            const auto gid = tablet_map.get_tablet_raft_info(tablet_id).group_id;
            result.emplace(gid, tid);
        }
    }
    return result;
}

struct filter_entries_result {
    std::vector<raft::log_entry_ptr> entries;
    uint64_t discarded_leader_change = 0;
};

// First pass: Filter entries by handling leader changes and out-of-order tails.
// This function does not perform any database or commitlog writes.
// Returns the filtered entries along with discard statistics.
//
// Handles crash recovery scenarios where both old and new commitlog segments are replayed:
// - Detects leader changes and discards replaced uncommitted entries (idx >= new entry's idx)
// - Stops processing when an out-of-order entry is encountered (idx <= last_idx with same or
//   lower term), which indicates the start of a duplicate tail from an older segment
//
// Note: This function does NOT filter by snapshot_idx. All entries pass through so that
// the caller can apply committed entries to memtables even if they are already snapshotted.
// After a crash, snapshotted entries may not have been flushed to sstables, so they must
// be re-applied during replay.
filter_entries_result filter_entries(utils::chunked_vector<raft::log_entry_ptr>& entries_list, raft::group_id group_id) {
    filter_entries_result result;

    raft::index_t last_idx{0};
    raft::term_t last_term{0};

    for (auto& entry : entries_list) {
        // Check for out-of-order indices which indicate either a leader change
        // (term changed) or a duplicate tail from an older segment.
        const auto ordering = raft_buffer_detail::check_entry_ordering({.term = entry->term, .idx = entry->idx}, {.term = last_term, .idx = last_idx});

        switch (ordering) {
        case raft_buffer_detail::entry_ordering_check_result::in_order:
            break;
        case raft_buffer_detail::entry_ordering_check_result::leader_change: {
            // Leader changed — discard all entries with idx >= entry->idx from
            // the previous term. These were uncommitted entries from the old leader
            // that were replaced by the new leader.
            //
            // Since entries are sorted by idx, we can use binary search to find
            // the first entry with idx >= entry->idx and erase from there to the end.
            auto it = std::ranges::lower_bound(result.entries, entry->idx, {}, [](const raft::log_entry_ptr& e) {
                return e->idx;
            });
            auto removed_count = std::distance(it, result.entries.end());
            result.entries.erase(it, result.entries.end());
            result.discarded_leader_change += removed_count;

            logger.debug("group {}: leader change detected at idx={} term={} (previous term={}), "
                         "discarded {} entries with idx >= {}",
                    group_id, entry->idx, entry->term, last_term, removed_count, entry->idx);
            break;
        }
        case raft_buffer_detail::entry_ordering_check_result::out_of_order:
            // Smaller index and same/smaller term - we hit the start of a duplicate
            // range from an older commitlog segment.  This happens after a crash
            // when uncommitted entries were re-written to a new segment and then
            // the old segment is replayed first.  All remaining entries from this
            // point are duplicates, so stop processing.
            logger.debug("group {}: detected duplicate tail from older segment at idx={} term={} "
                        "(last_idx={}), stopping entry processing",
                    group_id, entry->idx, entry->term, last_idx);
            return result;
        }

        last_idx = entry->idx;
        last_term = entry->term;

        result.entries.push_back(std::move(entry));
    }
    return result;
}

} // anonymous namespace

future<> raft_commitlog_replay_buffer::process_raft_replayed_items(replica::database& db, cql3::query_processor& qp, db::system_keyspace& sys_ks) {
    if (remaining_groups() == 0) {
        co_return;
    }

    const auto token_metadata = db.get_shared_token_metadata().get();
    const auto group_to_table = build_group_to_table_map(*token_metadata);

    auto* new_commitlog_ptr = db.commitlog();
    SCYLLA_ASSERT(new_commitlog_ptr);

    logger.info("processing {} raft groups with {} total entries from commitlog replay", remaining_groups(), total_entries());

    for (auto& [group_id, entries_list] : _replayed_commitlog_entries_by_group) {
        if (entries_list.empty()) {
            continue;
        }

        // Look up table_id for this group.
        auto table_it = group_to_table.find(group_id);
        if (table_it == group_to_table.end()) {
            // Group not found in tablet metadata — the tablet may have been moved away.
            // Discard these entries since this shard no longer owns the tablet.
            logger.debug("group {} not found in tablet metadata, discarding {} entries", group_id, entries_list.size());
            continue;
        }
        const auto table_id = table_it->second;

        // Query commit_idx from raft system tables. We treat commit_idx as
        // the effective snapshot index: all entries up to commit_idx are committed
        // and will be applied to memtables during replay.
        const auto commit_idx = co_await service::strong_consistency::raft_groups_storage::load_commit_idx(qp, group_id, this_shard_id());

        logger.debug("group {}: {} entries, commit_idx={}", group_id, entries_list.size(), commit_idx);

        // First pass: filter entries (leader changes, out-of-order tails).
        // This must be done before any database writes to avoid applying entries that would
        // later be discarded due to leader changes.
        auto filtered = filter_entries(entries_list, group_id);

        // Second pass: Apply committed entries to database and rewrite uncommitted entries to commitlog.
        uint64_t applied = 0;
        uint64_t rewritten = 0;
        auto& group_data = _per_group_data[group_id];

        for (auto& entry : filtered.entries) {
            // Apply committed command entries to the memtables. It is safe not to append them
            // to the new commitlog, because the old commitlog (currently being replayed) will
            // only be deleted after the memtables are flushed. Therefore, the data will either
            // be persisted to SSTables or, in case of a crash, still be available in the old commitlog.
            if (entry->idx <= commit_idx && std::holds_alternative<raft::command>(entry->data)) {
                utils::chunked_vector<frozen_mutation> muts;
                muts.emplace_back(service::strong_consistency::detail::deserialize_to_frozen_mutation(entry));
                // Resolve schema and upgrade mutation if needed (no barrier during replay).
                auto schemas = co_await service::strong_consistency::resolve_and_upgrade_mutations(muts, table_id, db, sys_ks);
                co_await db.apply_in_memory(muts[0], schemas[0], db::rp_handle(), db::no_timeout, db::noop_large_data_guardrail::instance());
                ++applied;
            }

            // Rewrite uncommitted entries to the new commitlog to obtain rp_handles
            // that ensure the commitlog segments won't be deleted.
            if (entry->idx > commit_idx) {
                commitlog_raft_log_entry_writer writer(table_id, raft_commitlog_entry{.group_id = group_id, .entry = entry});
                const auto write_fn = [&writer](auto& out) {
                    return writer.write(out);
                };
                const auto target_size = writer.size();
                auto handle = co_await new_commitlog_ptr->add(table_id, target_size, db::no_timeout, db::commitlog_force_sync::yes, write_fn);
                group_data.replay_positions.push_back(service::strong_consistency::index_and_replay_position{.index = entry->idx, .replay_position_handle = std::move(handle)});
                ++rewritten;
            }

            // Only add uncommitted entries to the raft log. Committed entries have
            // already been applied to memtables above and are covered by the
            // snapshot index that bump_snapshot_indices() advances to commit_idx
            // after replay (the sole snapshot-index advancer on startup).
            if (entry->idx > commit_idx) {
                group_data.entries.push_back(std::move(entry));
            }

            co_await seastar::coroutine::maybe_yield();
        }

        logger.debug("group {}: discarded_leader_change={}, applied={}, rewritten={}, total_in_log={}", group_id, filtered.discarded_leader_change, applied,
                rewritten, group_data.entries.size());
    }

    // The old items are not needed anymore.
    _replayed_commitlog_entries_by_group.clear();
    logger.info("Raft groups commit log replayed data processing complete");
}

future<> raft_commitlog_replay_buffer::bump_snapshot_indices(replica::database& db, cql3::query_processor& qp) {
    // See header for rationale. Runs for every known raft group and is the sole
    // place that advances the persisted snapshot index on startup. store_snapshot_index
    // has an internal "only advance" guard, so groups whose snapshot is already at or
    // beyond commit_idx (e.g. from a prior run) are left untouched.
    const auto token_metadata = db.get_shared_token_metadata().get();
    const auto group_to_table = build_group_to_table_map(*token_metadata);

    // Process groups concurrently: each does independent CQL reads/writes, and
    // doing them serially would make startup O(groups) round-trips.
    co_await seastar::coroutine::parallel_for_each(group_to_table,
            [&qp] (const auto& entry) -> future<> {
        const auto group_id = entry.first;
        const auto commit_idx = co_await service::strong_consistency::raft_groups_storage::load_commit_idx(qp, group_id, this_shard_id());
        if (commit_idx.value() == 0) {
            co_return;
        }
        auto [snap_idx, snap_term] = co_await service::strong_consistency::raft_groups_storage::load_snapshot_idx_and_term(qp, group_id, this_shard_id());
        if (snap_idx >= commit_idx) {
            co_return;
        }
        // Reuse the previously-recorded snapshot term (the term of the last real
        // raft snapshot). It is <= term(commit_idx), so it errs conservative /
        // too-low, which is safe: the replica looks no more up-to-date than its
        // data, and any log-matching mismatch is repaired by InstallSnapshot from
        // a peer. The exact term(commit_idx) is not recoverable here once the
        // commitlog is empty; persisting it for an exact bump is tracked in
        // SCYLLADB-3357.
        co_await service::strong_consistency::raft_groups_storage::store_snapshot_index(
                qp, group_id, this_shard_id(), raft::snapshot_descriptor{
                    .idx = commit_idx,
                    .term = snap_term,
                    .id = raft::snapshot_id(utils::make_random_uuid()),
                });
        logger.debug("group {}: post-replay catch-up, advanced snapshot idx {} -> {} (term={})",
                group_id, snap_idx, commit_idx, snap_term);
    });
}
namespace raft_buffer_detail {
entry_ordering_check_result check_entry_ordering(raft_term_and_idx current, raft_term_and_idx last) {
    if (last.idx == raft::index_t{0}) {
        return entry_ordering_check_result::in_order;
    }
    if (current.idx > last.idx) {
        return entry_ordering_check_result::in_order;
    }
    if (current.term > last.term) {
        return entry_ordering_check_result::leader_change;
    }
    return entry_ordering_check_result::out_of_order;
}
} // namespace raft_buffer_detail

} // namespace db
