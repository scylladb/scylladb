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
#include <unordered_map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace db {

static seastar::logger logger("raft_commitlog_replay");

namespace {
// Build a mapping from group_id to table_id for the raft groups whose tablet this shard
// holds a replica of.
//
// The ownership test is what keeps replay from applying a group's entries into a tablet
// that has moved away: the group still exists in tablet metadata - it lives on its other
// replicas - so its presence there says nothing about whether this shard should be
// replaying it. has_replica() covers the old replica set and, through the transition's
// next set, the pending replica, so a replica in the middle of joining still replays what
// it received before the restart.
std::unordered_map<raft::group_id, table_id> build_group_to_table_map(const locator::token_metadata& tm) {
    const auto this_replica = locator::tablet_replica {
        .host = tm.get_my_id(),
        .shard = this_shard_id()
    };

    std::unordered_map<raft::group_id, table_id> result;
    const auto& tablets = tm.tablets();
    for (const auto& [tid, _] : tablets.all_table_groups()) {
        const auto& tablet_map = tablets.get_tablet_map(tid);
        if (!tablet_map.has_raft_info()) {
            continue;
        }
        for (const auto& tablet_id : tablet_map.tablet_ids()) {
            if (!tablet_map.has_replica(tablet_id, this_replica)) {
                continue;
            }
            const auto gid = tablet_map.get_tablet_raft_info(tablet_id).group_id;
            result.emplace(gid, tid);
        }
    }
    return result;
}
} // anonymous namespace

namespace raft_buffer_detail {

std::vector<raft::log_entry_ptr> drop_stale_copies(segment_cursors& cursors,
        const std::vector<raft::log_entry_ptr>& entries) {
    std::vector<raft::log_entry_ptr> rest;
    rest.reserve(entries.size());
    for (const auto& entry : entries) {
        bool stale = false;
        // Overlapping truncations leave several cursors live at one index. Only
        // the cursor waiting for that index claims the copy.
        for (auto& cursor : cursors) {
            if (cursor.exhausted() || cursor.next != entry->idx) {
                continue;
            }
            ++cursor.next;
            stale = true;
            break;
        }
        if (!stale) {
            rest.push_back(entry);
        }
    }
    return rest;
}

size_t superseded_by(const std::deque<buffered_entry>& buf, const raft::log_entry_ptr& first) {
    size_t count = 0;
    for (auto entry_it = buf.rbegin(); entry_it != buf.rend() && entry_it->entry->idx >= first->idx; ++entry_it) {
        ++count;
    }
    if (count == 0) {
        return 0;
    }
    // The buffered copy at the same index. Equal terms mean the same entry, so
    // this batch is a second copy of what is buffered. A crash between the runs
    // of a split rewrite leaves exactly that, and superseding on it would drop
    // the tail above the run.
    if (buf[buf.size() - count].entry->term == first->term) {
        return 0;
    }
    return count;
}

} // namespace raft_buffer_detail

future<> raft_commitlog_replay_buffer::resolve_group(replica::database& db, cql3::query_processor& qp,
        raft::group_id group_id, group_state& group) {
    group.resolved = true;

    const auto token_metadata = db.get_shared_token_metadata().get();
    if (!token_metadata->get_my_id()) {
        // Everything below decides what to replay by asking whether this node holds a
        // replica, so without our own host id the answer is "nothing" for every group and
        // we would silently discard committed entries that were never flushed. The id is
        // published into the topology config early in boot, long before replay, so this
        // means the boot sequence changed under us. Fail loudly instead.
        on_internal_error(logger, "processing the raft replay buffer before the local host id is known");
    }
    const auto group_to_table = build_group_to_table_map(*token_metadata);
    const auto table_it = group_to_table.find(group_id);
    if (table_it == group_to_table.end()) {
        // Nothing may be resurrected for this group, including its row.
        logger.debug("group {} is not hosted on this shard, discarding its entries", group_id);
        co_return;
    }
    // The floor: every index at or below it is committed.
    auto persisted = co_await service::strong_consistency::raft_groups_storage::load_descriptor(qp, group_id, this_shard_id());
    if (!persisted.exists) {
        // Tablet cleanup erased this shard's raft state for the group, and crashed or was
        // interrupted before it finished removing the tablet's storage; the tablet metadata
        // read above is from before that. The replica has left the group, so its entries
        // are not ours to apply - the coordinator retries the cleanup.
        logger.info("group {} has no persisted raft state on this shard, discarding its entries", group_id);
        co_return;
    }
    group.known = true;
    group.table = table_it->second;
    group.commit_idx = persisted.idx;
    group.commit_term = persisted.term;
    group.config = std::move(persisted.config);
    // The persisted configuration sits at the floor, so only a later one supersedes it.
    group.config_idx = persisted.idx;
    group.truncations = std::move(persisted.truncations);
    for (const auto& truncation : group.truncations) {
        group.cursors[truncation.segment].push_back(raft_buffer_detail::truncation_cursor{
                .from = truncation.from, .to = truncation.to, .next = truncation.from});
    }
    logger.debug("group {}: recovered floor ({}, {}), {} truncation records",
            group_id, group.commit_idx, group.commit_term, group.truncations.size());
}

void raft_commitlog_replay_buffer::note_committed(group_state& group, const raft::log_entry_ptr& entry) {
    if (entry->idx > group.commit_idx) {
        return;
    }
    group.commit_term = entry->term;
    if (std::holds_alternative<raft::configuration>(entry->data) && entry->idx >= group.config_idx) {
        group.config = std::get<raft::configuration>(entry->data);
        group.config_idx = entry->idx;
    }
}

future<> raft_commitlog_replay_buffer::apply_committed(replica::database& db, db::system_keyspace& sys_ks,
        const raft::log_entry_ptr& entry) {
    if (!std::holds_alternative<raft::command>(entry->data)) {
        co_return;
    }
    if (!_schemas) {
        _schemas.emplace(db, sys_ks);
    }
    auto mut = service::strong_consistency::detail::deserialize_to_frozen_mutation(entry);
    auto schema = co_await _schemas->resolve_and_upgrade(mut);
    co_await db.apply_in_memory(mut, std::move(schema), db::rp_handle(), db::no_timeout,
            db::noop_large_data_guardrail::instance());
}

future<> raft_commitlog_replay_buffer::drain_committed(replica::database& db, db::system_keyspace& sys_ks,
        group_state& group) {
    while (!group.buf.empty() && group.buf.front().entry->idx <= group.commit_idx) {
        auto entry = std::move(group.buf.front().entry);
        group.buf.pop_front();
        note_committed(group, entry);
        co_await apply_committed(db, sys_ks, entry);
        ++group.applied;
        co_await seastar::coroutine::maybe_yield();
    }
}

future<> raft_commitlog_replay_buffer::add_batch(replica::database& db, cql3::query_processor& qp,
        db::system_keyspace& sys_ks, raft::group_id group_id, db::segment_id_type segment,
        raft::index_t commit_idx, const std::vector<raft::log_entry_ptr>& entries) {
    auto& group = _groups[group_id];
    if (!group.resolved) {
        co_await resolve_group(db, qp, group_id, group);
    }
    if (!group.known) {
        co_return;
    }
    _total_entries += entries.size();

    if (commit_idx > group.commit_idx) {
        group.commit_idx = commit_idx;
    }
    // A batch header is written only after the final copy of every index at or
    // below it, so what the buffer holds below the floor is final.
    co_await drain_committed(db, sys_ks, group);

    // Drop the copies a truncation superseded.
    std::vector<raft::log_entry_ptr> rest = entries;
    if (auto cursors_it = group.cursors.find(segment); cursors_it != group.cursors.end()) {
        rest = raft_buffer_detail::drop_stale_copies(cursors_it->second, entries);
        group.dropped_stale += entries.size() - rest.size();
    }

    if (rest.empty()) {
        co_return;
    }

    // Record per segment what the supersede dropped. The floor persisted at
    // the end of replay makes those indexes look committed. That row is
    // durable before the old segments are deleted, so a second replay of the
    // same segments would apply a copy no leader ever committed.
    const auto superseded_count = raft_buffer_detail::superseded_by(group.buf, rest.front());
    std::unordered_map<db::segment_id_type, std::pair<raft::index_t, raft::index_t>> dropped;
    for (size_t i = 0; i < superseded_count; ++i) {
        const auto& back = group.buf.back();
        auto [dropped_it, inserted] = dropped.try_emplace(back.segment,
                std::pair(back.entry->idx, back.entry->idx));
        if (!inserted) {
            dropped_it->second.first = std::min(dropped_it->second.first, back.entry->idx);
            dropped_it->second.second = std::max(dropped_it->second.second, back.entry->idx);
        }
        group.buf.pop_back();
    }
    // A dropped copy can come from an older segment than this batch's.
    for (const auto& [dropped_segment, range] : dropped) {
        group.truncations.push_back(service::strong_consistency::truncation_record{
                .segment = dropped_segment, .from = range.first, .to = range.second});
    }
    group.superseded += superseded_count;

    // What a truncation superseded is gone from the buffer, so this only bites
    // on the copies of a duplicate batch: keep the buffered ones and the tail
    // above them.
    if (!group.buf.empty()) {
        const auto buffered_up_to = group.buf.back().entry->idx;
        std::erase_if(rest, [buffered_up_to](const auto& entry) { return entry->idx <= buffered_up_to; });
        if (rest.empty()) {
            co_return;
        }
    }

    for (auto& entry : rest) {
        if (entry->idx <= group.commit_idx) {
            // A copy of a committed entry is identical, so re-applying it is a no-op by timestamp.
            note_committed(group, entry);
            co_await apply_committed(db, sys_ks, entry);
            ++group.applied;
        } else {
            group.buf.push_back(raft_buffer_detail::buffered_entry{.entry = entry, .segment = segment});
        }
        co_await seastar::coroutine::maybe_yield();
    }
}

future<> raft_commitlog_replay_buffer::finish_replay(replica::database& db, cql3::query_processor& qp) {
    if (_groups.empty()) {
        co_return;
    }
    auto* new_commitlog_ptr = db.commitlog();
    SCYLLA_ASSERT(new_commitlog_ptr);

    logger.info("processing {} raft groups with {} total entries from commitlog replay",
            _groups.size(), _total_entries);

    // The rewrite below writes batches, so the write path's size rule applies.
    // It runs before groups_manager::start() checks that rule, so check here.
    service::strong_consistency::check_commitlog_can_hold_a_raft_entry(*new_commitlog_ptr);

    for (auto& [group_id, group] : _groups) {
        if (!group.known) {
            continue;
        }
        // The recovered floor is a real descriptor: every index at or below
        // commit_idx is committed, and commit_term is the term of the entry there.
        co_await service::strong_consistency::raft_groups_storage::store_descriptor(
                qp, group_id, this_shard_id(), group.commit_idx, group.commit_term, group.config, group.truncations);

        auto& group_data = _per_group_data[group_id];
        if (!group.buf.empty()) {
            if (group.buf.front().entry->idx != group.commit_idx + raft::index_t{1}) {
                on_internal_error(logger, fmt::format(
                        "group {}: replayed log starts at {} with a floor of {}",
                        group_id, group.buf.front().entry->idx, group.commit_idx));
            }
            raft::log_entry_ptr_list uncommitted;
            uncommitted.reserve(group.buf.size());
            for (auto& buffered : group.buf) {
                uncommitted.push_back(buffered.entry);
            }
            for (size_t i = 1; i < uncommitted.size(); ++i) {
                if (uncommitted[i]->idx != uncommitted[i - 1]->idx + raft::index_t{1}) {
                    on_internal_error(logger, fmt::format(
                            "group {}: gap in the replayed log between {} and {}",
                            group_id, uncommitted[i - 1]->idx, uncommitted[i]->idx));
                }
            }
            // Batches in the format store_log_entries() writes, so a reference
            // holds their records and a descriptor releases them, as after
            // startup. The tail is split into runs that each fit one commitlog
            // entry. raft_max_log_size bounds it, which one entry need not hold,
            // and an oversized rewrite would abort every replay from here on.
            const auto ends = service::strong_consistency::split_raft_batch(
                    *new_commitlog_ptr, group_id, group.commit_idx, uncommitted);
            size_t begin = 0;
            for (const auto end : ends) {
                const raft::log_entry_ptr_list run(
                        uncommitted.begin() + begin, uncommitted.begin() + end);
                auto handle = co_await service::strong_consistency::write_raft_batch(
                        *new_commitlog_ptr, group.table, group_id, group.commit_idx, run);
                service::strong_consistency::account_batch(group_data.records,
                        db::system_keyspace::raft_groups()->id(), std::move(handle), run);
                begin = end;
            }
            for (auto& entry : uncommitted) {
                group_data.entries.push_back(std::move(entry));
            }
        }
        logger.debug("group {}: floor=({}, {}), applied={}, dropped_stale={}, superseded={}, in_log={}",
                group_id, group.commit_idx, group.commit_term, group.applied, group.dropped_stale, group.superseded,
                group_data.entries.size());
        group.buf.clear();
    }
    _groups.clear();
    logger.info("Raft groups commit log replayed data processing complete");
}

raft_commitlog_replay_buffer::~raft_commitlog_replay_buffer() {
    size_t records = 0;
    for (auto& [group_id, data] : _per_group_data) {
        for (auto& rec : data.records) {
            rec.detach();
            ++records;
        }
    }
    if (records) {
        logger.error("destroyed with {} unclaimed records in {} groups: stop() did not run. "
                "Their references are detached, so the segments survive for the next replay, "
                "but this should not happen.", records, _per_group_data.size());
    }
}

future<> raft_commitlog_replay_buffer::stop() {
    size_t records = 0;
    for (auto& [group_id, data] : _per_group_data) {
        for (auto& rec : data.records) {
            rec.detach();
            ++records;
        }
        logger.info("group {} never started; detaching the references of {} records so its "
                "replayed entries can be recovered again", group_id, data.records.size());
    }
    _per_group_data.clear();
    if (records) {
        logger.info("detached the references of {} unclaimed records", records);
    }
    return make_ready_future<>();
}

} // namespace db
