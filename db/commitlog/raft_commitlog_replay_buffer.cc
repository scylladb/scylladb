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
// Build a mapping from group_id to table_id using tablet metadata, for the
// groups this shard hosts.
//
// The replica check is the same rule groups_manager::update() applies when it
// decides which groups to run, and it has to be the same: a group whose tablet
// lives elsewhere now is never started here, so replaying its entries would
// resurrect data into a range this shard no longer owns, and would leave its
// rewritten tail pinning segments that nothing ever takes. has_replica() counts
// the transition target too, so a tablet being migrated *to* this shard still
// resolves, which is what lets its log be recovered.
std::unordered_map<raft::group_id, table_id> build_group_to_table_map(const locator::token_metadata& tm) {
    std::unordered_map<raft::group_id, table_id> result;
    const auto this_replica = locator::tablet_replica{.host = tm.get_my_id(), .shard = this_shard_id()};
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
        // The oldest live cursor *that is waiting for this index* claims the
        // copy. It is not enough to look at the oldest live cursor and stop:
        // truncations of one segment can reach back past each other. A
        // truncation that clamps a record leaves a cursor covering the tail it
        // discarded, and a later truncation that pops the same record covers a
        // range starting below it — so at any index several cursors can be live
        // with only a later one waiting for it.
        for (auto& cursor : cursors) {
            if (cursor.exhausted() || cursor.next != entry->idx) {
                continue;
            }
            cursor.next = entry->idx + raft::index_t{1};
            stale = true;
            break;
        }
        if (!stale) {
            rest.push_back(entry);
        }
    }
    return rest;
}

size_t superseded_by(const std::deque<buffered_entry>& buf, raft::index_t first) {
    size_t n = 0;
    for (auto it = buf.rbegin(); it != buf.rend() && it->entry->idx >= first; ++it) {
        ++n;
    }
    return n;
}

} // namespace raft_buffer_detail

future<> raft_commitlog_replay_buffer::resolve_group(replica::database& db, cql3::query_processor& qp,
        raft::group_id group_id, group_state& g) {
    g.resolved = true;

    const auto token_metadata = db.get_shared_token_metadata().get();
    const auto group_to_table = build_group_to_table_map(*token_metadata);
    const auto it = group_to_table.find(group_id);
    if (it == group_to_table.end()) {
        // The tablet was dropped, or it is no longer hosted on this shard, so
        // nothing may be resurrected for it — including its row.
        logger.debug("group {} is not hosted on this shard, discarding its entries", group_id);
        co_return;
    }
    g.known = true;
    g.table = it->second;

    // What the group already made durable. Everything at or below this index is
    // committed, so the pass starts from there rather than from zero.
    auto persisted = co_await service::strong_consistency::raft_groups_storage::load_descriptor(qp, group_id, this_shard_id());
    g.commit_idx = persisted.idx;
    g.commit_term = persisted.term;
    g.config = std::move(persisted.config);
    // The persisted configuration is the one at the floor, so only a
    // configuration at or above it can supersede it.
    g.config_idx = persisted.idx;
    g.truncations = std::move(persisted.truncations);
    for (const auto& t : g.truncations) {
        g.cursors[t.segment].push_back(raft_buffer_detail::truncation_cursor{.from = t.from, .to = t.to, .next = t.from});
    }
    logger.debug("group {}: recovered floor ({}, {}), {} truncation records",
            group_id, g.commit_idx, g.commit_term, g.truncations.size());
}

void raft_commitlog_replay_buffer::note_committed(group_state& g, const raft::log_entry_ptr& entry) {
    if (entry->idx > g.commit_idx) {
        return;
    }
    g.commit_term = entry->term;
    if (std::holds_alternative<raft::configuration>(entry->data) && entry->idx >= g.config_idx) {
        g.config = std::get<raft::configuration>(entry->data);
        g.config_idx = entry->idx;
    }
}

future<> raft_commitlog_replay_buffer::apply_committed(replica::database& db, db::system_keyspace& sys_ks,
        const group_state& g, const raft::log_entry_ptr& entry) {
    if (!std::holds_alternative<raft::command>(entry->data)) {
        co_return;
    }
    if (!_schemas) {
        _schemas.emplace(db, sys_ks);
    }
    auto mut = service::strong_consistency::detail::deserialize_to_frozen_mutation(entry);
    // Resolves the table from the mutation and upgrades it in place if it was
    // written with an older schema.
    auto schema = co_await _schemas->resolve_and_upgrade(mut);
    // No reference attached: the segment being replayed is deleted only after the
    // memtables are flushed, so the data is either in an sstable or still in that
    // segment.
    co_await db.apply_in_memory(mut, std::move(schema), db::rp_handle(), db::no_timeout,
            db::noop_large_data_guardrail::instance());
}

future<> raft_commitlog_replay_buffer::drain_committed(replica::database& db, db::system_keyspace& sys_ks,
        group_state& g) {
    while (!g.buf.empty() && g.buf.front().entry->idx <= g.commit_idx) {
        auto entry = std::move(g.buf.front().entry);
        g.buf.pop_front();
        note_committed(g, entry);
        co_await apply_committed(db, sys_ks, g, entry);
        ++g.applied;
        co_await seastar::coroutine::maybe_yield();
    }
}

future<> raft_commitlog_replay_buffer::add_batch(replica::database& db, cql3::query_processor& qp,
        db::system_keyspace& sys_ks, raft::group_id group_id, db::segment_id_type segment,
        raft::index_t commit_idx, const std::vector<raft::log_entry_ptr>& entries) {
    auto& g = _groups[group_id];
    if (!g.resolved) {
        co_await resolve_group(db, qp, group_id, g);
    }
    if (!g.known) {
        co_return;
    }
    _total_entries += entries.size();

    // The header's commit index only ever advances the floor.
    if (commit_idx > g.commit_idx) {
        g.commit_idx = commit_idx;
    }
    // A batch header is written only after the final copy of every index at or
    // below it, so what the buffer holds below the floor is final and can be
    // applied now.
    co_await drain_committed(db, sys_ks, g);

    // Drop the copies a truncation superseded.
    std::vector<raft::log_entry_ptr> rest = entries;
    if (auto cit = g.cursors.find(segment); cit != g.cursors.end()) {
        rest = raft_buffer_detail::drop_stale_copies(cit->second, entries);
        g.dropped_stale += entries.size() - rest.size();
    }

    if (!rest.empty()) {
        // A later write at index N supersedes anything buffered at or above N.
        //
        // Record what that dropped, per segment. The floor this replay persists
        // makes those indexes look committed, so if the same old segments are
        // replayed again — the row is durable as soon as it is written, while the
        // old segments are only deleted at the end of startup — nothing else
        // would tell the next pass that those copies are stale, and it would
        // apply an entry no leader ever committed.
        const auto n = raft_buffer_detail::superseded_by(g.buf, rest.front()->idx);
        std::unordered_map<db::segment_id_type, std::pair<raft::index_t, raft::index_t>> dropped;
        for (size_t i = 0; i < n; ++i) {
            const auto& back = g.buf.back();
            auto [it, inserted] = dropped.try_emplace(back.segment,
                    std::pair(back.entry->idx, back.entry->idx));
            if (!inserted) {
                it->second.first = std::min(it->second.first, back.entry->idx);
                it->second.second = std::max(it->second.second, back.entry->idx);
            }
            g.buf.pop_back();
        }
        for (const auto& [segment, range] : dropped) {
            g.truncations.push_back(service::strong_consistency::truncation_record{
                    .segment = segment, .from = range.first, .to = range.second});
        }
        g.superseded += n;
    }

    for (auto& entry : rest) {
        if (entry->idx <= g.commit_idx) {
            // Already committed, so this can only be a copy identical to the one
            // that made it committed. Applying it again is a no-op by timestamp.
            note_committed(g, entry);
            co_await apply_committed(db, sys_ks, g, entry);
            ++g.applied;
        } else {
            g.buf.push_back(raft_buffer_detail::buffered_entry{.entry = entry, .segment = segment});
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

    for (auto& [group_id, g] : _groups) {
        if (!g.known) {
            continue;
        }
        // The floor this replay recovered is what the group starts from, and it
        // is a real descriptor: every index at or below it is committed, and its
        // term is the term of the entry there.
        co_await service::strong_consistency::raft_groups_storage::store_descriptor(
                qp, group_id, this_shard_id(), g.commit_idx, g.commit_term, g.config, g.truncations);

        auto& group_data = _per_group_data[group_id];
        if (!g.buf.empty()) {
            if (g.buf.front().entry->idx != g.commit_idx + raft::index_t{1}) {
                on_internal_error(logger, fmt::format(
                        "group {}: replayed log starts at {} with a floor of {}",
                        group_id, g.buf.front().entry->idx, g.commit_idx));
            }
            raft::log_entry_ptr_list uncommitted;
            uncommitted.reserve(g.buf.size());
            for (auto& b : g.buf) {
                uncommitted.push_back(b.entry);
            }
            for (size_t i = 1; i < uncommitted.size(); ++i) {
                if (uncommitted[i]->idx != uncommitted[i - 1]->idx + raft::index_t{1}) {
                    on_internal_error(logger, fmt::format(
                            "group {}: gap in the replayed log between {} and {}",
                            group_id, uncommitted[i - 1]->idx, uncommitted[i]->idx));
                }
            }
            // Rewrite what is left as one batch, in the format
            // store_log_entries() writes, carrying the recovered floor. Its
            // records are the queue the group starts with, so these entries are
            // held by a reference and released by a descriptor exactly like the
            // ones appended after startup.
            auto handle = co_await service::strong_consistency::write_raft_batch(
                    *new_commitlog_ptr, g.table, group_id, g.commit_idx, uncommitted);
            service::strong_consistency::account_batch(group_data.records,
                    db::system_keyspace::raft_groups()->id(), std::move(handle), uncommitted);
            for (auto& entry : uncommitted) {
                group_data.entries.push_back(std::move(entry));
            }
        }
        logger.debug("group {}: floor=({}, {}), applied={}, dropped_stale={}, superseded={}, in_log={}",
                group_id, g.commit_idx, g.commit_term, g.applied, g.dropped_stale, g.superseded,
                group_data.entries.size());
        g.buf.clear();
    }
    _groups.clear();
    logger.info("Raft groups commit log replayed data processing complete");
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
