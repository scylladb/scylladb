/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/on_internal_error.hh>
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/system_keyspace.hh"
#include "raft/raft.hh"

#include <ranges>

#include "raft_commitlog.hh"

#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"

namespace service::strong_consistency {
namespace {
seastar::logger logger("raft_commitlog");

bool is_command(const raft::log_entry& e) {
    return std::holds_alternative<raft::command>(e.data);
}

bool is_config(const raft::log_entry& e) {
    return std::holds_alternative<raft::configuration>(e.data);
}
} // namespace

std::optional<raft::index_t> segment_record::last_cmd() const {
    // noncmd ascends, so walk it backwards in lockstep with the candidate
    // index. Both sequences are short: one element per non-command entry.
    auto it = noncmd.rbegin();
    for (auto idx = max.value(); idx >= first.value(); --idx) {
        while (it != noncmd.rend() && it->value() > idx) {
            ++it;
        }
        if (it == noncmd.rend() || it->value() != idx) {
            return raft::index_t{idx};
        }
        if (idx == 0) {
            break;
        }
    }
    return std::nullopt;
}

void segment_record::trim_from(const raft::index_t idx) {
    max = idx - raft::index_t{1};
    // The first term run survives: a record is only clamped when first < idx.
    while (terms.size() > 1 && terms.back().idx >= idx) {
        terms.pop_back();
    }
    while (!configs.empty() && configs.back().first >= idx) {
        configs.pop_back();
    }
    while (!noncmd.empty() && noncmd.back() >= idx) {
        noncmd.pop_back();
    }
}

future<db::rp_handle> write_raft_batch(db::commitlog& cl, table_id table,
        raft::group_id group_id, raft::index_t commit_idx, const raft::log_entry_ptr_list& entries) {
    commitlog_raft_batch_writer writer(raft_commitlog_batch{
            .group_id = group_id, .commit_idx = commit_idx,
            .entries = std::vector<raft::log_entry_ptr>(entries.begin(), entries.end())});
    const auto size = writer.size();
    if (size > cl.max_record_size()) {
        // Splitting is not an option: see write_raft_batch's declaration.
        on_internal_error(logger, fmt::format(
                "raft batch of {} entries does not fit in one commitlog entry ({} > {}) for group_id={}",
                entries.size(), size, cl.max_record_size(), group_id));
    }
    const auto write_fn = [&writer](auto& out) {
        return writer.write(out);
    };
    auto handle = co_await cl.add(table, size, db::no_timeout, db::commitlog_force_sync::yes, write_fn);
    logger.debug("wrote raft batch: group_id={}, entries=[{}, {}], commit_idx={}, size={}, rp={}",
            group_id, entries.front()->idx, entries.back()->idx, commit_idx, size, handle.rp());
    co_return handle;
}

void account_batch(std::deque<segment_record>& queue, const db::cf_id_type& raft_groups_table_id,
        db::rp_handle&& handle, std::span<const raft::log_entry_ptr> entries) {
    if (entries.empty()) {
        return;
    }
    // Segment ids strictly increase, so a batch either extends the newest record
    // or starts a new one. Comparing ids rather than positions is what makes a
    // batch landing in the still-allocating segment the common, allocation-free
    // path.
    if (queue.empty() || handle.rp().id > queue.back().segment()) {
        auto& rec = queue.emplace_back();
        rec.pin_rg = handle.clone(raft_groups_table_id);
        rec.pin_table = std::move(handle);
        rec.first = entries.front()->idx;
        rec.terms.push_back(raft_term_and_index{.idx = entries.front()->idx, .term = entries.front()->term});
    }
    // ...otherwise this batch's own reference is redundant: the record already
    // holds one for this segment, and retention is per segment.
    auto& rec = queue.back();
    rec.max = entries.back()->idx;
    for (const auto& e : entries) {
        if (rec.terms.back().term != e->term) {
            rec.terms.push_back(raft_term_and_index{.idx = e->idx, .term = e->term});
        }
        if (is_config(*e)) {
            rec.configs.emplace_back(e->idx, std::get<raft::configuration>(e->data));
        }
        if (!is_command(*e)) {
            rec.noncmd.push_back(e->idx);
        }
    }
}

raft_commitlog::raft_commitlog(raft::group_id group_id, db::commitlog& commit_log, table_id target_table_id,
        db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data)
    : _group_id(group_id)
    , _table_id(target_table_id)
    , _raft_groups_table_id(raft_groups_table_id)
    , _commit_log(commit_log)
    // Commitlog replay rewrote this group's uncommitted entries; the records for
    // that write are the queue the group starts with, so those entries are held
    // and released like any others.
    , _commitlog_segment_queue(std::move(replayed_data.records))
    , _replayed_entries(std::move(replayed_data.entries)) {
    logger.debug("raft_commitlog group_id={}: replayed_entries={}, seeded_records={}",
            _group_id, _replayed_entries.size(), _commitlog_segment_queue.size());
}

raft_commitlog::~raft_commitlog() {
    // Detach rather than release: the segments must survive this object so that
    // the entries they hold are still there to be replayed after a restart.
    for (auto& rec : _commitlog_segment_queue) {
        rec.detach();
    }
    logger.debug("detached the references of {} records for group_id={}",
            _commitlog_segment_queue.size(), _group_id);
}

future<> raft_commitlog::store_log_entries(const std::vector<raft::log_entry_ptr>& entries,
        raft::index_t commit_idx) {
    if (entries.empty()) {
        co_return;
    }
    auto handle = co_await write_raft_batch(_commit_log, _table_id, _group_id, commit_idx, entries);
    account_batch(_commitlog_segment_queue, _raft_groups_table_id, std::move(handle), entries);
    logger.debug("store_log_entries: group_id={}, entries=[{}, {}], segments={}",
            _group_id, entries.front()->idx, entries.back()->idx, _commitlog_segment_queue.size());
}

void raft_commitlog::truncate_log(const raft::index_t idx) {
    logger.debug("truncate_log: group_id={}, idx={}", _group_id, idx);
    // Raft calls this right before appending the conflicting entries, so the
    // copies being discarded are the current ones for their indexes.
    while (!_commitlog_segment_queue.empty() && _commitlog_segment_queue.back().first >= idx) {
        auto& rec = _commitlog_segment_queue.back();
        _truncations.push_back(truncation_record{
                .segment = rec.segment(), .from = rec.first, .to = rec.max});
        _commitlog_segment_queue.pop_back();
    }
    if (!_commitlog_segment_queue.empty() && _commitlog_segment_queue.back().max >= idx) {
        auto& rec = _commitlog_segment_queue.back();
        _truncations.push_back(truncation_record{
                .segment = rec.segment(), .from = idx, .to = rec.max});
        rec.trim_from(idx);
    }
}

db::rp_handle raft_commitlog::pin_for_apply(raft::index_t idx) {
    // The front record always holds the index being applied: records cover
    // ascending disjoint ranges, commands are applied in index order, and a
    // release is attempted after every apply and every commit-index advance — so
    // by the time a later record's entry is applied, an older one has already met
    // every condition in front_releasable(). The one subtlety is a record that
    // was the newest when its last command was applied: alone in the queue it
    // could not be released, but raft awaits store_commit_idx(), which releases,
    // before handing new entries to the applier
    // (server_impl::process_fsm_output), so its successor's release runs first.
    if (_commitlog_segment_queue.empty() || idx < _commitlog_segment_queue.front().first
            || idx > _commitlog_segment_queue.front().max) {
        on_internal_error(logger, fmt::format("no record holds idx={} for group_id={} (segments={})",
                idx, _group_id, _commitlog_segment_queue.size()));
    }
    return _commitlog_segment_queue.front().pin_table.clone(_table_id);
}

void raft_commitlog::mark_segment_closed(db::replay_position pos) {
    if (_closed_up_to < pos) {
        _closed_up_to = pos;
    }
}

segment_record* raft_commitlog::front_releasable(raft::index_t commit_idx, raft::index_t apply_idx) {
    if (_commitlog_segment_queue.empty()) {
        return nullptr;
    }
    auto& rec = _commitlog_segment_queue.front();
    // A record is final once no more of this group's entries can land in its
    // segment. Two signals say so. The group's own writes: a later record in
    // this queue means a newer segment was allocated, so this one is closed —
    // under steady writes that covers every record but the last. And for the
    // last record, whose group may have stopped writing, the commitlog's flush
    // rounds: they name only closed segments, so a reported position at or past
    // this record's says its segment is closed (mark_segment_closed).
    const bool final = _commitlog_segment_queue.size() > 1
            || rec.pin_table.rp() <= _closed_up_to;
    if (!final || rec.max > commit_idx) {
        return nullptr;
    }
    // Dummies and configurations never reach apply(), so waiting for `max` would
    // wait forever; the gate is the record's last command.
    if (const auto last = rec.last_cmd(); last && *last > apply_idx) {
        return nullptr;
    }
    return &rec;
}

void raft_commitlog::pop_released() {
    _commitlog_segment_queue.pop_front();
}

void raft_commitlog::purge_stale_truncations() {
    const auto oldest = _commit_log.min_position().id;
    // Every record, not just a leading run of them: the list is not sorted by
    // segment (see _truncations), so a stale record can sit after a live one.
    std::erase_if(_truncations, [oldest](const truncation_record& t) {
        return t.segment < oldest;
    });
}

raft::log_entries raft_commitlog::load_log() {
    return std::move(_replayed_entries);
}

} // namespace service::strong_consistency
