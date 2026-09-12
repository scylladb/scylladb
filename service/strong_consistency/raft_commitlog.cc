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
    // noncmd_indexes ascends, so walk it backwards in lockstep with the
    // candidate index.
    auto noncmd_it = noncmd_indexes.rbegin();
    for (auto idx = max.value(); idx >= first.value(); --idx) {
        while (noncmd_it != noncmd_indexes.rend() && noncmd_it->value() > idx) {
            ++noncmd_it;
        }
        if (noncmd_it == noncmd_indexes.rend() || noncmd_it->value() != idx) {
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
    while (!noncmd_indexes.empty() && noncmd_indexes.back() >= idx) {
        noncmd_indexes.pop_back();
    }
}

size_t max_single_entry_batch_size(size_t command_size) {
    // Measured, not summed from field widths, so it follows the format. Size
    // prefixes are fixed width, so one measurement of an empty command is enough.
    static thread_local const size_t envelope = [] {
        auto entry = seastar::make_lw_shared<const raft::log_entry>(raft::log_entry{
                .term = raft::term_t{1},
                .idx = raft::index_t{1},
                .data = raft::command{},
                .lease_time = raft::time_bounds{
                        raft::lease_clock::time_point(std::chrono::nanoseconds(1)),
                        raft::lease_clock::time_point(std::chrono::nanoseconds(2))}});
        const std::vector<raft::log_entry_ptr> entries{std::move(entry)};
        return commitlog_raft_batch_writer(raft::group_id{}, raft::index_t{0}, entries).size();
    }();
    return envelope + command_size;
}

void check_commitlog_can_hold_a_raft_entry(const db::commitlog& cl) {
    const auto largest_entry = max_single_entry_batch_size(raft_max_command_size);
    if (largest_entry > cl.max_record_size()) {
        throw std::runtime_error(fmt::format(
                "cannot start with strongly consistent tables enabled and "
                "commitlog_segment_size_in_mb={}: one raft entry with a command of "
                "max_command_size ({} bytes) needs {} bytes on disk, and a commitlog "
                "entry holds at most {}. Raise the segment size or lower "
                "max_command_size.",
                cl.active_config().commitlog_segment_size_in_mb,
                raft_max_command_size, largest_entry, cl.max_record_size()));
    }
    if (this_shard_id() == 0 && raft_max_log_size + raft_max_command_size > cl.max_record_size()) {
        logger.warn("commitlog_segment_size_in_mb={} leaves a commitlog entry ({} bytes) "
                "smaller than a raft batch can be ({} + {} bytes); a large batch will "
                "abort the node. A single entry still fits.",
                cl.active_config().commitlog_segment_size_in_mb, cl.max_record_size(),
                raft_max_log_size, raft_max_command_size);
    }
}

future<db::rp_handle> write_raft_batch(db::commitlog& cl, table_id table,
        raft::group_id group_id, raft::index_t commit_idx, const raft::log_entry_ptr_list& entries) {
    commitlog_raft_batch_writer writer(group_id, commit_idx, entries);
    const auto size = writer.size();
    if (size > cl.max_record_size()) {
        // See the declaration for why this is fatal.
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

void account_batch(std::deque<segment_record>& segment_queue, const db::cf_id_type& raft_groups_table_id,
        db::rp_handle&& handle, std::span<const raft::log_entry_ptr> entries) {
    if (entries.empty()) {
        return;
    }
    // Segment ids strictly increase, so a batch either extends the newest record
    // or starts a new one.
    if (segment_queue.empty() || handle.rp().id > segment_queue.back().segment()) {
        auto& rec = segment_queue.emplace_back();
        rec.pin_raft_groups = handle.clone(raft_groups_table_id);
        rec.pin_user_table = std::move(handle);
        rec.first = entries.front()->idx;
        rec.terms.push_back(raft_term_and_index{.idx = entries.front()->idx, .term = entries.front()->term});
    }
    // If the batch landed in the same segment, the record and its pins already
    // exist and this batch's own handle is dropped.
    auto& rec = segment_queue.back();
    rec.max = entries.back()->idx;
    for (const auto& entry : entries) {
        if (rec.terms.back().term != entry->term) {
            rec.terms.push_back(raft_term_and_index{.idx = entry->idx, .term = entry->term});
        }
        if (is_config(*entry)) {
            rec.configs.emplace_back(entry->idx, std::get<raft::configuration>(entry->data));
        }
        if (!is_command(*entry)) {
            rec.noncmd_indexes.push_back(entry->idx);
        }
    }
}

raft_commitlog::raft_commitlog(raft::group_id group_id, db::commitlog& commit_log, table_id target_table_id,
        db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data)
    : _group_id(group_id)
    , _table_id(target_table_id)
    , _raft_groups_table_id(raft_groups_table_id)
    , _commit_log(commit_log)
    , _commitlog_segment_queue(std::move(replayed_data.records))
    , _replayed_entries(std::move(replayed_data.entries)) {
    logger.debug("raft_commitlog group_id={}: replayed_entries={}, seeded_records={}",
            _group_id, _replayed_entries.size(), _commitlog_segment_queue.size());
}

raft_commitlog::~raft_commitlog() {
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
    // The front record holds the index being applied: records cover ascending
    // disjoint ranges, commands apply in index order, and a release is attempted
    // after every apply and every commit-index advance. Raft awaits
    // store_commit_idx(), which releases, before handing new entries to the
    // applier (server_impl::process_fsm_output).
    if (_commitlog_segment_queue.empty() || idx < _commitlog_segment_queue.front().first
            || idx > _commitlog_segment_queue.front().max) {
        on_internal_error(logger, fmt::format("no record holds idx={} for group_id={} (segments={})",
                idx, _group_id, _commitlog_segment_queue.size()));
    }
    return _commitlog_segment_queue.front().pin_user_table.clone(_table_id);
}

void raft_commitlog::mark_segment_closed(db::replay_position pos) {
    _reported_up_to = std::max(_reported_up_to, pos);
}

segment_record* raft_commitlog::front_releasable(raft::index_t commit_idx, raft::index_t apply_idx) {
    if (_commitlog_segment_queue.empty()) {
        return nullptr;
    }
    auto& rec = _commitlog_segment_queue.front();
    // A record is final once no more of this group's entries can land in its
    // segment: a later record in the queue means a newer segment was allocated.
    // The last record has no successor and relies on the flush rounds, which name
    // only closed segments (see closed_up_to()).
    const bool final = _commitlog_segment_queue.size() > 1
            || rec.pin_user_table.rp() <= closed_up_to();
    if (!final || rec.max > commit_idx) {
        return nullptr;
    }
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
    // The list is not sorted by segment (see _truncations), so a stale record can
    // sit after a live one.
    std::erase_if(_truncations, [oldest](const truncation_record& t) {
        return t.segment < oldest;
    });
}

std::optional<db::replay_position> raft_commitlog::flush_needed_on_release_all() const {
    const auto reported = closed_up_to();
    std::optional<db::segment_id_type> newest;
    // Oldest first, and segment ids increase, so the last match is the newest.
    for (const auto& rec : _commitlog_segment_queue) {
        // With no command, apply() got no reference into this segment, so the
        // tablet table holds nothing there.
        if (rec.last_cmd() && rec.pin_user_table.rp() <= reported) {
            newest = rec.segment();
        }
    }
    if (!newest) {
        return std::nullopt;
    }
    return db::replay_position(*newest + 1, 0);
}

void raft_commitlog::release_all() {
    // Destroying the handles decrements the segments' use counts, and the emptied
    // queue leaves the destructor nothing to detach.
    const auto released = _commitlog_segment_queue.size();
    _commitlog_segment_queue.clear();
    _truncations.clear();
    logger.debug("released the references of {} records for group_id={}", released, _group_id);
}

raft::log_entries raft_commitlog::load_log() {
    return std::move(_replayed_entries);
}

} // namespace service::strong_consistency
