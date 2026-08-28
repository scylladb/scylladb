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

bool is_command(const raft::log_entry& e) {
    return std::holds_alternative<raft::command>(e.data);
}

bool is_config(const raft::log_entry& e) {
    return std::holds_alternative<raft::configuration>(e.data);
}
} // namespace

std::optional<raft::index_t> segment_record::last_cmd() const {
    // noncmd_indexes ascends, so the entries after the last command are a run
    // ending at max_index. Walk that run back: the first index not in it is the
    // last command.
    raft::index_t idx = max_index;
    for (auto it = noncmd_indexes.rbegin(); it != noncmd_indexes.rend() && *it == idx; ++it) {
        if (idx == first_index) {
            return std::nullopt;
        }
        --idx;
    }
    return idx;
}

void segment_record::trim_from(const raft::index_t idx) {
    max_index = idx - raft::index_t{1};
    // The first term run survives: a record is only clamped when first_index < idx.
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

// Write a batch whose writer the caller already built. Measuring is a full serialize
// into a measuring stream, so a caller that had to measure the batch to decide whether
// it fits passes the writer on rather than paying for a second pass.
static future<db::rp_handle> add_measured_raft_batch(db::commitlog& cl, table_id table,
        raft::group_id group_id, const raft::log_entry_ptr_list& entries,
        const commitlog_raft_batch_writer& writer) {
    const auto batch_size = writer.size();
    if (batch_size > cl.max_record_size()) {
        // See the declaration for why this is fatal.
        on_internal_error(logger, fmt::format(
                "raft batch of {} entries does not fit in one commitlog entry ({} > {}) for group_id={}",
                entries.size(), batch_size, cl.max_record_size(), group_id));
    }
    const auto write_fn = [&writer](auto& out) {
        return writer.write(out);
    };
    auto handle = co_await cl.add(table, batch_size, db::no_timeout, db::commitlog_force_sync::yes, write_fn);
    logger.debug("wrote raft batch: group_id={}, entries=[{}, {}], size={}, rp={}",
            group_id, entries.front()->idx, entries.back()->idx, batch_size, handle.rp());
    co_return handle;
}

future<db::rp_handle> write_raft_batch(db::commitlog& cl, table_id table,
        raft::group_id group_id, raft::index_t commit_idx, const raft::log_entry_ptr_list& entries) {
    SCYLLA_ASSERT(!entries.empty());
    // Awaited, not returned: the writer is a local and add_measured_raft_batch()
    // holds a reference to it across its suspension.
    const commitlog_raft_batch_writer writer(group_id, commit_idx, entries);
    co_return co_await add_measured_raft_batch(cl, table, group_id, entries, writer);
}

std::vector<size_t> split_raft_batch(const db::commitlog& cl, raft::group_id group_id,
        raft::index_t commit_idx, const raft::log_entry_ptr_list& entries) {
    const auto max_batch_size = cl.max_record_size();
    if (commitlog_raft_batch_writer(group_id, commit_idx, entries).size() <= max_batch_size) {
        return {entries.size()};
    }
    // Every size prefix in the encoding is fixed width, so a batch measures as its
    // header plus the sum of its entries. That makes one measurement per entry
    // enough, where measuring each candidate batch would be quadratic.
    const raft::log_entry_ptr_list no_entries;
    const auto header_size = commitlog_raft_batch_writer(group_id, commit_idx, no_entries).size();

    std::vector<size_t> batch_ends;
    auto current_batch_size = header_size;
    for (size_t i = 0; i < entries.size(); ++i) {
        const raft::log_entry_ptr_list one_entry{entries[i]};
        const auto entry_size =
                commitlog_raft_batch_writer(group_id, commit_idx, one_entry).size() - header_size;
        // An entry too large on its own still goes in a batch of its own, for
        // write_raft_batch() to reject: that is the bound the boot check covers.
        if (current_batch_size > header_size && current_batch_size + entry_size > max_batch_size) {
            batch_ends.push_back(i);
            current_batch_size = header_size;
        }
        current_batch_size += entry_size;
    }
    batch_ends.push_back(entries.size());
    return batch_ends;
}

void account_batch(std::deque<segment_record>& segment_queue, const db::cf_id_type& raft_groups_table_id,
        db::rp_handle&& handle, std::span<const raft::log_entry_ptr> entries) {
    if (entries.empty()) {
        return;
    }
    // Segment ids strictly increase, so a batch either extends the newest record
    // or starts a new one. Asserted rather than assumed: the "extend" branch below
    // would otherwise stretch the newest record over a range its segment never held.
    if (!segment_queue.empty() && handle.rp().id < segment_queue.back().segment()) {
        on_internal_error(logger, fmt::format(
                "raft batch landed in segment {}, below the newest record's segment {}",
                handle.rp().id, segment_queue.back().segment()));
    }
    if (segment_queue.empty() || handle.rp().id > segment_queue.back().segment()) {
        // Built whole before it joins the queue. A throw partway through would
        // otherwise leave a record with no pins and no terms at the front, and
        // that record passes every release gate: the next flush round would
        // persist index 0 over the group's real descriptor.
        segment_record fresh;
        fresh.pin_raft_groups = handle.clone(raft_groups_table_id);
        fresh.pin_user_table = std::move(handle);
        fresh.first_index = entries.front()->idx;
        fresh.terms.push_back(raft_term_and_index{.idx = entries.front()->idx, .term = entries.front()->term});
        segment_queue.push_back(std::move(fresh));
    }
    // If the batch landed in the same segment, the record and its pins already
    // exist and this batch's own handle is dropped.
    auto& record = segment_queue.back();
    record.max_index = entries.back()->idx;
    for (const auto& entry : entries) {
        if (record.terms.back().term != entry->term) {
            record.terms.push_back(raft_term_and_index{.idx = entry->idx, .term = entry->term});
        }
        if (is_config(*entry)) {
            record.configs.emplace_back(entry->idx, std::get<raft::configuration>(entry->data));
        }
        if (!is_command(*entry)) {
            record.noncmd_indexes.push_back(entry->idx);
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
    for (auto& record : _commitlog_segment_queue) {
        record.detach();
    }
    logger.debug("detached the references of {} records for group_id={}",
            _commitlog_segment_queue.size(), _group_id);
}

future<> raft_commitlog::store_log_entries(const std::vector<raft::log_entry_ptr>& entries,
        raft::index_t commit_idx) {
    if (entries.empty()) {
        co_return;
    }
    // One commitlog entry per batch. Measured here rather than inside the write, so
    // the common batch - one that fits - is serialized once to measure and once to
    // write, and never a third time to decide whether it needs splitting.
    const commitlog_raft_batch_writer writer(_group_id, commit_idx, entries);
    if (writer.size() <= _commit_log.max_record_size()) {
        auto handle = co_await add_measured_raft_batch(
                _commit_log, _table_id, _group_id, entries, writer);
        account_batch(_commitlog_segment_queue, _raft_groups_table_id, std::move(handle), entries);
        logger.debug("store_log_entries: group_id={}, entries=[{}, {}], segments={}",
                _group_id, entries.front()->idx, entries.back()->idx,
                _commitlog_segment_queue.size());
        co_return;
    }

    // Too large for one commitlog entry: as many whole batches as it takes, which is
    // also what replay's rewrite does. Splitting is what keeps a segment size too
    // small for a whole batch a supported configuration.
    const auto batch_ends = split_raft_batch(_commit_log, _group_id, commit_idx, entries);
    size_t batch_begin = 0;
    for (const auto batch_end : batch_ends) {
        const raft::log_entry_ptr_list batch(
                entries.begin() + batch_begin, entries.begin() + batch_end);
        auto handle = co_await write_raft_batch(_commit_log, _table_id, _group_id, commit_idx, batch);
        account_batch(_commitlog_segment_queue, _raft_groups_table_id, std::move(handle), batch);
        batch_begin = batch_end;
    }
    logger.debug("store_log_entries: group_id={}, entries=[{}, {}], batches={}, segments={}",
            _group_id, entries.front()->idx, entries.back()->idx, batch_ends.size(),
            _commitlog_segment_queue.size());
}

void raft_commitlog::truncate_log(const raft::index_t idx) {
    logger.debug("truncate_log: group_id={}, idx={}", _group_id, idx);
    // Raft calls this right before appending the conflicting entries, so the
    // copies being discarded are the current ones for their indexes.
    while (!_commitlog_segment_queue.empty() && _commitlog_segment_queue.back().first_index >= idx) {
        auto& record = _commitlog_segment_queue.back();
        _truncations.push_back(truncation_record{
                .segment = record.segment(), .from = record.first_index, .to = record.max_index});
        _commitlog_segment_queue.pop_back();
    }
    if (!_commitlog_segment_queue.empty() && _commitlog_segment_queue.back().max_index >= idx) {
        auto& record = _commitlog_segment_queue.back();
        _truncations.push_back(truncation_record{
                .segment = record.segment(), .from = idx, .to = record.max_index});
        record.trim_from(idx);
    }
}

db::rp_handle raft_commitlog::pin_for_apply(raft::index_t idx) {
    // The front record holds the index being applied: records cover ascending
    // disjoint ranges, commands apply in index order, and a release is attempted
    // after every apply and every commit-index advance. Raft awaits
    // store_commit_idx(), which releases, before handing new entries to the
    // applier (server_impl::process_fsm_output).
    if (_commitlog_segment_queue.empty() || idx < _commitlog_segment_queue.front().first_index
            || idx > _commitlog_segment_queue.front().max_index) {
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
    auto& record = _commitlog_segment_queue.front();
    // A record is final once no more of this group's entries can land in its
    // segment: a later record in the queue means a newer segment was allocated.
    // The last record has no successor and relies on the flush rounds, which name
    // only closed segments (see closed_up_to()).
    const bool is_final = _commitlog_segment_queue.size() > 1
            || record.pin_user_table.rp() <= closed_up_to();
    if (!is_final || record.max_index > commit_idx) {
        return nullptr;
    }
    if (const auto last_cmd = record.last_cmd(); last_cmd && *last_cmd > apply_idx) {
        return nullptr;
    }
    return &record;
}

void raft_commitlog::pop_released() {
    _commitlog_segment_queue.pop_front();
}

void raft_commitlog::purge_stale_truncations() {
    const db::segment_id_type oldest_segment = _commit_log.min_position().id;
    // The list is not sorted by segment (see _truncations), so a stale record can
    // sit after a live one.
    std::erase_if(_truncations, [oldest_segment](const truncation_record& t) {
        return t.segment < oldest_segment;
    });
}

raft::log_entries raft_commitlog::load_log() {
    return std::move(_replayed_entries);
}

} // namespace service::strong_consistency
