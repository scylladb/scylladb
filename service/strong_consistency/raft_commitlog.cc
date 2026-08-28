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

#include <limits>

#include "raft_commitlog.hh"

#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"

namespace service::strong_consistency {
namespace {
seastar::logger logger("raft_commitlog");

// Command entries are the ones state_machine::apply() consumes; their
// handles move to the target table's memtable there.
bool is_command_entry(const raft::log_entry& e) {
    return std::holds_alternative<raft::command>(e.data);
}

} // namespace

raft_commitlog::raft_commitlog(raft::group_id group_id, db::commitlog& commitlog, table_id target_table_id,
        db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data)
    : _group_id(group_id)
    , _table_id(target_table_id)
    , _raft_groups_table_id(raft_groups_table_id)
    , _commit_log(commitlog)
    , _replayed_entries(std::move(replayed_data.entries)) {
    _pending_covers = std::move(replayed_data.covers);
    // The rewritten batch, if replay rewrote anything, holds the reference on the
    // entries above the recovered commit index. Its range is what tells us
    // which replayed entries those are: everything below was already applied
    // by replay and needs nothing from us.
    const auto rewritten_from = replayed_data.rewritten
            ? replayed_data.rewritten->first
            : raft::index_t(std::numeric_limits<uint64_t>::max());
    if (replayed_data.rewritten) {
        _batch_refs.push_back(std::move(*replayed_data.rewritten));
    }
    for (const auto& e : _replayed_entries) {
        _appended_terms.push_back(raft_term_and_index{.idx = e->idx, .term = e->term});
        if (e->idx < rewritten_from) {
            continue;
        }
        if (is_command_entry(*e)) {
            _pending_commands.push_back(e->idx);
        }
    }
    logger.debug("starting raft_commitlog group_id={}, table_id={}, replayed_entries={}, "
            "rewritten_from={}, pending_commands={}, pending_covers={}",
            _group_id, _table_id, _replayed_entries.size(), rewritten_from,
            _pending_commands.size(), _pending_covers.size());
}

raft_commitlog::~raft_commitlog() {
    // Release every remaining reference without decrementing segment dirty counts.
    // This keeps the commitlog segments alive after this object is destroyed,
    // so the uncommitted raft entries (and the commit indexes their batch
    // headers carry) remain available for replay after a restart.
    for (auto& batch : _batch_refs) {
        batch.reference.release();
    }
    for (auto& [segment, cover] : _pending_covers) {
        cover.segment_holder.release();
    }
    for (auto& orphan : _orphaned_holders) {
        orphan.segment_holder.release();
    }
    logger.debug("released {} batch references, {} parked covers and {} orphaned holders for group_id={}",
            _batch_refs.size(), _pending_covers.size(), _orphaned_holders.size(), _group_id);
}

seastar::future<db::rp_handle> raft_commitlog::write_batches(db::commitlog& cl,
        db::cf_id_type table_id, db::cf_id_type raft_groups_table_id, raft::group_id group_id,
        const raft::log_entry_ptr_list& entries, raft_term_and_index committed, pending_cover_map& covers) {
    SCYLLA_ASSERT(!entries.empty());

    // One commitlog entry for the whole batch: the group id, the entries, and
    // the group's last committed (index, term) as the crash-replay floor. The
    // per-entry framing this replaces repeated the group id and an envelope
    // for every entry, and paid for a separate trailing record whose only
    // novel payload was one index.
    utils::chunked_vector<commitlog_raft_log_entry_writer> writers;
    writers.emplace_back(raft_commitlog_entry{
            .group_id = group_id,
            .entries = {entries.begin(), entries.end()},
            .commit_idx = committed.idx,
            .commit_idx_term = committed.term});
    if (logger.is_enabled(seastar::log_level::debug)) {
        logger.debug("  storing batch: group_id={}, idx={}..{}, committed=({}, {})", group_id,
                entries.front()->idx, entries.back()->idx, committed.idx, committed.term);
    }

    // A single write, force_sync. Below the commitlog's oversized threshold
    // (half a segment: 32MB at the default 64MB segment size) the batch is
    // placed atomically in one segment. Raft caps a batch at max_log_size
    // (20MB for tablet groups), so at the default segment size the threshold
    // is unreachable; with smaller configured segments the commitlog
    // fragments the entry across segments, which requires
    // allow_fragmented_entries (the fragmented_commitlog_entries cluster
    // feature; strongly consistent tables are gated behind an experimental
    // flag, so in practice it is on). Either way the batch has exactly one
    // position — its head segment — and the tail segments of a fragmented
    // entry outlive every reference taken at that position, because the head
    // segment counts the references holding them (segment::extended_entry).
    auto handles = co_await cl.add_raft_entries(table_id, std::move(writers));
    SCYLLA_ASSERT(handles.size() == 1);
    auto batch_handle = std::move(handles[0]);

    // The segment must also be held on behalf of system.raft_groups: without
    // that, a target-table flush could reclaim it before a covering commit_idx
    // value is durable. One reference per segment, not per batch — a segment
    // already in the caller's cover map keeps the reference it has (it has existed
    // continuously since the batch that first wrote to it, which is the
    // property the coverage relies on) and only its (max_idx, max_term) pair
    // advances. Batches keep landing in the same still-active segment, so that
    // is the common case.
    //
    // Entries ascend, so the batch's last entry carries the highest index the
    // group has put in this segment.
    const auto segment = batch_handle.rp().id;
    auto [it, inserted] = covers.try_emplace(segment);
    it->second.max_idx = entries.back()->idx;
    it->second.max_term = entries.back()->term;
    if (inserted) {
        it->second.segment_holder = cl.acquire_cf_count(batch_handle, raft_groups_table_id);
    }

    co_return batch_handle;
}

seastar::future<> raft_commitlog::store_log_entries(const raft::log_entry_ptr_list& entries) {
    logger.debug("store_log_entries: group_id={}, num_entries={}", _group_id, entries.size());
    if (entries.empty()) {
        co_return;
    }

    auto batch_handle = co_await write_batches(_commit_log, _table_id, _raft_groups_table_id, _group_id, entries,
            _last_committed, _pending_covers);

    bool has_commands = false;
    for (const auto& log_entry_ptr : entries) {
        _appended_terms.push_back(raft_term_and_index{.idx = log_entry_ptr->idx, .term = log_entry_ptr->term});
        if (is_command_entry(*log_entry_ptr)) {
            _pending_commands.push_back(log_entry_ptr->idx);
            has_commands = true;
        }
    }
    // Only a batch containing commands needs its reference kept: apply() mints
    // from it, and nothing else will. A batch of dummies and configurations is
    // outlived by its segment's cover (see batch_ref), so its reference goes
    // here.
    if (has_commands) {
        _batch_refs.push_back(batch_ref{
                .first = entries.front()->idx,
                .last = entries.back()->idx,
                .reference = std::move(batch_handle)});
    }
    logger.debug("store_log_entries completed: batch_refs={}, pending_commands={}, pending_covers={}",
            _batch_refs.size(), _pending_commands.size(), _pending_covers.size());
}

void raft_commitlog::note_commit_idx(const raft::index_t idx) {
    // Consume the appended entries the commit index has reached; the last one
    // consumed is the pair the next batch records as its floor. Raft advances
    // the commit index before the entries are applied, so every entry at or
    // below idx is still tracked here — no lookup can miss.
    while (!_appended_terms.empty() && _appended_terms.front().idx <= idx) {
        _last_committed = _appended_terms.front();
        _appended_terms.pop_front();
    }
}

raft::log_entries raft_commitlog::load_log() {
    logger.debug("load_log: group_id={}", _group_id);
    return std::move(_replayed_entries);
}

void raft_commitlog::truncate_log(const raft::index_t idx) {
    logger.debug("truncate_log: group_id={}, idx={}", _group_id, idx);
    // Remove entries with index >= idx (Raft semantics: truncate from idx
    // onward). Everything here is index-ordered, so each is a back trim.
    //
    // The discarded commands will never be applied, so nothing will ever mint
    // a memtable reference for them.
    while (!_pending_commands.empty() && _pending_commands.back() >= idx) {
        _pending_commands.pop_back();
    }
    // A batch entirely at or above the truncation point is gone: its reference can
    // go with it, because its entries are discarded and its segment's
    // retention for anything else is the business of that segment's cover.
    while (!_batch_refs.empty() && _batch_refs.back().first >= idx) {
        _batch_refs.pop_back();
    }
    // A batch straddling the truncation point keeps its reference — its surviving
    // commands may still be waiting for apply() — with its range clamped so
    // the drain check below only looks at what survived.
    if (!_batch_refs.empty() && _batch_refs.back().last >= idx) {
        _batch_refs.back().last = idx - raft::index_t{1};
    }
    // ...and if nothing survived that needs it, it goes too: the surviving
    // entries are then dummies, configurations, or commands already handed to
    // a memtable, none of which depend on this reference.
    while (!_batch_refs.empty()
            && (_pending_commands.empty() || _pending_commands.back() < _batch_refs.back().first)) {
        _batch_refs.pop_back();
    }
    // The discarded entries can no longer be committed, so their terms must
    // not end up in a floor, and their configurations must not be persisted.
    while (!_appended_terms.empty() && _appended_terms.back().idx >= idx) {
        _appended_terms.pop_back();
    }
    // Covers whose max_idx refers to truncated entries can never pop with a
    // correct (idx, term) pair, and leaving them parked would wedge
    // take_committed_covers()'s prefix pop for every later cover (the reused
    // indexes land in new, smaller-max covers behind them). Their references
    // must survive, though — the segments may still hold this group's earlier
    // entries — so the references move to _orphaned_holders, each tagged with the
    // release floor it must wait for: idx - 1 bounds the live entries left
    // in its segment from above, and a lower value must not release it (a
    // surviving pre-truncation cover can pop first, and its value would leave
    // entries between it and the truncation point uncovered).
    while (!_pending_covers.empty() && std::prev(_pending_covers.end())->second.max_idx >= idx) {
        const auto last = std::prev(_pending_covers.end());
        if (last->second.segment_holder) {
            _orphaned_holders.push_back(orphaned_holder{
                .release_floor = idx - raft::index_t{1},
                .segment_holder = std::move(last->second.segment_holder),
            });
        }
        _pending_covers.erase(last);
    }
}

void raft_commitlog::truncate_log_tail(const raft::index_t index) {
    logger.debug("truncate_log_tail: group_id={}, index={}", _group_id, index);
    // Entries at or below a persisted snapshot index are all applied — raft
    // does not snapshot past what the state machine consumed — so nothing here
    // is still waiting on them. Front trims, and the references are destructed
    // normally, decrementing segment dirty counts and letting segments be
    // reclaimed once nothing else holds them.
    while (!_pending_commands.empty() && _pending_commands.front() <= index) {
        _pending_commands.pop_front();
    }
    while (!_batch_refs.empty() && _batch_refs.front().last <= index) {
        _batch_refs.pop_front();
    }
    // _pending_covers is left alone: covers up to the snapshot index (<= the
    // applied <= the commit index) were already taken by
    // take_committed_covers(), which store_commit_idx() drives before entries
    // can be applied, let alone snapshotted; segments still pending extend
    // beyond it.
    logger.debug("truncate_log_tail completed: batch_refs={}, pending_commands={}",
            _batch_refs.size(), _pending_commands.size());
}

std::vector<segment_cover> raft_commitlog::take_committed_covers(raft::index_t commit_idx) {
    std::vector<segment_cover> ret;
    // The map's key order is also its max_idx order (see pending_cover_map),
    // so the fully-committed covers form a prefix.
    while (!_pending_covers.empty() && _pending_covers.begin()->second.max_idx <= commit_idx) {
        auto node = _pending_covers.extract(_pending_covers.begin());
        auto& cover = node.mapped();
        if (!cover.segment_holder) {
            // Replayed data constructed without real handles (tests); there
            // is no reference to manage and nothing for a pin to guarantee.
            continue;
        }
        ret.push_back(segment_cover{
            .segment = node.key(),
            .max_idx = cover.max_idx,
            .max_term = cover.max_term,
            .segment_holder = std::move(cover.segment_holder),
        });
    }
    if (!ret.empty()) {
        // References parked by truncate_log() whose floor this pop reaches ride
        // the last cover's mutation: its value is >= their floors, hence >=
        // any index remaining in their segments. References with higher floors
        // stay parked for a later pop.
        for (auto it = _orphaned_holders.begin(); it != _orphaned_holders.end(); ) {
            if (it->release_floor <= ret.back().max_idx) {
                ret.back().extra_holders.push_back(std::move(it->segment_holder));
                it = _orphaned_holders.erase(it);
            } else {
                ++it;
            }
        }
    }
    if (!ret.empty()) {
        logger.debug("take_committed_covers: group_id={}, commit_idx={}, covers={}, remaining_pending={}",
                _group_id, commit_idx, ret.size(), _pending_covers.size());
    }
    return ret;
}

std::vector<index_and_replay_position> raft_commitlog::acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries) {
    logger.debug("acquire_replay_position_handles_for: group_id={}, entries_count={}, pending_commands={}",
            _group_id, entries.size(), _pending_commands.size());

    std::vector<index_and_replay_position> ret;
    ret.reserve(entries.size());

    // Both sequences are command-only and index-sorted, so a single forward
    // walk matches them up, as does a second one over the batches. A pending
    // command below the requested one is left where it is rather than dropped:
    // its data never reached a memtable, so its batch's reference must go on
    // holding the segment.
    auto pending = _pending_commands.begin();
    auto batch = _batch_refs.begin();
    for (const auto& entry : entries) {
        while (pending != _pending_commands.end() && *pending < entry->idx) {
            ++pending;
        }
        if (pending == _pending_commands.end() || *pending != entry->idx) {
            on_internal_error(logger, fmt::format("no pending command for group_id={}, idx={}", _group_id, entry->idx));
        }
        while (batch != _batch_refs.end() && batch->last < entry->idx) {
            ++batch;
        }
        if (batch == _batch_refs.end() || batch->first > entry->idx) {
            on_internal_error(logger, fmt::format("no batch reference for group_id={}, idx={}", _group_id, entry->idx));
        }
        // The minted handle carries the batch's position — the position the
        // mutation was really written at — so the target memtable's recorded
        // replay position, and the truncation and cleanup records derived from
        // it, stay truthful.
        ret.emplace_back(index_and_replay_position{
                .index = entry->idx,
                .replay_position_handle = _commit_log.acquire_cf_count(batch->reference, _table_id)});
        pending = _pending_commands.erase(pending);
    }

    // A batch with no pending command left has nothing more to hand out: the
    // memtables hold references of their own now, and its other entry kinds are
    // outlived by the segment's cover. Front only — commands are applied in
    // index order, so earlier batches drain first.
    while (!_batch_refs.empty()
            && (_pending_commands.empty() || _pending_commands.front() > _batch_refs.front().last)) {
        _batch_refs.pop_front();
    }

    logger.debug("acquire_replay_position_handles_for completed: returned={}, batch_refs={}, pending_commands={}",
            ret.size(), _batch_refs.size(), _pending_commands.size());
    return ret;
}
} // namespace service::strong_consistency
