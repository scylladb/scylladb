/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include "raft/raft.hh"
#include "db/commitlog/commitlog.hh"
#include <deque>
#include <map>

namespace service::strong_consistency {

// A db::rp_handle held here is one reference in a commitlog segment's use
// count: the segment cannot be discarded or recycled while it exists. See
// rp_handle in db/commitlog/replay_position.hh for the three ways a reference
// is given up — in particular that rp_handle::release() keeps the segment
// rather than releasing it.

struct index_and_replay_position {
    raft::index_t index;
    db::rp_handle replay_position_handle;
};

// A segment's raft_groups-accounted reference together with the highest raft
// index the group has written to that segment so far (the pending_cover_map
// value; the segment is the key), parked until every entry at or below that
// index is committed. The reference is taken (commitlog::acquire_cf_count) when
// the group first writes to the segment and pins it from that moment until
// take_committed_covers() attaches it to a covering raft_groups mutation.
struct pending_cover {
    // Highest raft index the group has written to the reference's segment, and
    // that entry's term. The pair is persisted together by the covering
    // mutation; the term must be exact — see make_raft_schema()'s
    // commit_idx_term comment.
    raft::index_t max_idx;
    raft::term_t max_term;
    db::rp_handle segment_holder;
};

// A group's covers, keyed by commitlog segment: one reference per segment,
// updated in place by write_batches(). Batches arrive in index order and
// segments advance monotonically, so the key order is also the max_idx
// order — the fully-committed covers always form a prefix
// (take_committed_covers()) and the covers a truncation invalidates form a
// suffix (truncate_log()).
using pending_cover_map = std::map<db::segment_id_type, pending_cover>;

// One reference on the commitlog entry a batch was written as, with the batch's
// raft index range. It is what keeps the segment alive between the write and
// apply(): the segment's cover can be released by a raft_groups flush before
// the applier fiber runs, and until every command in the batch has reached a
// memtable this reference is the only thing left holding the segment.
//
// One reference serves the whole batch because the batch is one commitlog entry,
// so all of its entries share a position; apply() mints a per-command handle
// from it (commitlog::acquire_cf_count), which then carries that same
// position into the target memtable.
//
// Dummy and configuration entries need no reference of their own. The segment's
// cover has max_idx >= their index, so it is released only once a durable
// commit index covers them, by which point replay does not need the entries;
// and a committed configuration is persisted by the very mutation that
// carries the cover's reference (take_committed_config()).
struct batch_ref {
    raft::index_t first;
    raft::index_t last;
    db::rp_handle reference;
};

struct replayed_data_per_group {
    // The reference on the batch the commitlog replay rewrote this group's
    // uncommitted entries as, if it rewrote any. Its range says which of
    // `entries` are the rewritten (uncommitted) ones.
    std::optional<batch_ref> rewritten;
    raft::log_entries entries;
    // The cover map write_batches() built while the commitlog replay
    // rewrote the group's batches into the new commitlog; moved wholesale
    // into the group's _pending_covers on construction.
    pending_cover_map covers;
};

// A commitlog segment all of whose entries (for this group) are committed,
// together with the highest raft index the group appended to it and the reference
// that has held the segment since the group first wrote to it. Produced by
// take_committed_covers(); see its comment for the contract.
struct segment_cover {
    db::segment_id_type segment;
    raft::index_t max_idx;
    raft::term_t max_term;
    db::rp_handle segment_holder;
    // References of covers removed by truncate_log() (see _orphaned_holders),
    // attached once this cover's max_idx reaches their floors. Each must
    // enter the raft_groups memtable with the same covering mutation as
    // segment_holder.
    std::vector<db::rp_handle> extra_holders;
};

// This class implements the persistence for raft log using database commit log.
// It is used by tablet raft groups to persist their log entries.
//
// Segment lifetime. A commitlog segment holding raft entries may be reclaimed
// only once a durable system.raft_groups commit_idx value >= the highest
// applied index in that segment exists — otherwise a restart would recover a
// commit index behind data already flushed to SSTables. That invariant is
// maintained with plain dirty-count references and no flush hooks:
//  - the first time the group writes to a segment, a raft_groups-accounted
//    reference for it is taken (commitlog::acquire_cf_count) and
//    parked here (_pending_covers), pinning the segment from that moment;
//  - applied command entries pin their segment through the target table's
//    memtable (their handles move there in apply()), released by that
//    memtable's flush once the data is durable;
//  - once every entry in a segment is committed, a covering
//    system.raft_groups mutation carrying the segment's max index enters the
//    raft_groups memtable together with the parked reference
//    (take_committed_covers(), driven by store_commit_idx()), released by
//    that memtable's flush once the value is durable.
// The reference exists continuously from write to the raft_groups flush that
// persists its covering value, so there is no window in which the target
// table's memtable is a segment's sole owner.
class raft_commitlog {
private:
    const raft::group_id _group_id;
    const db::cf_id_type _table_id;
    // cf the segments' references are minted under (system.raft_groups): the
    // covering mutation rides that table's memtable, so its flush both
    // persists the value and releases the references it carried.
    const db::cf_id_type _raft_groups_table_id;
    // Common commit log.
    db::commitlog& _commit_log;
    // One reference per appended batch, in index order (see batch_ref). Held
    // until every command in the batch has been handed to a memtable by
    // acquire_replay_position_handles_for(); a batch with no commands at all
    // never needs one. truncate_log() drops the batches a leader change
    // discarded and clamps a straddling one; truncate_log_tail() drops what a
    // snapshot covered.
    std::deque<batch_ref> _batch_refs;
    // Indexes of the command entries appended but not yet handed to apply(),
    // in order. Drained by acquire_replay_position_handles_for(), which mints
    // each command's memtable reference from its batch's reference; a batch with no
    // pending command left is dropped. Eight bytes per in-flight command
    // rather than a handle each, and nothing at all for the other kinds.
    std::deque<raft::index_t> _pending_commands;
    // The group's parked references (see pending_cover_map). Updated in place
    // by write_batches() and seeded by the constructor (the map built while
    // the commitlog replay rewrote the group's batches); consumed by
    // take_committed_covers() once the whole index range is committed;
    // covers invalidated by truncate_log() move their references to
    // _orphaned_holders.
    pending_cover_map _pending_covers;
    // References of covers truncate_log() removed from _pending_covers: their
    // max_idx referred to truncated entries, and leaving them parked would
    // wedge take_committed_covers()'s prefix pop, but their segments may
    // still hold this group's earlier entries and must stay pinned. Each
    // reference carries the release floor a covering value must reach before the
    // reference may ride it: the truncation point minus one, an upper bound on the
    // live entries left in the orphaned segment (everything at or above the
    // truncation point was discarded). It is unrelated to the commit index
    // recovered at startup, which this code also calls a floor.
    // take_committed_covers() attaches a reference to the last cover of the first
    // pop that reaches its release floor; a pop of lower, pre-truncation
    // covers leaves it parked.
    struct orphaned_holder {
        raft::index_t release_floor;
        db::rp_handle segment_holder;
    };
    std::vector<orphaned_holder> _orphaned_holders;
    // Configurations of the configuration entries appended but not yet known
    // to be committed, in index order. note_commit_idx() promotes the ones
    // the commit index has reached into _committed_config, and truncate_log()
    // drops the discarded tail. A configuration is a handful of members, and
    // only the in-flight window is held.
    //
    // Fed from both sources of an uncommitted entry: store_log_entries() for
    // entries appended in this run, and the constructor for entries the
    // commitlog replay rewrote. Missing the second one loses a configuration
    // that was uncommitted at a crash and commits after the restart, since
    // the replay-side recovery only handles committed ones (SCYLLADB-3842).
    std::deque<std::pair<raft::index_t, raft::configuration>> _appended_configs;
    // The newest committed configuration not yet handed to a covering
    // mutation, with the index of the entry that carried it. Consumed by
    // take_committed_config().
    std::optional<std::pair<raft::index_t, raft::configuration>> _committed_config;
    // (index, term) of the entries appended but not yet known to be
    // committed, in index order. note_commit_idx() consumes the prefix the
    // commit index has reached, keeping the last consumed pair in
    // _last_committed; truncate_log() drops the discarded tail. Small: it
    // only ever holds the in-flight window.
    std::deque<raft_term_and_index> _appended_terms;
    // (index, term) of the last entry known to be committed, recorded by
    // every batch's trailing commit_idx entry. Zero until the first commit
    // index this instance observes; until then no record is written, which
    // costs nothing — a replay-restored floor or an earlier run's covering
    // mutations already cover everything committed before this instance.
    raft_term_and_index _last_committed{};
    // The log entries that were loaded from database commit log on startup.
    raft::log_entries _replayed_entries;

public:
    raft_commitlog(raft::group_id group_id, db::commitlog& commit_log, table_id target_table_id,
            db::cf_id_type raft_groups_table_id, replayed_data_per_group replayed_data);

    ~raft_commitlog();

    // Write the given entries to the commit log as ONE commitlog entry: the
    // group id once, the entries, and `committed` — the index and term of the
    // last entry the group had committed — as the batch header.
    //
    // That pair is the crash-replay floor: startup restores the highest
    // surviving value per group into system.raft_groups, so replay applies
    // the entries below it to memtables instead of re-adding them to the raft
    // log. The term travels with the index because the entry at that index is
    // normally in an earlier batch, whose segment may already be gone, so the
    // restore cannot look it up — and the boot-time snapshot bump needs it
    // exact (see make_raft_schema's commit_idx_term comment). A zero index
    // means this instance has not observed a commit index yet.
    //
    // `covers` is the caller's cover map, updated in place: a segment new to
    // the map gets a reference of its own (commitlog::acquire_cf_count) — one per
    // segment per group, not one per batch — while a segment already there
    // keeps its reference and only its (max index, term) pair advances.
    //
    // Returns the batch's own reference, accounted to the target table and
    // carrying the batch's position. The caller keeps it until every command
    // in the batch has been applied (see batch_ref).
    //
    // Static so the commitlog replay's rewrite path can write the same
    // format, building the map that seeds the group's _pending_covers.
    static future<db::rp_handle> write_batches(db::commitlog& cl, db::cf_id_type table_id,
            db::cf_id_type raft_groups_table_id, raft::group_id group_id,
            const raft::log_entry_ptr_list& entries, raft_term_and_index committed, pending_cover_map& covers);

    // Persist the given log entries in the commit log via write_batches()
    // (which maintains _pending_covers), keeping the batch's reference and the
    // indexes of its command entries. The batch header carries the pair
    // note_commit_idx() last saw.
    future<> store_log_entries(const raft::log_entry_ptr_list& entries);

    // Record that the group has committed up to `idx`. Keeps the (index,
    // term) pair of the last committed entry, which the next batch's header
    // persists as the crash-replay floor. The
    // term is taken from the entry itself: raft calls this before the entry
    // is applied, so it is still tracked here.
    void note_commit_idx(raft::index_t idx);

    // Get the log items that were loaded from database commit log on startup.
    raft::log_entries load_log();

    // Remove all the items with index >= idx, as they are considered truncated in Raft semantics.
    void truncate_log(raft::index_t idx);

    // Remove replay position handles for entries that have been snapshotted
    // and are no longer needed in the raft log. This allows the commitlog
    // segments holding those entries to be reclaimed.
    // Called from store_snapshot_descriptor after the snapshot is persisted.
    void truncate_log_tail(raft::index_t index);

    // Pop and return covers for the segments whose entries are all committed
    // at `commit_idx` (write_batches() keeps one cover per segment, so no
    // coalescing happens here). Commitment — not application — is the gate:
    // the covering value only claims that entries at or below it are
    // committed, and a committed entry stays recoverable whether or not it
    // has been applied, because its batch's reference is held until apply()
    // mints from it and the target table's memtable then holds that reference
    // until the data is durable. Contract for the caller (raft_groups_storage): for each
    // cover, apply a system.raft_groups mutation carrying a commit index >=
    // max_idx to the raft_groups memtable together with the pin. The
    // mutation and its pin share a memtable generation, so the pin can only
    // be released by the flush that makes the covering value durable.
    std::vector<segment_cover> take_committed_covers(raft::index_t commit_idx);

    // Hand out the newest committed configuration whose entry is at or below
    // `idx`, if one is pending. The caller must persist it in the same
    // system.raft_groups mutation as the covering value for that entry's
    // segment: the configuration then becomes durable exactly when the
    // segment's reference is released, so the entry can only be reclaimed once
    // its configuration is safe. Returns nothing when no configuration was
    // committed in that range, or when it has already been handed out.
    std::optional<std::pair<raft::index_t, raft::configuration>> take_committed_config(raft::index_t idx);

    // Mint a memtable reference for each of the specified command entries from
    // the reference of the batch it was written in, and forget the command. Each
    // returned handle carries the batch's position, which is the position the
    // mutation was actually written at, so the memtable's recorded replay
    // position stays truthful. A batch with no pending command left is
    // dropped, since the memtables now hold references of their own.
    //
    // This opens no window where the target memtable is the segment's sole
    // owner: the batch's reference exists continuously from the write until the
    // last of its commands is handed over here. Triggers on_internal_error if
    // a requested entry has no pending command or no batch to mint from.
    std::vector<index_and_replay_position> acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries);
};
} // namespace service::strong_consistency
