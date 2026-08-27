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
struct index_and_replay_position {
    raft::index_t index;
    db::rp_handle replay_position_handle;
};

// Raft indexes only increase, so entries are naturally sorted by index.
// A deque allows efficient access from both ends: front removal for
// truncate_log_tail() and back removal for truncate_log().
using replay_position_list = std::deque<index_and_replay_position>;

// A segment's raft_groups-accounted claim together with the highest raft
// index the group has written to that segment so far (the pending_cover_map
// value; the segment is the key), parked until every entry at or below that
// index is committed. The claim is taken (commitlog::acquire_cf_count) when
// the group first writes to the segment and pins it from that moment until
// take_committed_covers() attaches it to a covering raft_groups mutation.
struct pending_cover {
    // Highest raft index the group has written to the claim's segment, and
    // that entry's term. The pair is persisted together by the covering
    // mutation; the term must be exact — see make_raft_schema()'s
    // commit_idx_term comment.
    raft::index_t max_idx;
    raft::term_t max_term;
    db::rp_handle segment_holder;
};

// A group's covers, keyed by commitlog segment: one claim per segment,
// updated in place by write_batches(). Batches arrive in index order and
// segments advance monotonically, so the key order is also the max_idx
// order — the fully-committed covers always form a prefix
// (take_committed_covers()) and the covers a truncation invalidates form a
// suffix (truncate_log()).
using pending_cover_map = std::map<db::segment_id_type, pending_cover>;

struct replayed_data_per_group {
    replay_position_list replay_positions;
    raft::log_entries entries;
    // The cover map write_batches() built while the commitlog replay
    // rewrote the group's batches into the new commitlog; moved wholesale
    // into the group's _pending_covers on construction.
    pending_cover_map covers;
};

// A commitlog segment all of whose entries (for this group) are committed,
// together with the highest raft index the group appended to it and the claim
// that has held the segment since the group first wrote to it. Produced by
// take_committed_covers(); see its comment for the contract.
struct segment_cover {
    db::segment_id_type segment;
    raft::index_t max_idx;
    raft::term_t max_term;
    db::rp_handle segment_holder;
    // Claims of covers removed by truncate_log() (see _orphaned_holders),
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
// maintained with plain dirty-count claims and no flush hooks:
//  - the first time the group writes to a segment, a raft_groups-accounted
//    claim for it is taken (commitlog::acquire_cf_count) and
//    parked here (_pending_covers), pinning the segment from that moment;
//  - applied command entries pin their segment through the target table's
//    memtable (their handles move there in apply()), released by that
//    memtable's flush once the data is durable;
//  - once every entry in a segment is committed, a covering
//    system.raft_groups mutation carrying the segment's max index enters the
//    raft_groups memtable together with the parked claim
//    (take_committed_covers(), driven by store_commit_idx()), released by
//    that memtable's flush once the value is durable.
// The claim exists continuously from write to the raft_groups flush that
// persists its covering value, so there is no window in which the target
// table's memtable is a segment's sole owner.
class raft_commitlog {
private:
    const raft::group_id _group_id;
    const db::cf_id_type _table_id;
    // cf the segments' claims are minted under (system.raft_groups): the
    // covering mutation rides that table's memtable, so its flush both
    // persists the value and releases the claims it carried.
    const db::cf_id_type _raft_groups_table_id;
    // Common commit log.
    db::commitlog& _commit_log;
    // Replay position handles for command entries (raft::command), in index
    // order. Moved out by acquire_replay_position_handles_for() when
    // state_machine::apply() hands the entry to its target memtable, which
    // then holds the claim until the data is flushed. Until then this deque
    // is the pin that lets covering happen at commit time: a covering value
    // may become durable before the entry is applied, and the segment must
    // survive for replay to re-apply it.
    replay_position_list _command_positions;
    // Replay position handles for dummy entries (raft::log_entry::dummy), in
    // index order. Never handed to apply(); released once their segment is
    // covered (release_covered_dummies()) — a dummy carries no state, so a
    // durable commit index at or above it is all a restart needs.
    replay_position_list _dummy_positions;
    // Replay position handles for configuration entries (raft::configuration).
    // Never consumed by apply(). Unlike commands and dummies these carry state
    // that is only persisted by store_snapshot_descriptor(), and commitlog
    // replay discards committed non-command entries — so their handles must
    // stay held until a snapshot has written the configuration durably.
    // Released only by truncate_log() / truncate_log_tail(). See SCYLLADB-3842.
    replay_position_list _config_positions;
    // The group's parked claims (see pending_cover_map). Updated in place
    // by write_batches() and seeded by the constructor (the map built while
    // the commitlog replay rewrote the group's batches); consumed by
    // take_committed_covers() once the whole index range is committed;
    // covers invalidated by truncate_log() move their claims to
    // _orphaned_holders.
    pending_cover_map _pending_covers;
    // Claims of covers truncate_log() removed from _pending_covers: their
    // max_idx referred to truncated entries, and leaving them parked would
    // wedge take_committed_covers()'s prefix pop, but their segments may
    // still hold this group's earlier entries and must stay pinned. Each
    // claim carries the release floor a covering value must reach before the
    // claim may ride it: the truncation point minus one, an upper bound on the
    // live entries left in the orphaned segment (everything at or above the
    // truncation point was discarded). It is unrelated to the commit index
    // recovered at startup, which this code also calls a floor.
    // take_committed_covers() attaches a claim to the last cover of the first
    // pop that reaches its release floor; a pop of lower, pre-truncation
    // covers leaves it parked.
    struct orphaned_holder {
        raft::index_t release_floor;
        db::rp_handle segment_holder;
    };
    std::vector<orphaned_holder> _orphaned_holders;
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

    // Write the given entries to the commit log as one batch, followed by a
    // small commit_idx record carrying `committed` — the index and term of
    // the last entry the group had committed — and make sure every segment
    // the entries landed in holds a raft_groups-accounted claim.
    //
    // That record is the crash-replay floor: startup restores the highest
    // surviving value per group into system.raft_groups, so replay applies
    // the entries below it to memtables instead of re-adding them to the raft
    // log. It carries the term with the index so the restore needs no lookup
    // — the pair goes straight into system.raft_groups, and the boot-time
    // snapshot bump needs it exact (see make_raft_schema's commit_idx_term
    // comment). Nothing keeps the record's own handle: it is written in the
    // batch's cf like the entries and dropped right after, because segment
    // retention is the claims' job, and losing a record to a reclaimed
    // segment only lowers the recovered floor. A zero index means this
    // instance has not observed a commit index yet, and no record is written.
    //
    // `covers` is the caller's cover map, updated in place: a segment new to
    // the map gets a claim of its own (commitlog::acquire_cf_count) — one per
    // segment per group, not one per batch — while a segment already there
    // keeps its claim and only its (max index, term) pair advances. Returns
    // one rp_handle per input entry, in input order, accounted to the target
    // table.
    //
    // Static so the commitlog replay's rewrite path can write the same
    // format, building the map that seeds the group's _pending_covers.
    static future<utils::chunked_vector<db::rp_handle>> write_batches(db::commitlog& cl, db::cf_id_type table_id,
            db::cf_id_type raft_groups_table_id, raft::group_id group_id,
            const raft::log_entry_ptr_list& entries, raft_term_and_index committed, pending_cover_map& covers);

    // Persist the given log entries in the commit log via write_batches()
    // (which maintains _pending_covers), tracking the entries' handles. The
    // trailing commit_idx record carries the pair note_commit_idx() last saw.
    future<> store_log_entries(const raft::log_entry_ptr_list& entries);

    // Record that the group has committed up to `idx`. Keeps the (index,
    // term) pair of the last committed entry, which the next batch's
    // trailing commit_idx record persists as the crash-replay floor. The
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
    // has been applied, because its handle stays in _command_positions until
    // apply() acquires it and in the target table's memtable until the data
    // is durable. Contract for the caller (raft_groups_storage): for each
    // cover, apply a system.raft_groups mutation carrying a commit index >=
    // max_idx to the raft_groups memtable together with the pin. The
    // mutation and its pin share a memtable generation, so the pin can only
    // be released by the flush that makes the covering value durable.
    std::vector<segment_cover> take_committed_covers(raft::index_t commit_idx);

    // Release dummy handles with index <= idx. Only valid once every segment
    // containing them is covered per the take_committed_covers() contract —
    // from that point their retention no longer depends on these handles:
    // the covering value pins the segment via the raft_groups memtable until
    // the value is durable. (Command handles are consumed by apply();
    // configuration handles are exempt and live until truncation.)
    void release_covered_dummies(raft::index_t idx);

    // Move replay position handles out of _command_positions for the
    // specified entries. The handles are handed to memtables in the raft
    // state machine apply(), and removed since the memtable takes over that
    // claim. This opens no window where the target memtable is the segment's
    // sole owner: the segment's claim, parked at write time and attached to
    // the covering raft_groups mutation at commit, exists continuously until
    // the covering value is durable. Triggers on_internal_error if a
    // requested entry is missing.
    std::vector<index_and_replay_position> acquire_replay_position_handles_for(const raft::log_entry_ptr_list& entries);
};
} // namespace service::strong_consistency
