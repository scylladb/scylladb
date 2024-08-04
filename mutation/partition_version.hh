/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation_partition.hh"
#include "mutation_partition_v2.hh"
#include "utils/assert.hh"
#include "utils/anchorless_list.hh"
#include "utils/logalloc.hh"
#include "utils/coroutine.hh"
#include "utils/chunked_vector.hh"

#include <boost/intrusive/parent_from_member.hpp>
#include <boost/intrusive/slist.hpp>

class static_row;

// This is MVCC implementation for mutation_partitions.
//
// See docs/dev/mvcc.md for important design information.
//
// It is assumed that mutation_partitions are stored in some sort of LSA-managed
// container (memtable or row cache).
//
// partition_entry - the main handle to the mutation_partition, allows writes
//                   and reads.
// partition_version - mutation_partition inside a list of partition versions.
//                     mutation_partition represents just a difference against
//                     the next one in the list. To get a single
//                     mutation_partition fully representing this version one
//                     needs to merge this one and all its successors in the
//                     list.
// partition_snapshot - a handle to some particular partition_version. It allows
//                      only reads and itself is immutable the partition version
//                      it represents won't be modified as long as the snapshot
//                      is alive.
//
// pe - partition_entry
// pv - partition_version
// ps - partition_snapshot
// ps(u) - partition_snapshot marked as unique owner

// Scene I. Write-only loads
//   pv
//   ^
//   |
//   pe
// In case of write-only loads all incoming mutations are directly applied
// to the partition_version that partition_entry is pointing to. The list
// of partition_versions contains only a single element.
//
// Scene II. Read-only loads
//   pv
//   ^
//   |
//   pe <- ps
// In case of read-only scenarios there is only a single partition_snapshot
// object that points to the partition_entry. There is only a single
// partition_version.
//
// Scene III. Writes and reads
//   pv -- pv -- pv
//   ^     ^     ^
//   |     |     |
//   pe    ps    ps
// If the partition_entry that needs to be modified is currently read from (i.e.
// there exist a partition_snapshot pointing to it) instead of applying new
// mutation directly a new partition version is created and added at the front
// of the list. partition_entry points to the new version (so that it has the
// most recent view of stored data) while the partition_snapshot points to the
// same partition_version it pointed to before (so that the data it sees doesn't
// change).
// As a result the list may contain multiple partition versions used by
// different partition snapshots.
// When the partition_snapshot is destroyed partition_versions are squashed
// together to minimize the amount of elements on the list.
//
// Scene IV. partition_entry eviction
//   pv
//   ^
//   |
//   ps(u)
// When partition_entry is removed (e.g. because it was evicted from cache)
// the partition versions are removed in a similar manner than in the schema
// upgrade scenario. The unused ones are destroyed right away and the first
// snapshot on the list is marked as unique owner so that on its destruction
// it continues removal of the partition versions.
//

// Schema upgrades
//
// After a schema change (e.g. a column is removed), the layout of existing
// rows in memory becomes outdated and has to be adjusted before they are
// emitted by a query expecting the newer schema.
//
// Rows can be upgraded on the fly during queries. But upgrades have a high CPU
// cost, so we want them to happen only once. The upgraded row should be saved
// in memory so that future queries don't have to upgrade it again.
// And it should replace the old row as soon as possible (when there
// the row is no longer reachable through the old schema) to conserve memory.
//
// This behavior is akin to MVCC. A schema upgrade can be thought of as a
// special kind of update which affects all rows, and the MVCC machinery can be
// naturally hijacked to implement it.
//
// Currently, we do it as follows:
//
// - Each MVCC version has its own schema pointer. Versions in the same chain
//   can be of different schemas.
//
// - The schema of a partition entry is defined as the schema of the newest version.
//   A partition entry upgrade is performed simply by inserting a new empty version with
//   the new schema. (And triggering a background version merge by creating and immediately
//   destroying a snapshot pointing at the previous newest version).
//   Due to this, schemas of versions in the chain are ordered chronologically.
//   (The order is important because it's forbidden to upgrade to an older version,
//   because that's lossy -- e.g. a new column can be lost).
//
// - On read, the cursor upgrades rows on the fly to the cursor's schema.
//   If the cursor reads the latest version, the upgraded rows are written to the latest
//   version.
//
// - When versions are merged, rows are upgraded to the newer schema, the result of the
//   merge has the newer schema.
//
//   This one is tricky. A natural idea is to merge older versions into the newer version,
//   (upgrading rows when moving/copying them between versions), so that after a merge
//   only the new version is left. But usually we want to merge in the other direction.
//
//   (When an database write arrives, we want to merge it into the existing
//   older version, so that it has a cost proportional to the size of the
//   write, not to the size of the existing version, which can be arbitrarily
//   large. Doing otherwise would invite quadratic behaviour)
//
//   The merging algorithm is already very complicated and making it work in both
//   directions (or adding a separate algorithm specifically for upgrades) would
//   complicate things even further.
//
//   So instead, when two versions of different schema are merged, the older version
//   (which also has the older schema) is first upgraded to the newer schema in a special
//   upgrade process which only uses regular newer-into-older merging.
//   This is done by appending a fresh empty version with the newer schema after
//   the version-to-be-upgraded, and merging the version-to-be-upgraded into the new one.
//   In the end, only the new version with the newer schema is left.
//
//   Technically the above procedure temporarily violates the rule that schema versions
//   in the chain are ordered chronologically (which is needed for correctness).
//   So while the above is happening, the version-to-be-upgraded has _is_being_upgraded set.
//   A version with _is_being_upgraded is understood to be special in that its
//   schema is older than its next neighbour's, and care is taken so that the
//   neighbour isn't recursively downgraded back to the older schema.
//   A version with _is_being_upgraded can be viewed together with its next() as
//   conceptually a single version with the schema of next().
//
// The typical upgrade sequence, illustrated:
//   1. Initial state:
//        pv1 (s1)
//        ^
//        |
//        pe
//   2. partition_entry::upgrade(s2) is called. Empty pv2 is added.
//        pv2 (s2) -- pv1 (s1)
//        ^           ^
//        |           |
//        pe          ps1 (created and instantly dropped, so that merging is initiated)
//   3. Some time later, mutation_cleaner calls merge_partition_versions(ps1).
//      Merge of pv2 and pv1 is attempted.
//      Schemas differ, so instead an upgrade of pv1 is initiated. Empty pv1' is added.
//      pv1 is now conceptually "owned" by pv1', and no snapshot is allowed to point to it
//      after this point.
//        pv2 (s2) -- pv1 (s1, _is_being_upgraded) -- pv1' (s2)
//        ^                                           ^
//        |                                           |
//        pe                                          ps1
//   4. Eventually pv1 is fully upgrade-merged into pv1' and destroyed.
//        pv2 (s2) -- pv1' (s2)
//        ^           ^
//        |           |
//        pe          ps1
//   5. Upgrade over, further merge proceeds as usual. Eventually pv2 is fully merged into pv1'.
//        pv1' (s2)
//        ^
//        |
//        pe

class partition_version_ref;

class partition_version : public anchorless_list_base_hook<partition_version> {
    partition_version_ref* _backref = nullptr;
    schema_ptr _schema;
    bool _is_being_upgraded = false;
    mutation_partition_v2 _partition;

    friend class partition_version_ref;
    friend class partition_entry;
    friend class partition_snapshot;
public:
    static partition_version& container_of(mutation_partition_v2& mp) {
        return *boost::intrusive::get_parent_from_member(&mp, &partition_version::_partition);
    }

    explicit partition_version(schema_ptr s) noexcept
        : _schema(std::move(s))
        , _partition(*_schema)
    {
        SCYLLA_ASSERT(_schema);
    }
    explicit partition_version(mutation_partition_v2 mp, schema_ptr s) noexcept
        : _schema(std::move(s))
        , _partition(std::move(mp))
    {
        SCYLLA_ASSERT(_schema);
    }

    partition_version(partition_version&& pv) noexcept;
    partition_version& operator=(partition_version&& pv) noexcept;
    ~partition_version();
    // Frees elements of this version in batches.
    // Returns stop_iteration::yes iff there are no more elements to free.
    stop_iteration clear_gently(cache_tracker* tracker) noexcept;

    mutation_partition_v2& partition() { return _partition; }
    const mutation_partition_v2& partition() const { return _partition; }

    bool is_referenced() const { return _backref; }
    // Returns true iff this version is directly referenced from a partition_entry (is its newset version).
    bool is_referenced_from_entry() const;
    partition_version_ref& back_reference() const { return *_backref; }

    size_t size_in_allocator(allocation_strategy& allocator) const;

    const schema_ptr& get_schema() const noexcept { return _schema; }
};

using partition_version_range = anchorless_list_base_hook<partition_version>::range;
using partition_version_reversed_range = anchorless_list_base_hook<partition_version>::reversed_range;

class partition_version_ref {
    partition_version* _version = nullptr;
    bool _unique_owner = false;

    friend class partition_version;
public:
    partition_version_ref() = default;
    explicit partition_version_ref(partition_version& pv, bool unique_owner = false) noexcept
        : _version(&pv)
        , _unique_owner(unique_owner)
    {
        SCYLLA_ASSERT(!_version->_backref);
        _version->_backref = this;
    }
    ~partition_version_ref() {
        if (_version) {
            _version->_backref = nullptr;
        }
    }
    partition_version_ref(partition_version_ref&& other) noexcept
        : _version(other._version)
        , _unique_owner(other._unique_owner)
    {
        if (_version) {
            _version->_backref = this;
        }
        other._version = nullptr;
    }
    partition_version_ref& operator=(partition_version_ref&& other) noexcept {
        if (this != &other) {
            this->~partition_version_ref();
            new (this) partition_version_ref(std::move(other));
        }
        return *this;
    }

    explicit operator bool() const { return _version; }

    partition_version& operator*() {
        SCYLLA_ASSERT(_version);
        return *_version;
    }
    const partition_version& operator*() const {
        SCYLLA_ASSERT(_version);
        return *_version;
    }
    partition_version* operator->() {
        SCYLLA_ASSERT(_version);
        return _version;
    }
    const partition_version* operator->() const {
        SCYLLA_ASSERT(_version);
        return _version;
    }

    bool is_unique_owner() const { return _unique_owner; }
    void mark_as_unique_owner() { _unique_owner = true; }

    void release() {
        if (_version) {
            _version->_backref = nullptr;
        }
        _version = nullptr;
    }
};

inline
bool partition_version::is_referenced_from_entry() const {
    return !prev() && _backref && !_backref->is_unique_owner();
}

class partition_entry;
class cache_tracker;
class mutation_cleaner;

static constexpr cache_tracker* no_cache_tracker = nullptr;
static constexpr mutation_cleaner* no_cleaner = nullptr;

class partition_snapshot : public enable_lw_shared_from_this<partition_snapshot> {
public:
    // Only snapshots created with the same value of phase can point to the same version.
    using phase_type = uint64_t;
    static constexpr phase_type default_phase = 0; // For use with non-evictable snapshots
    static constexpr phase_type min_phase = 1; // Use 1 to prevent underflow on apply_to_incomplete()
    static constexpr phase_type max_phase = std::numeric_limits<phase_type>::max();

    // Ordinal number of a partition version within a snapshot. Starts with 0.
    using version_number_type = size_t;
public:
    // Used for determining reference stability.
    // References and iterators into versions owned by the snapshot
    // obtained between two equal change_mark objects were produced
    // by that snapshot are guaranteed to be still valid.
    //
    // Has a null state which is != than anything returned by get_change_mark().
    class change_mark {
        uint64_t _reclaim_count = 0;
        size_t _versions_count = 0; // merge_partition_versions() removes versions on merge
    private:
        friend class partition_snapshot;
        change_mark(uint64_t reclaim_count, size_t versions_count)
            : _reclaim_count(reclaim_count), _versions_count(versions_count) {}
    public:
        change_mark() = default;
        bool operator==(const change_mark& m) const {
            return _reclaim_count == m._reclaim_count && _versions_count == m._versions_count;
        }
        explicit operator bool() const {
            return _reclaim_count > 0;
        }
    };
private:
    // Either _version or _entry is non-null.
    partition_version_ref _version;
    partition_entry* _entry;
    phase_type _phase;
    logalloc::region* _region;
    mutation_cleaner* _cleaner;
    cache_tracker* _tracker;
    boost::intrusive::slist_member_hook<> _cleaner_hook;
    std::optional<apply_resume> _version_merging_state;
    bool _locked = false;
    friend class partition_entry;
    friend class mutation_cleaner_impl;
public:
    explicit partition_snapshot(logalloc::region& region,
                                mutation_cleaner& cleaner,
                                partition_entry* entry,
                                cache_tracker* tracker, // non-null for evictable snapshots
                                phase_type phase = default_phase)
        : _entry(entry), _phase(phase), _region(&region), _cleaner(&cleaner), _tracker(tracker) { }
    partition_snapshot(const partition_snapshot&) = delete;
    partition_snapshot(partition_snapshot&&) = delete;
    partition_snapshot& operator=(const partition_snapshot&) = delete;
    partition_snapshot& operator=(partition_snapshot&&) = delete;

    // Makes the snapshot locked.
    // See is_locked() for meaning.
    // Can be called only when at_lastest_version(). The snapshot must remain latest as long as it's locked.
    void lock() noexcept;

    // Makes the snapshot no longer locked.
    // See is_locked() for meaning.
    void unlock() noexcept;

    // Tells whether the snapshot is locked.
    // Locking the snapshot prevents it from getting detached from the partition entry.
    // It also prevents the partition entry from being evicted.
    bool is_locked() const {
        return _locked;
    }

    static partition_snapshot& container_of(partition_version_ref* ref) {
        return *boost::intrusive::get_parent_from_member(ref, &partition_snapshot::_version);
    }

    static const partition_snapshot& container_of(const partition_version_ref* ref) {
        return *boost::intrusive::get_parent_from_member(ref, &partition_snapshot::_version);
    }

    // Returns a reference to the partition_snapshot which is attached to given non-latest partition version.
    // Assumes !v.is_referenced_from_entry() && v.is_referenced().
    static const partition_snapshot& referer_of(const partition_version& v) {
        return container_of(v._backref);
    }

    // If possible, merges the version pointed to by this snapshot with
    // adjacent partition versions. Leaves the snapshot in an unspecified state.
    // Can be retried if previous merge attempt has failed.
    stop_iteration merge_partition_versions(mutation_application_stats& app_stats);

    // Prepares the snapshot for cleaning by moving to the right-most unreferenced version.
    // Returns stop_iteration::yes if there is nothing to merge with and the snapshot
    // should be collected right away, and stop_iteration::no otherwise.
    // When returns stop_iteration::no, the snapshots is guaranteed to not be attached
    // to the latest version.
    stop_iteration slide_to_oldest() noexcept;

    // Brings the snapshot to the front of the LRU.
    void touch() noexcept;

    // Must be called after snapshot's original region is merged into a different region
    // before the original region is destroyed, unless the snapshot is destroyed earlier.
    void migrate(logalloc::region* region, mutation_cleaner* cleaner) noexcept {
        _region = region;
        _cleaner = cleaner;
    }

    ~partition_snapshot();

    partition_version_ref& version();

    change_mark get_change_mark() {
        return {_region->reclaim_counter(), version_count()};
    }

    const partition_version_ref& version() const;

    partition_version_range versions() {
        return version()->elements_from_this();
    }

    unsigned version_count();

    bool at_latest_version() const {
        return _entry != nullptr;
    }

    bool at_oldest_version() const {
        return !version()->next();
    }

    const schema_ptr& schema() const { return version()->get_schema(); }
    logalloc::region& region() const { return *_region; }
    cache_tracker* tracker() const { return _tracker; }
    mutation_cleaner& cleaner() { return *_cleaner; }

    tombstone partition_tombstone() const;
    ::static_row static_row(bool digest_requested) const;
    bool static_row_continuous() const;
    mutation_partition squashed() const;

    using range_tombstone_result = utils::chunked_vector<range_tombstone>;

    phase_type phase() const { return _phase; }
};

class partition_snapshot_ptr {
    lw_shared_ptr<partition_snapshot> _snp;
public:
    using value_type = partition_snapshot;
    partition_snapshot_ptr() = default;
    partition_snapshot_ptr(partition_snapshot_ptr&&) = default;
    partition_snapshot_ptr(const partition_snapshot_ptr&) = default;
    partition_snapshot_ptr(lw_shared_ptr<partition_snapshot> snp) : _snp(std::move(snp)) {}
    ~partition_snapshot_ptr();
    partition_snapshot_ptr& operator=(partition_snapshot_ptr&& other) noexcept {
        if (this != &other) {
            this->~partition_snapshot_ptr();
            new (this) partition_snapshot_ptr(std::move(other));
        }
        return *this;
    }
    partition_snapshot_ptr& operator=(const partition_snapshot_ptr& other) noexcept {
        if (this != &other) {
            this->~partition_snapshot_ptr();
            new (this) partition_snapshot_ptr(other);
        }
        return *this;
    }
    partition_snapshot& operator*() { return *_snp; }
    const partition_snapshot& operator*() const { return *_snp; }
    partition_snapshot* operator->() { return &*_snp; }
    const partition_snapshot* operator->() const { return &*_snp; }
    explicit operator bool() const { return bool(_snp); }
};

class real_dirty_memory_accounter;

// Represents mutation_partition with snapshotting support a la MVCC.
//
// Internally the state is represented by an ordered list of mutation_partition
// objects called versions. The logical mutation_partition state represented
// by that chain is equal to reducing the chain using mutation_partition::apply()
// from left (latest version) to right.
//
// We distinguish evictable and non-evictable partition entries. Entries which
// are non-evictable have all their elements non-evictable and fully continuous.
// Partition snapshots inherit evictability of the entry, which remains invariant
// for a snapshot.
//
// After evictable partition_entry is linked into a cache_tracker, that cache_tracker
// must always be passed to methods which accept a pointer to a cache_tracker.
// Also, evict() must be called before the entry is unlinked from a cache_tracker.
// For non-evictable entries, no_cache_tracker should be passed to methods which accept a cache_tracker.
//
// As long as an entry is linked to a cache_tracker, it must belong to a cache_entry.
// partition_version objects may be linked with a cache_tracker and detached from a cache_entry
// if owned by a snapshot.
//
class partition_entry {
    partition_snapshot* _snapshot = nullptr;
    partition_version_ref _version;

    friend class partition_snapshot;
    friend class cache_entry;
private:
    void set_version(partition_version*);
public:
    struct evictable_tag {};
    // Constructs a non-evictable entry holding empty partition
    partition_entry() = default;
    // Constructs a non-evictable entry
    partition_entry(const schema&, mutation_partition_v2);
    partition_entry(const schema&, mutation_partition);
    // Returns a reference to partition_entry containing given pv,
    // assuming pv.is_referenced_from_entry().
    static partition_entry& container_of(partition_version& pv) {
        return *boost::intrusive::get_parent_from_member(&pv.back_reference(), &partition_entry::_version);
    }
    // Constructs an evictable entry
    // Strong exception guarantees for the state of mp.
    partition_entry(evictable_tag, const schema& s, mutation_partition&& mp);
    ~partition_entry();
    // Frees elements of this entry in batches.
    // Active snapshots are detached, data referenced by them is not cleared.
    // Returns stop_iteration::yes iff there are no more elements to free.
    stop_iteration clear_gently(cache_tracker*) noexcept;
    static partition_entry make_evictable(const schema& s, mutation_partition&& mp);
    static partition_entry make_evictable(const schema& s, const mutation_partition& mp);

    partition_entry(partition_entry&& pe) noexcept
        : _snapshot(pe._snapshot), _version(std::move(pe._version))
    {
        if (_snapshot) {
            _snapshot->_entry = this;
        }
        pe._snapshot = nullptr;
    }
    partition_entry& operator=(partition_entry&& other) noexcept {
        if (this != &other) {
            this->~partition_entry();
            new (this) partition_entry(std::move(other));
        }
        return *this;
    }

    // Removes data contained by this entry, but not owned by snapshots.
    // Snapshots will be unlinked and evicted independently by reclaimer.
    // This entry is invalid after this and can only be destroyed.
    void evict(mutation_cleaner&) noexcept;

    partition_version_ref& version() {
        return _version;
    }

    partition_version_range versions() {
        return _version->elements_from_this();
    }

    partition_version_reversed_range versions_from_oldest() {
        return _version->all_elements_reversed();
    }

    // Tells whether this entry is locked.
    // Locked entries are undergoing an update and should not have their snapshots
    // detached from the entry.
    // Certain methods can only be called when !is_locked().
    bool is_locked() const {
        return _snapshot && _snapshot->is_locked();
    }

    // Strong exception guarantees.
    // Assumes this instance and mp are fully continuous.
    // Use only on non-evictable entries.
    // Must not be called when is_locked().
    void apply(logalloc::region&,
               mutation_cleaner&,
               const schema& s,
               const mutation_partition_v2& mp,
               const schema& mp_schema,
               mutation_application_stats& app_stats);

    void apply(logalloc::region&,
               mutation_cleaner&,
               const schema& s,
               mutation_partition_v2&& mp,
               const schema& mp_schema,
               mutation_application_stats& app_stats);

    void apply(logalloc::region&,
               mutation_cleaner&,
               const schema& s,
               const mutation_partition& mp,
               const schema& mp_schema,
               mutation_application_stats& app_stats);

    // Adds mutation_partition represented by "pe" to the one represented
    // by this entry.
    // This entry must be evictable.
    // "pe" must be fully-continuous.
    // (Alternatively: applies the "pe" memtable entry to "this" cache entry.)
    //
    // The continuity of this entry remains unchanged. Information from "pe"
    // which is incomplete in this instance is dropped. In other words, this
    // performs set intersection on continuity information, drops information
    // which falls outside of the continuity range, and applies regular merging
    // rules for the rest.
    // (Rationale: updates from the memtable are only applied to intervals
    // which were already in cache. The cache treats the entire sstable set as a
    // single source -- it isn't able to store partial information only from a
    // single sstable.)
    //
    // Weak exception guarantees.
    // If an exception is thrown, "this" and "pe" will be left in some valid states
    // such that if the operation is retried (possibly many times) and eventually
    // succeeds the result will be as if the first attempt didn't fail.
    //
    // The schema of "pe" must conform to "s".
    //
    // Returns a coroutine object representing the operation.
    // The coroutine must be resumed with the region being unlocked.
    //
    // The coroutine cannot run concurrently with other apply() calls.
    utils::coroutine apply_to_incomplete(const schema& s,
        partition_entry&& pe,
        mutation_cleaner& pe_cleaner,
        logalloc::allocating_section&,
        logalloc::region&,
        cache_tracker& this_tracker,
        partition_snapshot::phase_type,
        real_dirty_memory_accounter&,
        preemption_source&);

    // If this entry is evictable, cache_tracker must be provided.
    // Must not be called when is_locked().
    partition_version& add_version(const schema& s, cache_tracker*);

    // Returns a reference to existing version with an active snapshot of given phase
    // or creates a new version and returns a reference to it.
    // Doesn't affect value or continuity of the partition.
    partition_version& open_version(const schema& s, cache_tracker* t, partition_snapshot::phase_type phase = partition_snapshot::max_phase) {
        if (_snapshot) {
            if (_snapshot->_phase == phase) {
                return *_version;
            } else if (phase < _snapshot->_phase) {
                // If entry is being updated, we will get reads for non-latest phase, and
                // they must attach to the non-current version.
                partition_version* second = _version->next();
                SCYLLA_ASSERT(second && second->is_referenced());
                auto&& snp = partition_snapshot::referer_of(*second);
                SCYLLA_ASSERT(phase == snp._phase);
                return *second;
            } else { // phase > _snapshot->_phase
                add_version(s, t);
            }
        }
        return *_version;
    }

    mutation_partition_v2 squashed_v2(const schema& to, is_evictable);
    mutation_partition squashed(const schema&, is_evictable);
    tombstone partition_tombstone() const;

    // needs to be called with reclaiming disabled
    // Must not be called when is_locked().
    void upgrade(logalloc::region& r, schema_ptr to, mutation_cleaner&, cache_tracker*);

    const schema_ptr& get_schema() const noexcept { return _version->get_schema(); }

    // Snapshots with different values of phase will point to different partition_version objects.
    // When is_locked(), read() can only be called with a phase which is <= the phase of the current snapshot.
    partition_snapshot_ptr read(logalloc::region& region,
        mutation_cleaner&,
        cache_tracker*,
        partition_snapshot::phase_type phase = partition_snapshot::default_phase);

    class printer {
        const partition_entry& _partition_entry;
    public:
        printer(const partition_entry& pe) : _partition_entry(pe) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend fmt::formatter<printer>;
    };
    friend fmt::formatter<printer>;
};

// Monotonic exception guarantees
void merge_versions(const schema&, mutation_partition& newer, mutation_partition&& older, is_evictable);

inline partition_version_ref& partition_snapshot::version()
{
    if (_version) {
        return _version;
    } else {
        return _entry->_version;
    }
}

inline const partition_version_ref& partition_snapshot::version() const
{
    if (_version) {
        return _version;
    } else {
        return _entry->_version;
    }
}

template <> struct fmt::formatter<partition_entry::printer> : fmt::formatter<string_view> {
    auto format(const partition_entry::printer&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
