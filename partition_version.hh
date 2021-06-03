/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "mutation_partition.hh"
#include "mutation_fragment.hh"
#include "utils/anchorless_list.hh"
#include "utils/logalloc.hh"
#include "utils/coroutine.hh"
#include "utils/chunked_vector.hh"

#include <boost/intrusive/parent_from_member.hpp>
#include <boost/intrusive/slist.hpp>

// This is MVCC implementation for mutation_partitions.
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
// Scene IV. Schema upgrade
//   pv    pv --- pv
//   ^     ^      ^
//   |     |      |
//   pe    ps(u)  ps
// When there is a schema upgrade the list of partition versions pointed to
// by partition_entry is replaced by a new single partition_version that is a
// result of squashing and upgrading the old versions.
// Old versions not used by any partition snapshot are removed. The first
// partition snapshot on the list is marked as unique which means that upon
// its destruction it won't attempt to squash versions but instead remove
// the unused ones and pass the "unique owner" mark the next snapshot on the
// list (if there is any).
//
// Scene V. partition_entry eviction
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
// Continuity merging rules.
//
// Non-evictable snapshots contain fully continuous partitions in all versions at all times.
// For evictable snapshots, that's not the case.
//
// Each version has its own continuity, fully specified in that version,
// independent of continuity of other versions. Continuity of the snapshot is a
// union of continuities of each version. This rule follows from the fact that we
// want eviction from older versions to not have to touch newer versions.
//
// It is assumed that continuous intervals in different versions are non-
// overlapping,  with exceptions for points corresponding to complete rows.
// A row may overlap  with another row, in which case it completely overrides
// it. A later version may have a row which falls into a continuous interval
// in the older version. A newer version cannot have a continuous interval
// which is not a row and covers a row in the older version. We make use of
// this assumption to make calculation of the union of intervals on merging
// easier.
//
// versions of evictable entries always have a dummy entry at position_in_partition::after_all_clustered_rows().
// This is needed so that they can be always made fully discontinuous by eviction, and because
// we need a way to link partitions with no rows into the LRU.
//
// Snapshots of evictable entries always have a row entry at
// position_in_partition::after_all_clustered_rows().
//

class partition_version_ref;

class partition_version : public anchorless_list_base_hook<partition_version> {
    partition_version_ref* _backref = nullptr;
    mutation_partition _partition;

    friend class partition_version_ref;
    friend class partition_entry;
    friend class partition_snapshot;
public:
    static partition_version& container_of(mutation_partition& mp) {
        return *boost::intrusive::get_parent_from_member(&mp, &partition_version::_partition);
    }

    using is_evictable = bool_class<class evictable_tag>;

    explicit partition_version(schema_ptr s) noexcept
        : _partition(std::move(s)) { }
    explicit partition_version(mutation_partition mp) noexcept
        : _partition(std::move(mp)) { }
    partition_version(partition_version&& pv) noexcept;
    partition_version& operator=(partition_version&& pv) noexcept;
    ~partition_version();
    // Frees elements of this version in batches.
    // Returns stop_iteration::yes iff there are no more elements to free.
    stop_iteration clear_gently(cache_tracker* tracker) noexcept;

    mutation_partition& partition() { return _partition; }
    const mutation_partition& partition() const { return _partition; }

    bool is_referenced() const { return _backref; }
    // Returns true iff this version is directly referenced from a partition_entry (is its newset version).
    bool is_referenced_from_entry() const;
    partition_version_ref& back_reference() { return *_backref; }

    size_t size_in_allocator(const schema& s, allocation_strategy& allocator) const;
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
        assert(!_version->_backref);
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
        assert(_version);
        return *_version;
    }
    const partition_version& operator*() const {
        assert(_version);
        return *_version;
    }
    partition_version* operator->() {
        assert(_version);
        return _version;
    }
    const partition_version* operator->() const {
        assert(_version);
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
        bool operator!=(const change_mark& m) const {
            return !(*this == m);
        }
        explicit operator bool() const {
            return _reclaim_count > 0;
        }
    };
private:
    schema_ptr _schema;
    // Either _version or _entry is non-null.
    partition_version_ref _version;
    partition_entry* _entry;
    phase_type _phase;
    logalloc::region* _region;
    mutation_cleaner* _cleaner;
    cache_tracker* _tracker;
    boost::intrusive::slist_member_hook<> _cleaner_hook;
    bool _locked = false;
    friend class partition_entry;
    friend class mutation_cleaner_impl;
public:
    explicit partition_snapshot(schema_ptr s,
                                logalloc::region& region,
                                mutation_cleaner& cleaner,
                                partition_entry* entry,
                                cache_tracker* tracker, // non-null for evictable snapshots
                                phase_type phase = default_phase)
        : _schema(std::move(s)), _entry(entry), _phase(phase), _region(&region), _cleaner(&cleaner), _tracker(tracker) { }
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

    const schema_ptr& schema() const { return _schema; }
    logalloc::region& region() const { return *_region; }
    cache_tracker* tracker() const { return _tracker; }
    mutation_cleaner& cleaner() { return *_cleaner; }

    tombstone partition_tombstone() const;
    ::static_row static_row(bool digest_requested) const;
    bool static_row_continuous() const;
    mutation_partition squashed() const;

    using range_tombstone_result = utils::chunked_vector<range_tombstone>;

    // Returns range tombstones overlapping with [start, end)
    range_tombstone_result range_tombstones(position_in_partition_view start, position_in_partition_view end);
    // Returns all range tombstones
    range_tombstone_result range_tombstones();
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
    explicit partition_entry(mutation_partition mp);
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
    void apply(const schema& s, const mutation_partition& mp, const schema& mp_schema, mutation_application_stats& app_stats);
    void apply(const schema& s, mutation_partition&& mp, const schema& mp_schema, mutation_application_stats& app_stats);

    // Adds mutation_partition represented by "other" to the one represented
    // by this entry.
    // This entry must be evictable.
    //
    // The argument must be fully-continuous.
    //
    // The continuity of this entry remains unchanged. Information from "other"
    // which is incomplete in this instance is dropped. In other words, this
    // performs set intersection on continuity information, drops information
    // which falls outside of the continuity range, and applies regular merging
    // rules for the rest.
    //
    // Weak exception guarantees.
    // If an exception is thrown this and pe will be left in some valid states
    // such that if the operation is retried (possibly many times) and eventually
    // succeeds the result will be as if the first attempt didn't fail.
    //
    // The schema of pe must conform to s.
    //
    // Returns a coroutine object representing the operation.
    // The coroutine must be resumed with the region being unlocked.
    //
    // The coroutine cannot run concurrently with other apply() calls.
    coroutine apply_to_incomplete(const schema& s,
        partition_entry&& pe,
        mutation_cleaner& pe_cleaner,
        logalloc::allocating_section&,
        logalloc::region&,
        cache_tracker& this_tracker,
        partition_snapshot::phase_type,
        real_dirty_memory_accounter&);

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
                assert(second && second->is_referenced());
                auto&& snp = partition_snapshot::referer_of(*second);
                assert(phase == snp._phase);
                return *second;
            } else { // phase > _snapshot->_phase
                add_version(s, t);
            }
        }
        return *_version;
    }

    mutation_partition squashed(schema_ptr from, schema_ptr to);
    mutation_partition squashed(const schema&);
    tombstone partition_tombstone() const;

    // needs to be called with reclaiming disabled
    // Must not be called when is_locked().
    void upgrade(schema_ptr from, schema_ptr to, mutation_cleaner&, cache_tracker*);

    // Snapshots with different values of phase will point to different partition_version objects.
    // When is_locked(), read() can only be called with a phase which is <= the phase of the current snapshot.
    partition_snapshot_ptr read(logalloc::region& region,
        mutation_cleaner&,
        schema_ptr entry_schema,
        cache_tracker*,
        partition_snapshot::phase_type phase = partition_snapshot::default_phase);

    class printer {
        const schema& _schema;
        const partition_entry& _partition_entry;
    public:
        printer(const schema& s, const partition_entry& pe) : _schema(s), _partition_entry(pe) { }
        printer(const printer&) = delete;
        printer(printer&&) = delete;

        friend std::ostream& operator<<(std::ostream& os, const printer& p);
    };
    friend std::ostream& operator<<(std::ostream& os, const printer& p);
};

// Monotonic exception guarantees
void merge_versions(const schema&, mutation_partition& newer, mutation_partition&& older);

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
