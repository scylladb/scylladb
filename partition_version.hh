/*
 * Copyright (C) 2016 ScyllaDB
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
#include "streamed_mutation.hh"
#include "utils/anchorless_list.hh"
#include "utils/logalloc.hh"

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

class partition_version_ref;

class partition_version : public anchorless_list_base_hook<partition_version> {
    partition_version_ref* _backref = nullptr;
    mutation_partition _partition;

    friend class partition_version_ref;
public:
    explicit partition_version(schema_ptr s) noexcept
        : _partition(std::move(s)) { }
    explicit partition_version(mutation_partition mp) noexcept
        : _partition(std::move(mp)) { }
    partition_version(partition_version&& pv) noexcept;
    partition_version& operator=(partition_version&& pv) noexcept;
    ~partition_version();

    mutation_partition& partition() { return _partition; }
    const mutation_partition& partition() const { return _partition; }

    bool is_referenced() const { return _backref; }
    partition_version_ref& back_reference() { return *_backref; }

    size_t size_in_allocator(allocation_strategy& allocator) const;
};

using partition_version_range = anchorless_list_base_hook<partition_version>::range;

class partition_version_ref {
    partition_version* _version = nullptr;
    bool _unique_owner = false;

    friend class partition_version;
public:
    partition_version_ref() = default;
    explicit partition_version_ref(partition_version& pv) noexcept : _version(&pv) {
        assert(!_version->_backref);
        _version->_backref = this;
    }
    ~partition_version_ref() {
        if (_version) {
            _version->_backref = nullptr;
        }
    }
    partition_version_ref(partition_version_ref&& other) noexcept : _version(other._version) {
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
};

class partition_entry;

class partition_snapshot : public enable_lw_shared_from_this<partition_snapshot> {
public:
    // Only snapshots created with the same value of phase can point to the same version.
    using phase_type = uint64_t;
    static constexpr phase_type default_phase = 0;
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
    logalloc::region& _region;

    friend class partition_entry;
public:
    explicit partition_snapshot(schema_ptr s,
                                logalloc::region& region,
                                partition_entry* entry,
                                phase_type phase = default_phase)
        : _schema(std::move(s)), _entry(entry), _phase(phase), _region(region) { }
    partition_snapshot(const partition_snapshot&) = delete;
    partition_snapshot(partition_snapshot&&) = delete;
    partition_snapshot& operator=(const partition_snapshot&) = delete;
    partition_snapshot& operator=(partition_snapshot&&) = delete;

    // If possible merges the version pointed to by this snapshot with
    // adjacent partition versions. Leaves the snapshot in an unspecified state.
    // Can be retried if previous merge attempt has failed.
    void merge_partition_versions();

    ~partition_snapshot();

    partition_version_ref& version();

    change_mark get_change_mark() {
        return {_region.reclaim_counter(), version_count()};
    }

    const partition_version_ref& version() const;

    partition_version_range versions() {
        return version()->elements_from_this();
    }

    unsigned version_count();

    bool at_latest_version() const {
        return _entry != nullptr;
    }

    const schema_ptr& schema() const { return _schema; }
    logalloc::region& region() const { return _region; }

    tombstone partition_tombstone() const;
    row static_row() const;
    mutation_partition squashed() const;
    // Returns range tombstones overlapping with [start, end)
    std::vector<range_tombstone> range_tombstones(const ::schema& s, position_in_partition_view start, position_in_partition_view end);
};

// Represents mutation_partition with snapshotting support a la MVCC.
//
// Internally the state is represented by an ordered list of mutation_partition
// objects called versions. The logical mutation_partition state represented
// by that chain is equal to reducing the chain using mutation_partition::apply()
// from left (latest version) to right.
class partition_entry {
    partition_snapshot* _snapshot = nullptr;
    partition_version_ref _version;

    friend class partition_snapshot;
    friend class cache_entry;
private:
    // Detaches all versions temporarily around execution of the function.
    // The function receives partition_version* pointing to the latest version.
    template<typename Func>
    void with_detached_versions(Func&&);

    void set_version(partition_version*);

    void apply_to_incomplete(const schema& s, partition_version* other);
public:
    class rows_iterator;
    partition_entry() = default;
    explicit partition_entry(mutation_partition mp);
    ~partition_entry();

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

    // Removes all data marking affected ranges as discontinuous.
    // Includes versions referenced by snapshots.
    void evict() noexcept;

    partition_version_ref& version() {
        return _version;
    }

    partition_version_range versions() {
        return _version->elements_from_this();
    }

    // Strong exception guarantees.
    // Assumes this instance and mp are fully continuous.
    void apply(const schema& s, const mutation_partition& mp, const schema& mp_schema);

    // Strong exception guarantees.
    // Assumes this instance and mpv are fully continuous.
    void apply(const schema& s, mutation_partition_view mpv, const schema& mp_schema);

    // Adds mutation_partition represented by "other" to the one represented
    // by this entry.
    //
    // The argument must be fully-continuous.
    //
    // The rules of addition differ from that used by regular
    // mutation_partition addition with regards to continuity. The continuity
    // of the result is the same as in this instance. Information from "other"
    // which is incomplete in this instance is dropped. In other words, this
    // performs set intersection on continuity information, drops information
    // which falls outside of the continuity range, and applies regular merging
    // rules for the rest.
    //
    // Weak exception guarantees.
    // If an exception is thrown this and pe will be left in some valid states
    // such that if the operation is retried (possibly many times) and eventually
    // succeeds the result will be as if the first attempt didn't fail.
    void apply_to_incomplete(const schema& s, partition_entry&& pe, const schema& pe_schema);

    // Ensures that the latest version can be populated with data from given phase
    // by inserting a new version if necessary.
    // Doesn't affect value or continuity of the partition.
    // Returns a reference to the new latest version.
    partition_version& open_version(const schema& s, partition_snapshot::phase_type phase = partition_snapshot::max_phase) {
        if (_snapshot && _snapshot->_phase != phase) {
            auto new_version = current_allocator().construct<partition_version>(mutation_partition(s.shared_from_this()));
            new_version->partition().set_static_row_continuous(_version->partition().static_row_continuous());
            new_version->insert_before(*_version);
            set_version(new_version);
            return *new_version;
        }
        return *_version;
    }

    mutation_partition squashed(schema_ptr from, schema_ptr to);
    mutation_partition squashed(const schema&);
    tombstone partition_tombstone() const;

    // needs to be called with reclaiming disabled
    void upgrade(schema_ptr from, schema_ptr to);

    // Snapshots with different values of phase will point to different partition_version objects.
    lw_shared_ptr<partition_snapshot> read(logalloc::region& region, schema_ptr entry_schema,
        partition_snapshot::phase_type phase = partition_snapshot::default_phase);

    friend std::ostream& operator<<(std::ostream& out, partition_entry& e);
};

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
