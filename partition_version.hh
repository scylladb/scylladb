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
    explicit partition_version(mutation_partition mp) noexcept
        : _partition(std::move(mp)) { }
    partition_version(partition_version&& pv) noexcept;
    partition_version& operator=(partition_version&& pv) noexcept;
    ~partition_version();

    mutation_partition& partition() { return _partition; }
    const mutation_partition& partition() const { return _partition; }

    bool is_referenced() { return _backref; }
    partition_version_ref& back_reference() { return *_backref; }
};

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

    explicit operator bool() { return _version; }

    partition_version& operator*() {
        assert(_version);
        return *_version;
    }
    partition_version* operator->() {
        assert(_version);
        return _version;
    }

    bool is_unique_owner() const { return _unique_owner; }
    void mark_as_unique_owner() { _unique_owner = true; }
};

class partition_entry;

class partition_snapshot : public enable_lw_shared_from_this<partition_snapshot> {
    schema_ptr _schema;
    // Either _version or _entry is non-null.
    partition_version_ref _version;
    partition_entry* _entry;

    friend class partition_entry;
public:
    explicit partition_snapshot(schema_ptr s, partition_entry* entry)
        : _schema(std::move(s)), _entry(entry) { }
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

    auto versions() {
        return version()->elements_from_this();
    }

    unsigned version_count();
};

class partition_entry {
    partition_snapshot* _snapshot = nullptr;
    partition_version_ref _version;

    friend class partition_snapshot;
private:
    void set_version(partition_version*);

    void apply(const schema& s, partition_version* pv, const schema& pv_schema);
public:
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

    // Strong exception guarantees.
    void apply(const schema& s, const mutation_partition& mp, const schema& mp_schema);

    // Same exception guarantees as:
    // mutation_partition::apply(const schema&, mutation_partition&&, const schema&)
    void apply(const schema& s, mutation_partition&& mp, const schema& mp_schema);

    // Strong exception guarantees.
    void apply(const schema& s, mutation_partition_view mpv, const schema& mp_schema);

    // Weak exception guarantees.
    // If an exception is thrown this and pe will be left in some valid states
    // such that if the operation is retried (possibly many times) and eventually
    // succeeds the result will be as if the first attempt didn't fail.
    void apply(const schema& s, partition_entry&& pe, const schema& pe_schema);

    mutation_partition squashed(schema_ptr from, schema_ptr to);

    // needs to be called with reclaiming disabled
    void upgrade(schema_ptr from, schema_ptr to);

    lw_shared_ptr<partition_snapshot> read(schema_ptr entry_schema);
};

inline partition_version_ref& partition_snapshot::version()
{
    if (_version) {
        return _version;
    } else {
        return _entry->_version;
    }
}

class partition_snapshot_reader : public streamed_mutation::impl {
    struct rows_position {
        mutation_partition::rows_type::const_iterator _position;
        mutation_partition::rows_type::const_iterator _end;
    };

    class heap_compare {
        position_in_partition::less_compare& _cmp;
    public:
        explicit heap_compare(position_in_partition::less_compare cmp) : _cmp(cmp) { }
        bool operator()(const rows_position& a, const rows_position& b) {
            return _cmp(*b._position, *a._position);
        }
    };
private:
    // Keeps shared pointer to the container we read mutation from to make sure
    // that its lifetime is appropriately extended.
    boost::any _container_guard;

    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _current_ck_range;
    query::clustering_row_ranges::const_iterator _ck_range_end;
    bool _in_ck_range = false;

    position_in_partition::less_compare _cmp;
    position_in_partition::equal_compare _eq;

    lw_shared_ptr<partition_snapshot> _snapshot;
    stdx::optional<position_in_partition> _last_entry;

    std::vector<rows_position> _clustering_rows;

    range_tombstone_stream _range_tombstones;

    logalloc::region& _lsa_region;
    logalloc::allocating_section& _read_section;

    uint64_t _reclaim_counter;
    unsigned _version_count = 0;
private:
    void refresh_iterators();
    void pop_clustering_row();

    mutation_fragment_opt read_static_row();
    mutation_fragment_opt read_next();
    void do_fill_buffer();
    static tombstone tomb(partition_snapshot& snp);
public:
    partition_snapshot_reader(schema_ptr s, dht::decorated_key dk, lw_shared_ptr<partition_snapshot> snp,
        query::clustering_key_filter_ranges crr,
        logalloc::region& region, logalloc::allocating_section& read_section,
        boost::any pointer_to_container);
    ~partition_snapshot_reader();
    virtual future<> fill_buffer() override;
};

streamed_mutation make_partition_snapshot_reader(schema_ptr s, dht::decorated_key dk,
    query::clustering_key_filter_ranges crr,
    lw_shared_ptr<partition_snapshot> snp, logalloc::region& region,
    logalloc::allocating_section& read_section, boost::any pointer_to_container);
