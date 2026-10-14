/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "db/cache_tracker.hh"
#include "mutation/mutation.hh"
#include "replica/logstor/single_row_partition.hh"
#include "utils/entangled.hh"
#include "utils/lru.hh"
#include "utils/logalloc.hh"
#include <optional>
#include <type_traits>

namespace replica::logstor {

class cache_tracker;
class primary_index;
class primary_index_entry;
class cached_entry_slot;

// LSA compaction moves a cached entry with a constructor that must not throw.
static_assert(std::is_nothrow_move_constructible_v<single_row_partition>);

// An evictable node holding the cached partition of one primary_index_entry, as a
// single_row_partition. The cache admits a single_row_partition, so the shape comes with the type.
//
// Lives inside the shared cache tracker's LSA region. When the shared LSA
// reclaimer needs memory it calls on_evicted(), which:
//   1. Nulls out the back-pointer in the owning primary_index_entry.
//   2. Destroys and frees this object via the LSA allocator.
//
// The back-pointer (_slot_link) is an entangled paired with the
// primary_index_entry::_cached_entry._entry_link, which lives in the
// standard-heap B+tree and is therefore stable across LSA compaction.
class cached_partition_entry final : public evictable {
    schema_ptr _schema;
    partition_key _key;
    single_row_partition _partition;
    entangled _slot_link; // Paired with the primary_index_entry's cached_entry_slot::_entry_link

public:
    // Note: constructed inside the LSA region, as a copy of `partition`.
    cached_partition_entry(schema_ptr schema, const partition_key& key, const single_row_partition& partition, cached_entry_slot& slot);

    // LSA compaction moves cached entries between addresses, so the owning
    // primary_index_entry::_cached_entry pointer must be rebound to the new object.
    cached_partition_entry(cached_partition_entry&& other) noexcept
            : evictable(std::move(other))
            , _schema(std::move(other._schema))
            , _key(std::move(other._key))
            , _partition(std::move(other._partition))
            , _slot_link(std::move(other._slot_link))
    {
    }

    // Not copyable.
    cached_partition_entry(const cached_partition_entry&) = delete;
    cached_partition_entry& operator=(const cached_partition_entry&) = delete;
    cached_partition_entry& operator=(cached_partition_entry&&) = delete;

    const schema_ptr& schema() const noexcept {
        return _schema;
    }

    const partition_key& key() const noexcept {
        return _key;
    }

    const single_row_partition& partition() const noexcept {
        return _partition;
    }

    // Converts the row to `schema`, in place. Called with the region's allocator current.
    void upgrade(schema_ptr schema);

    // Called by lru under memory pressure.
    void on_evicted() noexcept override;

    friend class cached_entry_slot;
};

class cached_entry_slot {
    mutable entangled _entry_link;

public:
    cached_entry_slot() = default;

    cached_entry_slot(cached_entry_slot&&) noexcept = default;
    cached_entry_slot& operator=(cached_entry_slot&&) noexcept = default;
    cached_entry_slot(const cached_entry_slot&) = delete;
    cached_entry_slot& operator=(const cached_entry_slot&) = delete;

    cached_partition_entry* get() const noexcept {
        return _entry_link.get(&cached_partition_entry::_slot_link);
    }

    explicit operator bool() const noexcept { return bool(_entry_link); }
    cached_partition_entry& operator*() const noexcept { return *get(); }
    cached_partition_entry* operator->() const noexcept { return get(); }

    friend class cached_partition_entry;
};

// The cache of a primary index: uses the shared row-cache LSA region and LRU list, and is read
// and written through the index, since a cached partition hangs off the index entry of its key.
class cache_tracker {
    ::cache_tracker& _shared_tracker;
    // Used by the read path to lock the LSA region without changing the current allocator
    logalloc::allocating_section _read_section;
    // Used by cache admission to reserve/retry LSA allocations under pressure.
    logalloc::allocating_section _populate_section;

public:
    explicit cache_tracker(::cache_tracker& shared_tracker);

    void evict(const primary_index_entry&);

    // Admits a copy of `partition`, of the key `key` under `schema`, unless the entry holds a
    // partition already.
    void populate(const primary_index_entry&, const schema_ptr& schema, const partition_key& key, const single_row_partition& partition);

    // The hit path of a read: touches the LRU, counts a hit or a miss, upgrades the entry in place,
    // and hands out the partition with its key. A reader turns the partition into fragments without
    // a mutation in between. The index entry has only the hash of the key, so a point read compares
    // the key it gets with its own. The schema is taken by reference, because only a hit that
    // upgrades the entry needs a reference to it. Never yields.
    std::optional<keyed_single_row_partition> read_partition(const primary_index_entry&, const schema& target_schema);

    // For introspection: a snapshot of the cached partition as a mutation, taken without
    // touching the LRU, without counting a hit or a miss and without upgrading the cached
    // entry in place - the snapshot is upgraded instead.
    std::optional<mutation> peek(const primary_index_entry&, schema_ptr);

private:
    logalloc::region& region() noexcept {
        return _shared_tracker.region();
    }

    allocation_strategy& allocator() noexcept {
        return _shared_tracker.allocator();
    }

    lru& get_lru() noexcept {
        return _shared_tracker.get_lru();
    }
};

} // namespace replica::logstor
