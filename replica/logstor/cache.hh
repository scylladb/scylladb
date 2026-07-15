/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "db/cache_tracker.hh"
#include "mutation/mutation.hh"
#include "utils/entangled.hh"
#include "utils/lru.hh"
#include "utils/logalloc.hh"
#include <optional>

namespace replica::logstor {

class cache_tracker;
class primary_index;
class primary_index_entry;
class cached_entry_slot;

// An evictable node holding the deserialized mutation for one primary_index_entry.
//
// Lives inside the shared cache tracker's LSA region. When the shared LSA
// reclaimer needs memory it calls on_evicted(), which:
//   1. Nulls out the back-pointer in the owning primary_index_entry.
//   2. Destroys and frees this object via the LSA allocator.
//
// The back-pointer (_slot_link) is an entangled paired with the
// primary_index_entry::_cached_entry._entry_link, which lives in the
// standard-heap B+tree and is therefore stable across LSA compaction.
class cached_mutation_entry final : public evictable {
    schema_ptr _schema;
    mutation_partition _partition;
    entangled _slot_link; // Paired with the primary_index_entry's cached_entry_slot::_entry_link

public:
    // Note: constructed inside the LSA region.
    cached_mutation_entry(schema_ptr schema, const mutation_partition& partition, cached_entry_slot& slot);

    // LSA compaction moves cached entries between addresses, so the owning
    // primary_index_entry::_cached_entry pointer must be rebound to the new object.
    cached_mutation_entry(cached_mutation_entry&& other) noexcept
            : evictable(std::move(other))
            , _schema(std::move(other._schema))
            , _partition(std::move(other._partition))
            , _slot_link(std::move(other._slot_link))
    {
    }

    // Not copyable.
    cached_mutation_entry(const cached_mutation_entry&) = delete;
    cached_mutation_entry& operator=(const cached_mutation_entry&) = delete;
    cached_mutation_entry& operator=(cached_mutation_entry&&) = delete;

    const schema_ptr& schema() const noexcept {
        return _schema;
    }

    const mutation_partition& partition() const noexcept {
        return _partition;
    }

    mutation_partition& partition() noexcept {
        return _partition;
    }

    void upgrade(schema_ptr schema) {
        if (_schema != schema) {
            _partition.upgrade(*_schema, *schema);
            _schema = std::move(schema);
        }
    }

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

    cached_mutation_entry* get() const noexcept {
        return _entry_link.get(&cached_mutation_entry::_slot_link);
    }

    explicit operator bool() const noexcept { return bool(_entry_link); }
    cached_mutation_entry& operator*() const noexcept { return *get(); }
    cached_mutation_entry* operator->() const noexcept { return get(); }

    friend class cached_mutation_entry;
};

// Uses the shared row-cache LSA region and LRU list for the logstor cache.
class cache_tracker {
public:
    friend class cached_mutation_entry;

private:
    ::cache_tracker& _shared_tracker;
    // Used by the read path to lock the LSA region without changing the current allocator
    logalloc::allocating_section _read_section;
    // Used by cache admission to reserve/retry LSA allocations under pressure.
    logalloc::allocating_section _populate_section;

public:
    explicit cache_tracker(::cache_tracker& shared_tracker);

    void evict(const primary_index_entry&);

    std::optional<mutation_partition> lookup(const primary_index_entry&, schema_ptr);

    void populate(const primary_index_entry&, const mutation&);

    logalloc::region& region() noexcept {
        return _shared_tracker.region();
    }

    const logalloc::region& region() const noexcept {
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
