/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "db/cache_tracker.hh"
#include "mutation/mutation.hh"
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
// The back-pointer (_owner_cached_ptr) points into the primary_index_entry's
// _cached_entry field, which lives in the standard-heap B+tree and is therefore
// stable across LSA compaction.
class cached_mutation_entry final : public evictable {
    schema_ptr _schema;
    mutation_partition _partition;
    // Pointer to the primary_index_entry slot that holds "this".
    // Zeroed by on_evicted() so the index entry knows the cache is gone.
    // When set, call bind_to_owner_slot() to update the primary_index_entry's pointer to this entry.
    cached_entry_slot* _owner_slot;

public:
    // Note: constructed inside the LSA region
    cached_mutation_entry(schema_ptr schema, const mutation_partition& partition, cached_entry_slot* owner_slot)
            : _schema(std::move(schema))
            , _partition(*_schema, partition)
            , _owner_slot(owner_slot) {
        bind_to_owner_slot();
    }

    // LSA compaction moves cached entries between addresses, so the owning
    // primary_index_entry::_cached_entry pointer must be rebound to the new object.
    cached_mutation_entry(cached_mutation_entry&& other) noexcept
            : evictable(std::move(other))
            , _schema(std::move(other._schema))
            , _partition(std::move(other._partition))
            , _owner_slot(std::exchange(other._owner_slot, nullptr)) {
        bind_to_owner_slot();
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

    void rebind_owner_slot(cached_entry_slot* owner_slot) noexcept {
        _owner_slot = owner_slot;
        bind_to_owner_slot();
    }

    void bind_to_owner_slot() noexcept;

    // Called by lru under memory pressure.
    void on_evicted() noexcept override;
};

class cached_entry_slot {
    cached_mutation_entry* _entry = nullptr;

public:
    cached_entry_slot() = default;

    cached_entry_slot(cached_entry_slot&& other) noexcept
            : _entry(std::exchange(other._entry, nullptr)) {
        if (_entry) {
            _entry->rebind_owner_slot(this);
        }
    }

    cached_entry_slot& operator=(cached_entry_slot&& other) noexcept {
        if (this != &other) {
            _entry = std::exchange(other._entry, nullptr);
            if (_entry) {
                _entry->rebind_owner_slot(this);
            }
        }
        return *this;
    }

    cached_entry_slot(const cached_entry_slot&) = delete;
    cached_entry_slot& operator=(const cached_entry_slot&) = delete;

    cached_mutation_entry* get() const noexcept {
        return _entry;
    }

    explicit operator bool() const noexcept {
        return _entry != nullptr;
    }

    cached_mutation_entry& operator*() const noexcept {
        return *_entry;
    }

    cached_mutation_entry* operator->() const noexcept {
        return _entry;
    }

    void bind(cached_mutation_entry& entry) noexcept {
        _entry = &entry;
    }

    void clear() noexcept {
        _entry = nullptr;
    }
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

    // Allocates a new cached_mutation_entry inside the LSA region, links it
    // into the LRU, and stores it in owner_slot.
    //
    // owner_slot must reference the primary_index_entry slot that should be
    // cleared when the entry is evicted.
    //
    // Must be called with _region's allocator active (i.e. inside
    // with_allocator(_region.allocator(), ...)).
    //
    // Returns the new entry (also accessible via owner_slot.get()).
    cached_mutation_entry& insert(schema_ptr schema, const mutation_partition& partition, cached_entry_slot& owner_slot);

public:
    explicit cache_tracker(::cache_tracker& shared_tracker);

    void evict(const primary_index_entry&) noexcept;

    std::optional<mutation_partition> lookup(const primary_index_entry&, schema_ptr) noexcept;

    void populate(const primary_index_entry&, const mutation&) noexcept;

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
