/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/cache.hh"
#include "db/cache_tracker.hh"
#include "replica/logstor/index.hh"
#include "utils/assert.hh"

namespace replica::logstor {

// cached_mutation_entry

cached_mutation_entry::cached_mutation_entry(schema_ptr schema, const mutation_partition& partition, cached_entry_slot& slot)
        : _schema(std::move(schema))
        , _partition(*_schema, partition)
        , _slot_link(entangled::make_paired_with(slot._entry_link)) {
}

void cached_mutation_entry::on_evicted() noexcept {
    if (auto* ct = get_current_cache_tracker()) {
        ct->on_partition_eviction();
    }
    // Destroy and free this object using the LSA allocator that allocated it.
    current_allocator().destroy(this);
}

// cache_tracker

cache_tracker::cache_tracker(::cache_tracker& shared_tracker)
    : _shared_tracker(shared_tracker)
    , _read_section(abstract_formatter([] (fmt::context& ctx) {
        fmt::format_to(ctx.out(), "logstor_cache.read");
    }))
    , _populate_section(abstract_formatter([] (fmt::context& ctx) {
        fmt::format_to(ctx.out(), "logstor_cache.populate");
    })) {
}

void cache_tracker::evict(const primary_index_entry& pie) {
    if (!pie._cached_entry) {
        return;
    }

    with_allocator(allocator(), [&] {
        auto& e = *pie._cached_entry;
        get_lru().remove(e);
        current_allocator().destroy(&e);
        _shared_tracker.on_partition_remove();
    });
}

std::optional<mutation> cache_tracker::lookup(const primary_index_entry& pie, schema_ptr target_schema) {
    std::optional<mutation> cached_mut;
    _read_section(region(), [&] {
        if (pie._cached_entry) {
            get_lru().touch(*pie._cached_entry);
            if (pie._cached_entry->schema() != target_schema) {
                with_allocator(allocator(), [&] {
                    pie._cached_entry->upgrade(target_schema);
                });
            }
            cached_mut = mutation(target_schema, pie.key().dk, pie._cached_entry->partition());
        }
    });

    if (cached_mut) {
        _shared_tracker.on_partition_hit();
    } else {
        _shared_tracker.on_partition_miss();
    }

    return cached_mut;
}

void cache_tracker::populate(const primary_index_entry& pie, const mutation& m) {
    _populate_section(region(), [&] {
        with_allocator(allocator(), [&] {
            if (pie._cached_entry) {
                _shared_tracker.on_miss_already_populated();
                return;
            }
            auto* e = current_allocator().construct<cached_mutation_entry>(m.schema(), m.partition(), pie._cached_entry);
            get_lru().add(*e);
            _shared_tracker.on_partition_insert();
        });
    });
}

} // namespace replica::logstor
