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

// cached_partition_entry

cached_partition_entry::cached_partition_entry(schema_ptr schema, const partition_key& key, const single_row_partition& partition, cached_entry_slot& slot)
        : _schema(std::move(schema))
        , _key(key)
        , _partition(partition.copy(*_schema))
        , _slot_link(entangled::make_paired_with(slot._entry_link)) {
}

void cached_partition_entry::upgrade(schema_ptr schema) {
    if (_schema == schema) {
        return;
    }
    if (auto& row = _partition.row) {
        // Built beside the old row and swapped in, so that a failed allocation leaves the entry as
        // it was.
        std::optional<deletable_row> upgraded(std::in_place, *schema, *_schema, *row);
        row = std::move(upgraded);
    }
    _schema = std::move(schema);
}

void cached_partition_entry::on_evicted() noexcept {
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

std::optional<keyed_single_row_partition> cache_tracker::read_partition(const primary_index_entry& pie, const schema& target_schema) {
    // The partition is built inside the section and returned out of it by value: an allocation
    // failure re-runs the section's function from the start, and nothing outside it has been
    // touched by then.
    auto cached = _read_section(region(), [&] () -> std::optional<keyed_single_row_partition> {
        if (!pie._cached_entry) {
            return std::nullopt;
        }
        auto& e = *pie._cached_entry;
        get_lru().touch(e);
        if (e.schema().get() != &target_schema) {
            with_allocator(allocator(), [&] {
                e.upgrade(target_schema.shared_from_this());
            });
        }
        return keyed_single_row_partition{dht::decorated_key(pie.key().token(), e.key()), e.partition().copy(*e.schema())};
    });

    if (cached) {
        _shared_tracker.on_partition_hit();
    } else {
        _shared_tracker.on_partition_miss();
    }

    return cached;
}

std::optional<mutation> cache_tracker::peek(const primary_index_entry& pie, schema_ptr target_schema) {
    auto cached_mut = _read_section(region(), [&] () -> std::optional<mutation> {
        if (!pie._cached_entry) {
            return std::nullopt;
        }
        const auto& e = *pie._cached_entry;
        dht::decorated_key dk(pie.key().token(), e.key());
        return to_mutation(e.schema(), std::move(dk), e.partition().copy(*e.schema()));
    });

    if (cached_mut && cached_mut->schema() != target_schema) {
        cached_mut->upgrade(target_schema);
    }

    return cached_mut;
}

void cache_tracker::populate(const primary_index_entry& pie, const schema_ptr& schema, const partition_key& key,
        const single_row_partition& partition) {
    _populate_section(region(), [&] {
        with_allocator(allocator(), [&] {
            if (pie._cached_entry) {
                _shared_tracker.on_miss_already_populated();
                return;
            }
            auto* e = current_allocator().construct<cached_partition_entry>(schema, key, partition, pie._cached_entry);
            get_lru().add(*e);
            _shared_tracker.on_partition_insert();
        });
    });
}

} // namespace replica::logstor
