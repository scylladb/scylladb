/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/reader_cache.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "sstables/parquet/stats.hh"

namespace sstables::parquet {

metadata_cache::entry::~entry() {
    // The bytes are in the region, and the entry is not.
    with_allocator(_parent._region.allocator(), [this] {
        _bytes = managed_bytes();
    });
}

void metadata_cache::entry::on_evicted() noexcept {
    ++local_shard_stats.metadata_cache_evictions;
    _parent.erase(*this);
}

metadata_cache::~metadata_cache() {
    while (!_entries.empty()) {
        auto& e = *_entries.begin();
        _lru.remove(e);
        erase(e);
    }
}

future<> metadata_cache::evict_gently() {
    while (!_entries.empty()) {
        auto& e = *_entries.begin();
        _lru.remove(e);
        erase(e);
        co_await coroutine::maybe_yield();
    }
}

void metadata_cache::erase(entry& e) noexcept {
    _entries.erase(_entries.iterator_to(e));
    _used_bytes -= e._size;
    _stats.used_bytes -= e._size;
    local_shard_stats.metadata_cache_bytes -= e._size;
    delete &e;
}

metadata_cache::entry* metadata_cache::find(const key& k) noexcept {
    auto it = _entries.find(k);
    return it == _entries.end() ? nullptr : &*it;
}

bool metadata_cache::contains(const key& k) const noexcept {
    return _entries.find(k) != _entries.end();
}

void metadata_cache::count_lookup(const key& k, bool hit) noexcept {
    if (k.column == key::offset_index) {
        ++(hit ? local_shard_stats.offset_index_cache_hits : local_shard_stats.offset_index_cache_misses);
    } else {
        ++(hit ? local_shard_stats.dictionary_cache_hits : local_shard_stats.dictionary_cache_misses);
    }
}

future<std::optional<metadata_cache::cached_bytes>> metadata_cache::get(key k, std::optional<reader_permit> permit) {
    entry* e = find(k);
    if (!e) {
        count_lookup(k, false);
        co_return std::nullopt;
    }
    const uint64_t id = e->_id;
    const size_t size = e->_bytes.size();
    cached_bytes r;
    if (permit) {
        r.memory = co_await permit->request_memory(size);
    }
    // Allocating may run memory reclaim, which may evict the entry. So the
    // entry is looked up again before each contiguous piece is copied.
    r.bytes = fragmented_temporary_buffer::allocate_to_fit(size);
    auto out = r.bytes.get_ostream();
    size_t pos = 0;
    while (true) {
        e = find(k);
        if (!e || e->_id != id) {
            count_lookup(k, false);
            co_return std::nullopt;
        }
        const size_t n = std::min(size - pos, fragmented_temporary_buffer::default_fragment_size);
        {
            // Nothing here allocates, so the bytes cannot move. The lock
            // makes that explicit.
            logalloc::reclaim_lock rl(_region);
            managed_bytes_view v(e->_bytes);
            v.remove_prefix(pos);
            for (size_t left = n; left;) {
                const auto frag = v.current_fragment();
                const size_t m = std::min(frag.size(), left);
                out.write(reinterpret_cast<const char*>(frag.data()), m);
                v.remove_prefix(m);
                left -= m;
            }
        }
        pos += n;
        if (pos == size) {
            break;
        }
        co_await coroutine::maybe_yield();
    }
    _lru.touch(*e);
    count_lookup(k, true);
    co_return std::move(r);
}

// The page records that get_index() copies between preemption points: about 96 KiB.
static constexpr size_t index_copy_step = 8192;

future<std::optional<metadata_cache::cached_index>> metadata_cache::get_index(const file_summary& s, uint32_t g,
        std::span<const leaf_index> columns, std::optional<reader_permit> permit) {
    const key k = index_key(g);
    entry* e = find(k);
    if (!e) {
        count_lookup(k, false);
        co_return std::nullopt;
    }
    const uint64_t id = e->_id;
    // The copier reads the column table to find the size of the copy.
    // Nothing here allocates, so the bytes cannot move.
    auto copier = [&] {
        logalloc::reclaim_lock rl(_region);
        return row_group_index_copier(s, g, columns, managed_bytes_view(e->_bytes));
    }();
    std::optional<reader_permit::resource_units> memory;
    if (permit) {
        memory = co_await permit->request_memory(copier.memory_usage());
    }
    // Allocating may run memory reclaim, which may evict the entry. So the
    // entry is looked up again before each step of the copy.
    copier.allocate();
    while (true) {
        e = find(k);
        if (!e || e->_id != id) {
            count_lookup(k, false);
            co_return std::nullopt;
        }
        bool done;
        {
            logalloc::reclaim_lock rl(_region);
            done = copier.copy(managed_bytes_view(e->_bytes), index_copy_step);
        }
        if (done) {
            break;
        }
        co_await coroutine::maybe_yield();
    }
    _lru.touch(*e);
    count_lookup(k, true);
    co_return cached_index{std::move(copier).release(), std::move(memory)};
}

void metadata_cache::insert(key k, byte_array_view bytes) noexcept {
    if (find(k)) {
        return;
    }
    try {
        // The entry is in the standard allocator. Allocating it may evict
        // other entries, which does not affect the insertion.
        auto e = std::make_unique<entry>(*this, k, _next_id++);
        _as(_region, [&] {
            with_allocator(_region.allocator(), [&] {
                e->_bytes = managed_bytes(bytes);
                e->_size = e->_bytes.external_memory_usage();
            });
        });
        // Nothing yielded since find(), and eviction only removes entries,
        // so `k` is still absent. Linking allocates nothing.
        _entries.insert(*e);
        _lru.add(*e);
        _used_bytes += e->_size;
        _stats.used_bytes += e->_size;
        local_shard_stats.metadata_cache_bytes += e->_size;
        e.release();
    } catch (const std::bad_alloc&) {
        // Nothing was inserted.
    }
}

} // namespace sstables::parquet
