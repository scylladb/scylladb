/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The cache of the OffsetIndexes and the dictionaries of a file.
//
// The cache keeps them decoded, in compact forms without pointers, so that
// a hit parses, checks, and decompresses nothing:
//
// - The OffsetIndexes of a row group, in the form of
//   decode_offset_index_block() (page_decoder.hh): a fixed-size record per
//   leaf column of the file, and one per page. The unit is the row group,
//   with the OffsetIndexes of all its in-place chunks, not only the
//   projected ones, because the writer places them in one block, which
//   the reader fetches with one read.
// - The dictionary of a column chunk, in the form of dictionary
//   (encoding.hh): the uncompressed page body, and for byte arrays the
//   position of each entry.
//
// The reader decodes and checks a page or block that it fetches, and
// inserts the result.
//
// Memory. The bytes of an entry live in an LSA region, in fragments of at
// most logalloc::max_managed_object_size, and each entry is an index entry
// in an LRU. For sstables, both belong to the cache_tracker, so memory
// reclaim evicts the entries in LRU order with the row cache and the other
// index caches, and index_cache_fraction covers them (metadata_cache_stats).
// There is no other limit. The small object that holds an entry and the
// set that finds it are in the standard allocator, as for cached_file.
//
// Readers never pin an entry. A reader copies what it needs into memory of
// its own, charged to its permit before the copy is allocated, and uses
// the copy as it is: the records of the projected columns of an
// OffsetIndex entry, and the whole of a dictionary entry. An entry may be
// evicted at any time outside the short sections that copy it. Nothing
// that a reader allocates is stored in the cache: an insertion copies the
// bytes into the region.
//
// A cache serves one file on one shard. It must be destroyed on the shard
// that created it, before its region, LRU, and stats.

#include <cstdint>
#include <limits>
#include <optional>
#include <span>

#include <boost/intrusive/set.hpp>

#include <seastar/core/future.hh>

#include "reader_permit.hh"
#include "sstables/parquet/bit_util.hh"
#include "sstables/parquet/file_summary.hh"
#include "sstables/parquet/metadata_cache_stats.hh"
#include "sstables/parquet/page_decoder.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/logalloc.hh"
#include "utils/lru.hh"
#include "utils/managed_bytes.hh"

namespace sstables::parquet {

class metadata_cache {
public:
    /// What an entry holds: the OffsetIndexes of a row group, or the
    /// dictionary of a column chunk.
    struct key {
        uint32_t row_group;
        /// The column of a dictionary page, or offset_index.
        leaf_index column;
        static constexpr leaf_index offset_index = std::numeric_limits<leaf_index>::max();
        auto operator<=>(const key&) const = default;
    };
    static key index_key(uint32_t row_group) noexcept { return {row_group, key::offset_index}; }
    static key dictionary_key(uint32_t row_group, leaf_index column) noexcept { return {row_group, column}; }

    /// A reader's copy of the bytes of an entry, and the charge of the copy
    /// to the reader's permit. The charge must live as long as the bytes.
    struct cached_bytes {
        fragmented_temporary_buffer bytes;
        std::optional<reader_permit::resource_units> memory;
    };
    /// A reader's copy of the OffsetIndexes of some columns of a row group,
    /// and the charge of the copy to the reader's permit.
    struct cached_index {
        row_group_index index;
        std::optional<reader_permit::resource_units> memory;
    };

private:
    class entry final : public index_evictable {
    public:
        using hook_type = boost::intrusive::set_member_hook<boost::intrusive::link_mode<boost::intrusive::normal_link>>;
        metadata_cache& _parent;
        const key _key;
        // Distinguishes an entry from one that replaced it under the same key.
        const uint64_t _id;
        hook_type _hook;
        // In the region.
        managed_bytes _bytes;
        // The memory of _bytes in the region.
        size_t _size = 0;

        entry(metadata_cache& parent, key k, uint64_t id) noexcept : _parent(parent), _key(k), _id(id) {}
        ~entry();
        void on_evicted() noexcept override;
    };
    struct key_of_entry {
        using type = key;
        const key& operator()(const entry& e) const noexcept { return e._key; }
    };
    using set_type = boost::intrusive::set<entry,
            boost::intrusive::member_hook<entry, entry::hook_type, &entry::_hook>,
            boost::intrusive::key_of_value<key_of_entry>,
            boost::intrusive::constant_time_size<true>>;

    logalloc::region& _region;
    lru& _lru;
    metadata_cache_stats& _stats;
    logalloc::allocating_section _as;
    // Eviction erases entries from the set, from memory reclaim, so the set
    // is modified only in code that does not allocate.
    set_type _entries;
    uint64_t _next_id = 0;
    // The memory of the entries in the region.
    uint64_t _used_bytes = 0;

    entry* find(const key& k) noexcept;
    // Erases an entry that is not linked in the LRU, and frees it.
    void erase(entry& e) noexcept;
    void count_lookup(const key& k, bool hit) noexcept;
public:
    metadata_cache(logalloc::region& region, lru& lru, metadata_cache_stats& stats) noexcept
        : _region(region), _lru(lru), _stats(stats) {}
    metadata_cache(const metadata_cache&) = delete;
    ~metadata_cache();

    /// Whether the cache holds `k`. It does not count as a use of the entry.
    bool contains(const key& k) const noexcept;

    /// Returns a copy of the bytes cached under `k`, or nothing, and marks
    /// the entry as the most recently used. The copy is allocated with
    /// fragmented_temporary_buffer::allocate_to_fit(), and charged to
    /// `permit`, if there is one, before it is allocated. If the entry is
    /// evicted before it is copied, for example while the reader waits for
    /// memory, the result is nothing, as on a miss. Yields between the
    /// contiguous pieces of a large copy.
    future<std::optional<cached_bytes>> get(key k, std::optional<reader_permit> permit);

    /// Returns a copy of the OffsetIndexes of `columns`, in increasing
    /// order, of row group `g` of the file of summary `s`, or nothing, as
    /// get() does. The entry must hold the compact form of
    /// decode_offset_index_block(). The copy holds the records of
    /// `columns` only. Its memory is charged to `permit`, if there is
    /// one, before it is allocated. `columns` must live until the future
    /// resolves.
    future<std::optional<cached_index>> get_index(const file_summary& s, uint32_t g, std::span<const leaf_index> columns,
            std::optional<reader_permit> permit);

    /// Inserts a copy of `bytes` under `k`, unless the cache has an entry
    /// for `k` already, for example from another reader that missed at the
    /// same time. The bytes must be the compact form of what `k` names,
    /// decoded from checked input. If the copy cannot be allocated,
    /// nothing is inserted: the cache is an optimization.
    void insert(key k, byte_array_view bytes) noexcept;

    /// Evicts every entry, with preemption points.
    future<> evict_gently();

    /// The number of entries.
    size_t size() const noexcept { return _entries.size(); }
    /// The memory of the entries in the region.
    uint64_t memory_usage() const noexcept { return _used_bytes; }
};

} // namespace sstables::parquet
