/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>
#include <cstddef>
#include <algorithm>

#include "utils/chunked_vector.hh"

namespace utils {

/// A Count-Min Sketch with 4-bit counters, cache-line optimized layout.
///
/// Used by the W-TinyLFU cache admission policy to estimate access frequency.
/// The sketch uses a depth of 4 and returns the minimum count across all rows.
///
/// Hardware optimization (matches Caffeine's FrequencySketch):
/// All 4 row counters for a given key are co-located in the same 64-byte
/// block (one L1 cache line).  This is achieved with two-level hashing:
///   1. spread(key) selects the block (shared by all 4 rows)
///   2. rehash(spread) is split into 4 bytes; each byte selects the
///      counter position within that row's 2-word segment of the block.
///
/// Block layout (8 uint64_t words = 64 bytes):
///   words 0-1: row 0 (32 counters)
///   words 2-3: row 1
///   words 4-5: row 2
///   words 6-7: row 3
///
/// This turns 4 cache misses per operation into 1.
///
/// The sketch width (number of counters per row) is dynamic and can be
/// changed at runtime via resize().  Total memory:
///   (width / 32) blocks * 8 words * 8 bytes = width * 2 bytes
///   i.e. 2^width_log2 * 2 bytes  (e.g. width_log2=16 → 128 KiB)
///
/// Storage uses chunked_vector (128 KB chunks) to avoid oversized
/// contiguous allocations that would stall the Seastar reactor.
/// On the hot path (increment/estimate), we resolve the block's chunk
/// once via operator[] and then use raw pointer arithmetic for the 8
/// intra-block accesses — same cost as a contiguous vector.
///
/// Minimum width_log2 is 5 (32 counters per row = 1 block).
class count_min_sketch {
    static constexpr size_t depth = 4;
    static constexpr uint64_t reset_mask = 0x7777777777777777ULL;
    static constexpr size_t words_per_block = 8;  // depth * 2
    static constexpr size_t min_width_log2 = 5;   // 32 counters per row = 1 block

    utils::chunked_vector<uint64_t> _table;
    size_t _width_log2;
    size_t _width;       // counters per row (= num_blocks * 32)
    size_t _block_mask;  // num_blocks - 1

    // First hash: selects the block (shared by all 4 rows).
    // Uses splitmix64-style mixing for 64-bit keys.
    static uint32_t spread(uint64_t key) noexcept {
        uint64_t h = key * 0x9e3779b97f4a7c15ULL;
        h ^= h >> 32;
        h *= 0xd6e8feb86659fd93ULL;
        h ^= h >> 32;
        return static_cast<uint32_t>(h);
    }

    // Second hash: rehash of spread for within-block counter selection.
    // Matches Caffeine's rehash().
    static uint32_t rehash(uint32_t x) noexcept {
        x *= 0x31848babU;
        x ^= x >> 14;
        return x;
    }

    static uint8_t get_counter(uint64_t word, size_t pos) noexcept {
        return (word >> (pos * 4)) & 0x0FULL;
    }

public:
    /// Construct a sketch with the given number of counters per row.
    /// \param width_log2 Log base 2 of the number of counters per row.
    ///                   Clamped to minimum of 5 (32 counters = 1 block).
    explicit count_min_sketch(size_t width_log2 = 16)
        : _width_log2(std::max(width_log2, min_width_log2))
        , _width(size_t(1) << _width_log2)
        , _block_mask(_width / 32 - 1)
    {
        // table_size = num_blocks * 8 = (width / 32) * 8 = width / 4
        _table.resize(_width / 4);
    }

    void increment(uint64_t key) noexcept {
        uint32_t block_hash = spread(key);
        uint32_t counter_hash = rehash(block_hash);
        size_t block_offset = static_cast<size_t>(block_hash & _block_mask) * words_per_block;
        // Resolve chunk once, then use raw pointer for intra-block access.
        // All 8 words of a block are contiguous within a single chunk
        // (chunk size 128 KB = 16384 words, block size 8 words, no straddling).
        uint64_t* block = &_table[block_offset];

        for (size_t row = 0; row < depth; ++row) {
            uint32_t h = counter_hash >> (row * 8);
            size_t index = (h >> 1) & 15;       // which of 16 counters in the word
            size_t offset = h & 1;               // which of 2 words for this row
            uint64_t* word = &block[offset + (row * 2)];
            uint8_t val = get_counter(*word, index);
            if (val < 15) {
                *word += (1ULL << (index * 4));
            }
        }
    }

    uint8_t estimate(uint64_t key) const noexcept {
        uint32_t block_hash = spread(key);
        uint32_t counter_hash = rehash(block_hash);
        size_t block_offset = static_cast<size_t>(block_hash & _block_mask) * words_per_block;
        const uint64_t* block = &_table[block_offset];

        uint8_t min_val = 15;
        for (size_t row = 0; row < depth; ++row) {
            uint32_t h = counter_hash >> (row * 8);
            size_t index = (h >> 1) & 15;
            size_t offset = h & 1;
            min_val = std::min(min_val, get_counter(block[offset + (row * 2)], index));
        }
        return min_val;
    }

    /// Halve all counters (aging/decay).
    void reset() noexcept {
        for (auto& word : _table) {
            word = (word >> 1) & reset_mask;
        }
    }

    /// Zeroes all counters (full reset, as opposed to aging via reset()).
    void clear() noexcept {
        for (auto& word : _table) {
            word = 0;
        }
    }

    size_t width() const noexcept { return _width; }
    size_t width_log2() const noexcept { return _width_log2; }

    /// Resize the sketch to a new width (given as log2).
    ///
    /// Because the block layout uses two-level hashing, resizing changes
    /// both block assignment and within-block positions for every key.
    /// We cannot preserve individual counter values.  Instead, resize
    /// simply allocates a new zeroed table — the sketch will re-learn
    /// frequencies from subsequent accesses.  This matches Caffeine's
    /// ensureCapacity() which also discards old counts on resize.
    void resize(size_t new_width_log2) {
        new_width_log2 = std::max(new_width_log2, min_width_log2);
        if (new_width_log2 == _width_log2) {
            return;
        }
        _width_log2 = new_width_log2;
        _width = size_t(1) << _width_log2;
        _block_mask = _width / 32 - 1;
        // chunked_vector allocates/frees in 128 KB chunks, so this
        // won't stall the reactor even for multi-MB sketches.
        _table.clear();
        _table.resize(_width / 4);
    }
};

} // namespace utils
