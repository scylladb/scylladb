/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstables/sstable_position.hh"
#include "utils/assert.hh"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <optional>

namespace sstables::mc {

// Translates logical (uncompressed) Data.db positions into the positions that
// the BTI index of an sstable stores.
//
// A logically indexed sstable stores logical positions, so every position is
// known immediately. A physically indexed sstable stores (chunk, offset) pairs,
// so a position is known only after the compressing sink has reported the chunk
// that contains it. The writer reports each chunk with add_chunk().
class index_position_resolver {
public:
    // Position and size of a compressed chunk of Data.db.
    struct chunk {
        // Start of this chunk in the uncompressed data.
        uint64_t uncompressed_start;
        // Length of this chunk in the uncompressed data.
        uint64_t uncompressed_length;
        // Start of this chunk in the compressed (on-disk) file.
        uint64_t chunk_start;
        // Size of this chunk in the compressed (on-disk) file: its extent.
        uint64_t extent;

        uint64_t uncompressed_end() const noexcept {
            return uncompressed_start + uncompressed_length;
        }
    };
private:
    bool _physical;
    // The reported chunks that haven't been discarded yet, in file order.
    std::deque<chunk> _chunks;
    // End of the last reported chunk in the uncompressed data. Every logical
    // position before it is in a reported chunk.
    uint64_t _reported_end = 0;
public:
    explicit index_position_resolver(bool physical) noexcept : _physical(physical) {}

    // Reports a compressed chunk. Chunks must be reported in file order,
    // without gaps. Only a physically indexed sstable needs them.
    void add_chunk(const chunk& c) {
        SCYLLA_ASSERT(_physical);
        SCYLLA_ASSERT(c.uncompressed_start == _reported_end);
        _reported_end = c.uncompressed_end();
        _chunks.push_back(c);
    }

    // Returns the index position of the logical position `pos`, or nullopt if
    // the chunk that contains it hasn't been reported yet.
    //
    // A position at the end of a chunk belongs to the next chunk, at offset 0.
    // It must not be before the point passed to the last discard_before() call.
    std::optional<sstable_position> resolve(uint64_t pos) const {
        if (!_physical) {
            return sstable_position::from_logical(pos);
        }
        if (pos >= _reported_end) {
            return std::nullopt;
        }
        // The first chunk that ends after `pos`. It exists, because `pos` is
        // before the end of the last chunk.
        auto it = std::ranges::upper_bound(_chunks, pos, {}, &chunk::uncompressed_end);
        SCYLLA_ASSERT(it != _chunks.end() && it->uncompressed_start <= pos);
        // The exact extent is a valid extent_hint.
        return sstable_position::from_physical(it->chunk_start, it->extent, pos - it->uncompressed_start);
    }

    // Forgets the chunks that contain no position at or after `pos`. The caller
    // promises not to resolve positions before `pos` anymore.
    void discard_before(uint64_t pos) noexcept {
        while (!_chunks.empty() && _chunks.front().uncompressed_end() <= pos) {
            _chunks.pop_front();
        }
    }

    // The number of chunks held. For tests.
    size_t held_chunks() const noexcept {
        return _chunks.size();
    }
};

} // namespace sstables::mc
