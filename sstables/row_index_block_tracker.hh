/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include "mutation/tombstone.hh"
#include "sstables/mx/types.hh"
#include "utils/assert.hh"

namespace sstables {

// A block of the row index of one partition.
//
// Positions are opaque to this code. The writer chooses their unit, for
// example a byte offset in the Data file or a row ordinal, and uses the
// same unit for all positions of one sstable.
struct row_index_block {
    // The first and the last clustering row or range tombstone marker in the block.
    clustering_info first;
    clustering_info last;
    // The position of the block's first entry, minus the position of the partition.
    uint64_t offset;
    // The end of the block (see row_index_block_tracker::end_block()), minus
    // the position of the block's first entry.
    uint64_t width;
    // The range tombstone that is open at the end of the block, if any.
    std::optional<tombstone> open_marker;
    // The range tombstone that is open at the start of the block.
    // It is empty if none is open.
    tombstone preceding_range_tombstone;
};

// Groups the clustering rows and range tombstone markers of each partition
// into the blocks of the row index.
//
// The writer decides where a block ends. The tracker records the contents
// of the open block, and emits the finished blocks. A partition with one
// block gets no row index, because such an index would not carry its
// weight. So the tracker holds the first block back until a second one
// ends, and then emits both.
//
// The writer must call, for each partition:
//  1. start_partition(), before any other call for the partition.
//  2. For each clustering row or range tombstone marker, in order:
//     on_entry_start() before the entry is written,
//     on_entry_end() after it is written,
//     and end_block() when the writer decides to end the block after the entry.
//  3. At the end of the partition, end_block() if block_count() > 0 and
//     has_open_block(). If block_count() is still below 2 after that, the
//     partition has no row index.
//
// All methods are inline, because they run for every clustering row.
class row_index_block_tracker {
    // The position of the current partition.
    uint64_t _partition_start = 0;
    // The first block of the partition, held back until a second block ends.
    std::optional<row_index_block> _first_block;
    // The number of blocks of the partition that ended, including _first_block.
    uint64_t _block_count = 0;
    // The first entry of the open block. Disengaged if no block is open.
    std::optional<clustering_info> _first_clustering;
    // The last entry of the partition.
    std::optional<clustering_info> _last_clustering;
    // The range tombstone open at the start of the open block.
    tombstone _preceding_range_tombstone;
    // The position of the first entry of the open block.
    uint64_t _block_start = 0;
public:
    // `partition_start` is the position of the partition.
    void start_partition(uint64_t partition_start) {
        _partition_start = partition_start;
        _first_block.reset();
        _block_count = 0;
        _first_clustering.reset();
        _last_clustering.reset();
        _preceding_range_tombstone = {};
        _block_start = 0;
    }

    // Called before an entry is written. `position` is the position of the
    // entry. `preceding_range_tombstone` is the range tombstone open before
    // the entry, or an empty tombstone.
    // Returns true if the entry opens a new block.
    bool on_entry_start(const clustering_info& info, tombstone preceding_range_tombstone, uint64_t position) {
        if (_first_clustering) {
            return false;
        }
        _first_clustering = info;
        _preceding_range_tombstone = preceding_range_tombstone;
        _block_start = position;
        return true;
    }

    // Called after an entry is written.
    void on_entry_end(clustering_info info) {
        _last_clustering = std::move(info);
    }

    // True if a block holds entries and has not ended.
    bool has_open_block() const noexcept {
        return bool(_first_clustering);
    }

    // The number of blocks of the partition that ended so far, including
    // the one that is held back.
    uint64_t block_count() const noexcept {
        return _block_count;
    }

    // The last entry of the partition so far, if any.
    const std::optional<clustering_info>& last_clustering() const noexcept {
        return _last_clustering;
    }

    // Ends the open block. has_open_block() must be true.
    //
    // `end` is the position where the block ends: the position after its
    // last entry, or the end of the partition. `open_tombstone` is the range
    // tombstone open after the block's last entry, or an empty tombstone.
    //
    // Calls `emit(const row_index_block&)` for each block that goes into the
    // row index, in order: nothing for the first block of the partition,
    // the first and the second block when the second one ends, and the
    // ending block after that.
    // Returns true if `emit` was called.
    template <typename Emit>
    bool end_block(uint64_t end, tombstone open_tombstone, Emit&& emit) {
        SCYLLA_ASSERT(_first_clustering && _last_clustering);
        auto block = row_index_block{
            *_first_clustering,
            *_last_clustering,
            _block_start - _partition_start,
            end - _block_start,
            (open_tombstone ? std::make_optional(open_tombstone) : std::optional<tombstone>{}),
            _preceding_range_tombstone,
        };
        _first_clustering.reset();
        _preceding_range_tombstone = tombstone();

        if (_block_count == 0) {
            _first_block.emplace(std::move(block));
            ++_block_count;
            return false;
        } else if (_block_count == 1) {
            emit(std::as_const(*_first_block));
        }
        emit(std::as_const(block));
        ++_block_count;
        return true;
    }
};

} // namespace sstables
