/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "mutation/tombstone.hh"
#include "mutation/position_in_partition.hh"
#include "sstables/types.hh"

namespace sstables {

struct data_file_positions_range {
    uint64_t start;
    std::optional<uint64_t> end;
};

// Stores information about open end RT marker
// of the lower index bound
struct open_rt_marker {
    position_in_partition pos;
    tombstone tomb;
};

// An abstract interface for a reader of sstable indexes,
// which can be used by queries to locate the needed file offsets in the Data file.
//
// Conceptually, an index reader is a pair of index cursors -- "lower bound" and "upper bound" --
// which can be set to "point" before or after chosen positions in the dataset,
// and can be queried for Data file offsets which match the pointed-to positions.
//
// As of this writing, there is only one index format used in Scylla
// (the one used by SSTables in the "BIG" format, which is the only format
// supported by Scylla). And so there is only one implementation of this interface.
// But we want to add another implementation soon (BTI-format indexes),
// and this interface has been extracted in preparation for that.
//
// Note: in the comments below, "PK" means a position that corresponds either
// to a start of some partition or to EOF,
// while "position" means a position that corresponds either to the start of some partition,
// to the start of some clustering entry, or to EOF.
//
// Note: even though some methods of the index are inexact (i.e. they advance the index to *some*
// Data position close to the queried ring position), they are monotonic.
// I.e. if B >= A, then advance(B) >= advance(A).
class abstract_index_reader {
public:
    virtual ~abstract_index_reader() = default;
    // Must be called before the reader is destroyed.
    virtual future<> close() noexcept = 0;
    // True iff lower bound is at EOF.
    virtual bool eof() const = 0;

    // If `key` is a partition key present in the sstable, advances lower bound to `key`.
    // Otherwise advances lower bound to the some PK no greater than `key`.
    // Returns `true` iff it's possible that `key` is a partition key present in the sstable.
    // (In other words, if it returns `false`, then the key is definitely not present.
    // Otherwise it's unknown if it's present).
    //
    // Precondition: pos >= lower bound
    //
    // Note: this is the most important and performance-sensitive method of the reader.
    // This is what's used by sstable readers to find positions for single-partition reads.
    virtual future<bool> advance_lower_and_check_if_present(dht::ring_position_view key) = 0;
    // Advances lower bound to the first PK greater than dk.
    //
    // Preconditions: dk >= lower bound, dk is present in the sstable
    virtual future<> advance_past_definitely_present_partition(const dht::decorated_key& dk) = 0;
    // Advances lower bound to dk.
    //
    // Preconditions: dk >= lower bound, dk is present in the sstable
    virtual future<> advance_to_definitely_present_partition(const dht::decorated_key& dk) = 0;
    // Advances lower bound to the first PK which lies inside or after the range,
    // or to some close predecessor of that optimal PK.
    // Advances upper bound to the first PK which lies after the range.
    // or to some close successor of that optimal PK.
    // Preconditions:
    // 1. next lower bound >= lower bound
    // 2. next upper bound >= upper bound
    virtual future<> advance_to(const dht::partition_range& range) = 0;
    // Advances lower bound to the first PK greater than lower bound.
    // Precondition: !eof()
    virtual future<> advance_to_next_partition() = 0;
    // Advances upper bound to the first PK greater than lower bound.
    // (Or to EOF if lower bound is EOF).
    virtual future<> advance_reverse_to_next_partition() = 0;

    // Partially advances some internals in order to warm up some caches.
    //
    // Does not move the bounds, but does "advance" the lower bound to `pos`
    // for the purposes of "pos >= lower bound" preconditions.
    //
    // Preconditions:
    // 1. Lower bound has been advanced.
    // 2. !eof().
    // 3. Must be called for non-decreasing positions.
    virtual future<> prefetch_lower_bound(position_in_partition_view pos) = 0;
    // Advances lower bound to some position (in the current partition) no greater than pos.
    // Preconditions:
    // 1. !eof()
    // 2. `pos` >= lower bound
    virtual future<> advance_to(position_in_partition_view pos) = 0;
    // Advances upper bound to some position strictly after `pos`.
    // Preconditions:
    // 1. !eof()
    // 2. upper bound is unset
    virtual future<> advance_upper_past(position_in_partition_view pos) = 0;
    // Advances the upper bound to the start of the first promoted index block after `pos`,
    // or to the next PK if there are no blocks after `pos`.
    //
    // Supports advancing backwards (i.e. `pos` can be smaller than the previous upper bound position).
    virtual future<> advance_reverse(position_in_partition_view pos) = 0;

    // Tells whether details about current partition can be accessed.
    // If this returns false, you have to call read_partition_data(),
    // before calling the relevant accessors below.
    //
    // Calling read_partition_data() may involve doing I/O. The reason
    // why control over this is exposed and not done under the hood is that
    // in some cases it only makes sense to access partition details from index
    // if it is readily available, and if it is not, we're better off obtaining
    // them by continuing reading from sstable.
    virtual bool partition_data_ready() const = 0;
    // Ensures that partition_data_ready() returns true.
    // Precondition: !eof()
    virtual future<> read_partition_data() = 0;
    // Returns tombstone for the current partition,
    // if such information is available in the index.
    //
    // Note: it's an arbitrary decision of the writer of the index whether
    // the the partition tombstone has been attached to a given index entry,
    // and the user of the index reader should not assume that it has.
    //
    // The main use case for this information is reads which start in the
    // middle of a large partition. The Data reader needs to know the partition
    // header (full partition key and partition tombstone) to emit a partition,
    // but the header is at the beginning of the partition, potentially far
    // from the queried rows.
    // Embedding the partition header in the index lets the Data reader skip
    // avoid doing a separate disk read to get the header from the Data file.
    //
    // Thus, `partition_tombstone()` and `get_partition_key()` usually
    // return an engaged optional at least for those partitions which have an
    // intra-partition index (because that's when they can be used to skip
    // a disk seek) but the reader shouldn't assume that. It should check
    // if they are available, and if not, it should fall back to reading
    // the partition header from the Data file.
    //
    // Precondition: partition_data_ready()
    virtual std::optional<sstables::deletion_time> partition_tombstone() = 0;
    // Returns the key for current partition of the lower bound,
    // if available (se the comment for partition_tombstone) in the index.
    //
    // Precondition: partition_data_ready()
    virtual std::optional<partition_key> get_partition_key() = 0;
    // Returns data file positions corresponding to the bounds.
    // End position may be unset
    virtual data_file_positions_range data_file_positions() const = 0;
    // Returns the offset in the data file of the first row in the last promoted index block
    // in the current partition or nullopt if there are no blocks in the current partition.
    //
    // Preconditions: partition_data_ready()
    virtual future<std::optional<uint64_t>> last_block_offset() = 0;
    // Returns the kind of sstable element the cursor is pointing at.
    // No preconditions.
    virtual indexable_element element_kind() const = 0;
    // Returns info about the range tombstone (if any) which covers lower bound.
    // Precondition: !eof()
    virtual std::optional<open_rt_marker> end_open_marker() const = 0;
    // Returns info about the range tombstone (if any) which covers upper bound.
    // Precondition: !eof()
    virtual std::optional<open_rt_marker> reverse_end_open_marker() const = 0;
};

} // namespace sstables
