/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Statistics that every sstable writer collects for Statistics.db and
// Scylla.db, whatever the layout of its Data file.
//
// A writer collects the statistics of one partition in a column_stats
// (writer_impl::_c_stats) and merges them into the metadata collector
// (writer_impl::_collector) at the end of the partition.

#include "sstables/metadata_collector.hh"
#include "sstables/stats.hh"
#include "sstables/types.hh"
#include "sstables/mx/types.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/mutation_partition.hh"

namespace sstables {

// Collects the statistics of an atomic cell: a regular cell, or an element
// of a collection. The caller counts the cell in column_stats::cells_count
// and column_stats::column_count itself.
inline void collect_cell_stats(column_stats& stats, sstables_stats& sst_stats, atomic_cell_view cell) {
    auto timestamp = cell.timestamp();
    if (!cell.is_live()) {
        stats.update_timestamp(timestamp, is_live::no);
        stats.update_local_deletion_time_and_tombstone_histogram(cell.deletion_time());
        sst_stats.on_cell_tombstone_write();
        return;
    }

    stats.update_timestamp(timestamp, is_live::yes);

    if (cell.is_live_and_has_ttl()) {
        stats.update_ttl(cell.ttl());
        // tombstone histogram is updated with expiration time because if ttl is longer
        // than gc_grace_seconds for all data, sstable will be considered fully expired
        // when actually nothing is expired.
        stats.update_local_deletion_time_and_tombstone_histogram(cell.expiry());
    } else { // regular live cell
        stats.update_local_deletion_time(std::numeric_limits<int>::max());
    }
    sst_stats.on_cell_write();
}

// Collects the statistics of a row marker. A missing marker has none.
//
// A dead marker counts with the TTL expired_liveness_ttl, which is the
// TTL that the mx Data file stores for it.
inline void collect_row_marker_stats(column_stats& stats, const row_marker& marker) {
    if (marker.is_missing()) {
        return;
    }

    api::timestamp_type timestamp = marker.timestamp();
    if (marker.is_live()) {
        stats.update_timestamp(timestamp, is_live::yes);
        stats.update_live_row_marker_timestamp(timestamp);
    } else {
        stats.update_timestamp(timestamp, is_live::no);
    }

    if (!marker.is_live()) {
        stats.update_ttl(gc_clock::duration(expired_liveness_ttl));
        stats.update_local_deletion_time_and_tombstone_histogram(marker.deletion_time());
    } else if (marker.is_expiring()) {
        stats.update_ttl(marker.ttl());
        stats.update_local_deletion_time_and_tombstone_histogram(marker.expiry());
    } else {
        stats.update_ttl(0);
        stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

// The position of a clustering row (bound_kind_m::clustering) or of a range
// tombstone marker of kind `kind` at `ck`, as clustering_bounds_tracker
// receives it.
//
// An exclusive end bound gets the position after all keys with the prefix
// `ck`, although the range ends before them. The maximum clustering
// position in Statistics.db is then larger than necessary, but still covers
// the data. Every writer uses this function, so that sstables of all formats
// record the same bounds for the same data.
inline position_in_partition_view position_of(const clustering_key_prefix& ck, bound_kind_m kind) {
    if (kind == bound_kind_m::clustering) {
        return position_in_partition_view(position_in_partition_view::clustering_row_tag_t(), ck);
    }
    if (kind == bound_kind_m::incl_start || kind == bound_kind_m::excl_end_incl_start) {
        return position_in_partition_view::before_key(ck);
    }
    return position_in_partition_view::after_all_prefixed(ck);
}

// Tracks the smallest and the largest clustering position of an sstable in
// its metadata collector, which stores them in Statistics.db.
//
// Positions within a partition are weakly monotonic, so only the first
// clustering position of a partition can lower the minimum, and only the
// last one can raise the maximum. A partition tombstone covers the whole
// clustering range.
//
// The caller must call, for each partition:
//  - start_partition(), first;
//  - on_partition_tombstone() with the partition tombstone, if it has one;
//  - on_clustered() for each clustering row and range tombstone marker, in order;
//  - end_partition() with the position of the last clustering row or range
//    tombstone marker, if the partition has one.
class clustering_bounds_tracker {
    // True when nothing in the current partition can lower the minimum any more.
    bool _partition_min_checked = false;
public:
    void start_partition() noexcept {
        _partition_min_checked = false;
    }

    void on_partition_tombstone(metadata_collector& collector, tombstone t) {
        if (t) {
            // The tombstone covers the whole clustered region; feed the sentinels to the
            // collector directly - nothing in this partition can improve on them.
            collector.update_min(position_in_partition_view::before_all_clustered_rows());
            collector.update_max(position_in_partition_view::after_all_clustered_rows());
            _partition_min_checked = true;
        }
    }

    void on_clustered(metadata_collector& collector, position_in_partition_view pos) {
        if (!_partition_min_checked) {
            collector.update_min(pos);
            _partition_min_checked = true;
        }
    }

    void end_partition(metadata_collector& collector, position_in_partition_view last_clustered) {
        collector.update_max(last_clustered);
    }
};

} // namespace sstables
